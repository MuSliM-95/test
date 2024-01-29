const getPreparePrices = (prices) => {
  const data = {};
  prices?.forEach((item) => {
    if (!data[item?.nomenclature_id]) {
      data[item?.nomenclature_id] = {};
    }
    data[item?.nomenclature_id][item?.price_type] = item?.price;
  });
  return data;
};

const ChangeTypes = {
  Price: "price",
  Nomenclature: "nomenclature",
  Complex: "complex",
};

const getChangeType = (changes, priceTypes) => {
  if (
    changes?.every((change) =>
      priceTypes.some(({ name }) => name === change?.[1])
    )
  )
    return ChangeTypes.Price;
  if (
    changes?.every((change) =>
      priceTypes.every(({ name }) => name !== change?.[1])
    )
  )
    return ChangeTypes.Nomenclature;
  return ChangeTypes.Complex;
};

const convertArrayToObject = (arr) => {
  const obj = {};
  arr?.forEach(({ id, name }) => {
    obj[id] = name;
  });
  return obj;
};

const convertUnitsToObjectByValue = (arr) => {
  const obj = {};
  arr?.forEach(({ id, convent_national_view }) => {
    obj[convent_national_view] = id;
  });
  return obj;
};

const reversedObj = (obj) =>
  Object.entries(obj).reduce(
    (acc, [key, value]) => ({ ...acc, [value]: key }),
    {}
  );

const getPriceListForEditFetch = (rows, priceTypes, prices) => {
  const editPrice = [];
  const createPrice = [];
  rows.forEach((row) => {
    priceTypes.forEach((priceType) => {
      const typesName = priceType?.name;
      const cost = +row[typesName];
      const price = prices?.find(({ nomenclature_id, price_type }) => {
        return nomenclature_id === row.id && price_type === typesName;
      });
    
      if (isNaN(cost)) return;
      if (price) {
        editPrice.push({
          id:price.id,
          nomenclature: row.id,
          price: cost,
          price_type: priceType?.id,
        });
      } else {
        createPrice.push({

          nomenclature: row.id,
          price: cost,
          price_type: priceType?.id,
        });
      }
    });
  });
  return { editPrice, createPrice };
};

const getPriceListForCreateFetch = (rows, priceTypes) => {
  const pricesList = [];
  rows.forEach((row) => {
    priceTypes.forEach((priceType) => {
      const typesName = priceType?.name;
      const cost = +row[typesName];
      if (cost === null) return;
      pricesList.push({
        nomenclature: row.id,
        price: cost,
        price_type: priceType?.id,
      });
    });
  });
  return pricesList;
};

export {
  getPreparePrices,
  ChangeTypes,
  getChangeType,
  convertArrayToObject,
  convertUnitsToObjectByValue,
  reversedObj,
  getPriceListForEditFetch,
  getPriceListForCreateFetch,
};
