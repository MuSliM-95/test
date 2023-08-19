/* eslint-disable no-unused-vars */
/* eslint-disable react-hooks/exhaustive-deps */
import { useContext, useState, useEffect, useRef } from "react";
import { Typography } from "antd";
import { HotTable, HotColumn } from "@handsontable/react";
import { registerAllModules } from "handsontable/registry";
import { useQueryClient } from "react-query";
import { registerLanguageDictionary, deDE } from "handsontable/i18n";

import "handsontable/dist/handsontable.full.min.css";

import { PricesHandsontableContext } from "../../../../shared/lib/hooks/context/getPricesHandsontableContext";
import {
  useFetchGetPrices,
  useFetchCreatePrice,
  useFetchEditPrice,
  useFetchDeletePrices,
} from "../../../../enitities/Price/lib/hooks/usePriceQuery";
import {
  useFetchGetNomenclature,
  useFetchEditNomenclature,
  useFetchCreateNomenclature,
  useFetchDeleteNomenclature,
} from "../../../../enitities/Nomenclature/lib/hooks/useNomenclatureQuery";
import { useFetchGetPriceTypes } from "../../../../enitities/PriceType/lib/hooks/usePriceTypeQuery";
import { useFetchGetManufactures } from "../../../../enitities/Manufacture/lib/hooks/useManufactureQuery";
import { useFetchGetUnits } from "../../../../enitities/Unit/lib/hooks/useUnitQuery";
import { useFetchGetCategories } from "../../../../enitities/Category/lib/hooks/useCategoryQuery";

import {
  getPreparePrices,
  ChangeTypes,
  getChangeType,
  convertArrayToObject,
  convertUnitsToObjectByValue,
  reversedObj,
  getPriceListForEditFetch,
  getPriceListForCreateFetch,
} from "./utils";

import { useVirtualization } from "./hooks";

registerAllModules();
registerLanguageDictionary(deDE);

export default function TableDocsPurchases() {
  const hotTableRef = useRef(null);
  const [data, setData] = useState([]);
  const [changeCount, setChangeCount] = useState(0);

  const queryClient = useQueryClient();

  const { token, websocket } = useContext(PricesHandsontableContext);
  const {
    isLoading: isLoadingPrices,
    isError: isErrorPrices,
    data: prices,
  } = useFetchGetPrices({ token });
  const {
    isLoading: isLoadingNomenclatures,
    isError: isErrorNomenclatures,
    data: nomenclatures = [],
    isSuccess: isSuccessNomenclatures,
  } = useFetchGetNomenclature({ token });
  const {
    isLoading: isLoadingPriceTypes,
    isError: isErrorPriceTypes,
    data: priceTypes,
  } = useFetchGetPriceTypes({ token });
  const {
    isLoading: isLoadingManufactures,
    isError: isErrorManufactures,
    data: manufactures,
    isSuccess: isSuccessManufactures,
  } = useFetchGetManufactures({ token });
  const {
    isLoading: isLoadingUnits,
    isError: isErrorUnits,
    data: units,
  } = useFetchGetUnits();

  const {
    isLoading: isLoadingCategories,
    isError: isErrorCategories,
    data: categories,
    isSuccess: isSuccessCategories,
  } = useFetchGetCategories({ token });

  const [
    mutateEditNomenclatures,
    { isLoading: isLoadingEdit, isError: isErrorEdit, isSucces: isSuccessEdit },
  ] = useFetchEditNomenclature(token);

  const [
    _,
    {
      isLoading: isLoadingCreate,
      isError: isErrorCreate,
      isSucces: isSuccessCreate,
      mutateAsync: mutateAsyncCreateNomenclature,
    },
  ] = useFetchCreateNomenclature(token);

  const [
    mutateDeleteNomenclatures,
    {
      isLoading: isLoadingDelete,
      isError: isErrorDelete,
      isSuccess: isSuccesDelete,
    },
  ] = useFetchDeleteNomenclature(token);

  const [
    mutateDeletePrices,
    {
      isLoading: isLoadingDeletePrices,
      isError: isErrorDeletePrices,
      isSuccess: isSuccesDeletePrices,
    },
  ] = useFetchDeletePrices(token);

  const [
    mutateCreatePrices,
    { isLoading: isLoadingCreatePrices, isError: isErrorCreatePrices },
  ] = useFetchCreatePrice(token);

  const [
    mutateEditPrice,
    { isLoading: isLoadingEditPrice, isError: isErrorEditPrice },
  ] = useFetchEditPrice(token);

  const { rowRenderer, handleScroll } = useVirtualization();

  const isLoading =
    isLoadingPrices ||
    isLoadingNomenclatures ||
    isLoadingPriceTypes ||
    isLoadingManufactures ||
    isLoadingUnits ||
    isLoadingCategories ||
    isLoadingEdit ||
    isLoadingCreate ||
    isLoadingDelete ||
    isLoadingCreatePrices ||
    isLoadingEditPrice;

  const isError =
    isErrorPrices ||
    isErrorNomenclatures ||
    isErrorPriceTypes ||
    isErrorManufactures ||
    isErrorUnits ||
    isErrorCategories ||
    isErrorEdit ||
    isErrorCreate ||
    isErrorDelete ||
    isErrorCreatePrices ||
    isErrorEditPrice;
  if (isError) console.log("isError");

  const manumafacturesById = convertArrayToObject(manufactures);
  const manumafacturesByValue = reversedObj(manumafacturesById);

  const categoriesById = convertArrayToObject(categories);
  const categoriesByValue = reversedObj(categoriesById);

  const unitsByValue = convertUnitsToObjectByValue(units);

  const preparePrices = getPreparePrices(prices);

  useEffect(() => {
    if (
      (isSuccessNomenclatures &&
        isSuccessManufactures &&
        nomenclatures?.length) ||
      isError
    ) {
      const newData = nomenclatures.map((item) => {
        const prices = preparePrices[item?.id] || {};
        return {
          ...item,
          ...prices,
          manufacturer: manumafacturesById[item?.manufacturer],
          category: categoriesById[item?.category],
        };
      });
      setData(newData);
    }
  }, [
    isSuccessNomenclatures,
    isSuccessManufactures,
    isSuccessCategories,
    isSuccessEdit,
    isSuccessCreate,
    changeCount,
    nomenclatures?.length,
    prices?.length,
    isError,
  ]);

  useEffect(() => {
    websocket.onmessage = (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "nomenclature" && data.action === "edit") {
        const editNomenclature = data.result;
        const unit_name =
          editNomenclature?.unit_name ||
          units?.find(({ id }) => id === editNomenclature.unit)
            ?.convent_national_view;

        const editId = editNomenclature.id;
        queryClient.setQueriesData("nomenclature", (oldData) => {
          return oldData.map((item) =>
            item.id === editId ? { ...editNomenclature, unit_name } : item
          );
        });
      }
      if (data.target === "nomenclature" && data.action === "delete") {
        const deleteId = data.result.id;
        queryClient.setQueriesData("nomenclature", (oldData) => {
          const newData = oldData.filter((item) =>
            item.id === deleteId ? false : true
          );
          return newData;
        });
      }
      if (data.target === "nomenclature" && data.action === "create") {
        queryClient.setQueriesData("nomenclature", (oldData) => {
          const newData = oldData.concat(
            data?.result?.map((item) => {
              const unit_name = units?.find(
                ({ id }) => id === item.unit
              )?.convent_national_view;
              return { ...item, unit_name };
            })
          );
          return newData;
        });
      }
      if (data.target === "prices" && data.action === "create") {
        const editPrices = data.result;
        const editId = editPrices.id;
        queryClient.setQueriesData("prices", (oldData) => {
          return oldData.map((item) =>
            item.id === editId ? editPrices : item
          );
        });
      }
      if (data.target === "prices" && data.action === "create") {
        queryClient.setQueriesData("prices", (oldData) => {
          const newData = oldData.concat(data.result);
          return newData;
        });
      }
      setChangeCount((prev) => prev++);
    };
  }, [token, units]);

  const addRow = () => {
    setData((prevData) => [
      ...prevData,
      {
        name: "",
        type: "",
        description_short: "",
        description_long: "",
        code: "",
        unit: "",
        category: "",
        manufacturer: "",
        unit_name: "",
      },
    ]);
  };

  const afterChange = async (changes, source) => {
    const coords = hotTableRef?.current?.hotInstance.getSelected(); //[[top, left,bottom,right]]
    const left = coords?.[0]?.[1];
    if (left === -1) return;
    const changeType = getChangeType(changes, priceTypes);
    if (
      source === "edit" ||
      source === "CopyPaste.paste" ||
      source === "Autofill.fill"
    ) {
      if (changes) {
        const updatedData = [...data];
        changes.forEach(([row, prop, oldValue, newValue]) => {
          updatedData[row][prop] = newValue;
          updatedData[row]["updated"] = oldValue !== newValue;
        });
        const createRows = [];
        const editRows = [];
        updatedData
          .filter((row) => row.updated)
          .map(({ updated, manufacturer, ...rest }) => {
            return {
              ...rest,
              manufacturer: +manumafacturesByValue[manufacturer],
              unit: unitsByValue[rest?.unit_name],
              category: +categoriesByValue[rest?.category],
            };
          })
          .forEach((item) => {
            if (typeof item?.id === "number") {
              editRows.push(item);
            } else {
              createRows.push({ ...item });
            }
          });
        const { editPrice = [], createPrice = [] } = getPriceListForEditFetch(
          editRows,
          priceTypes,
          prices
        );

        if (
          editRows.length &&
          (changeType === ChangeTypes.Complex ||
            changeType === ChangeTypes.Nomenclature)
        ) {
          await mutateEditNomenclatures(editRows);
        }
        if (
          createRows.length &&
          (changeType === ChangeTypes.Complex ||
            changeType === ChangeTypes.Nomenclature)
        ) {
          const newNomenclature = await mutateAsyncCreateNomenclature(
            createRows,
            priceTypes
          );
          const createRowsWithId = createRows.map((row, index) => ({
            ...row,
            id: newNomenclature?.data?.[index]?.id,
          }));
          const priceCreateList =
            getPriceListForCreateFetch(createRowsWithId, priceTypes) || [];
          if (
            priceCreateList.length &&
            (changeType === ChangeTypes.Complex ||
              changeType === ChangeTypes.Price)
          ) {
            await mutateCreatePrices(priceCreateList);
          }
        }
        if (
          createPrice.length &&
          (changeType === ChangeTypes.Complex ||
            changeType === ChangeTypes.Price)
        ) {
          await mutateCreatePrices(createPrice);
        }

        if (
          editPrice.length &&
          (changeType === ChangeTypes.Complex ||
            changeType === ChangeTypes.Price)
        ) {
          await mutateEditPrice(editPrice);
        }
      }
    }
  };

  const handleAfterRemoveRow = (_, __, indexes) => {
    const removedIds = indexes
      .map((index) => {
        const nomenclature = nomenclatures[index];
        return nomenclature?.id || null;
      })
      .filter((item) => item !== null);
    if (removedIds.length) {
      mutateDeleteNomenclatures(removedIds);
      const deletePrices = prices
        .filter((price) =>
          removedIds.some((item) => item === price.nomenclature_id)
        )
        .map((item) => item.id);
      if (deletePrices.length) mutateDeletePrices(deletePrices);
    }
  };

  const handleDeleteKeyPress = (event) => {
    event.stopImmediatePropagation();
    if (event.code === "Delete" || event.code === "Backspace") {
      const coords = hotTableRef.current.hotInstance.getSelected(); //[[top, left,bottom,right]]
      const [top, left, bottom] = coords?.[0];
      if (left === -1) {
        const deleteNomenclatures = data
          .slice(top, bottom + 1)
          ?.map(({ id }) => id);
        if (deleteNomenclatures.length) {
          mutateDeleteNomenclatures(deleteNomenclatures);
          const deletePrices = prices
            .filter((price) =>
              deleteNomenclatures.some((item) => item === price.nomenclature_id)
            )
            .map((item) => item.id);
          if (deletePrices.length) mutateDeletePrices(deletePrices);
        }
      }
    }
  };

  return (
    <>
      <div>
        <div
          style={{ display: "flex", flexDirection: "column", height: "100%" }}
        >
          <HotTable
            ref={hotTableRef}
            className={"contragents-table__td htCenter htMiddle"}
            rowHeights={"65px"}
            columnHeaderHeight={"65px"}
            licenseKey="non-commercial-and-evaluation"
            autoRowSize={true}
            autoColumnSize={true}
            rowHeaders={true}
            contextMenu
            data={data}
            rowRenderer={rowRenderer}
            stretchH="all"
            stretchV="all"
            viewportRowRenderingOffset={20}
            onScrollVertically={handleScroll}
            dropdownMenu
            // language="ru-RU"
            // menu={menu}
            filters
            afterDocumentKeyDown={handleDeleteKeyPress}
            afterRemoveRow={handleAfterRemoveRow}
            afterChange={afterChange}
          >
            <HotColumn data="id" title="id" readOnly={true} />
            <HotColumn data="name" title="Название" />
            <HotColumn data="type" title="Тип" />
            <HotColumn data="description_short" title="Короткое описание" />
            <HotColumn data="description_long" title="Описание" />
            <HotColumn data="code" title="Код" />
            <HotColumn
              data="unit_name"
              title="Еденица измерения"
              type="dropdown"
              source={units?.map((item) => item?.convent_national_view)}
            />
            <HotColumn
              data="category"
              title="Категория"
              type="dropdown"
              source={categories?.map((item) => item?.name)}
            />
            <HotColumn
              data="manufacturer"
              title="Производитель"
              type="dropdown"
              source={manufactures?.map((item) => item?.name)}
            />
            {priceTypes?.map(({ name, id }) => (
              <HotColumn data={name} title={name} key={id} />
            ))}
          </HotTable>
        </div>
        <button onClick={addRow}>Добавить</button>
      </div>
    </>
  );
}
