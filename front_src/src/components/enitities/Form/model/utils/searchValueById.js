export default function searchValueById(initialData, filterData, key) {
  if (filterData.length === 0) return initialData;
  const copyInitialData = JSON.parse(JSON.stringify(initialData));
  if (Array.isArray(initialData)) {
    const newData = copyInitialData.map((item) => {
      item[key] = {
        ...filterData.filter((value) => value.id === item[key]),
      }[0];
      return item;
    });
    return newData;
  }
  copyInitialData[key] = { ...filterData.filter((value) => value.id === copyInitialData[key])}
  return copyInitialData;
}
