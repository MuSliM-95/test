export default function saveRow(dataSource, row, setData) {
  const newData = JSON.parse(JSON.stringify(dataSource));
  const index = newData.findIndex((item) => item.id === row.id);
  newData[index] = { ...row };
  setData(newData);
}
