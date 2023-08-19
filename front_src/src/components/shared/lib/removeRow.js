export default function removeRow(dataSource, id, setData) {
  const copyDataSource = JSON.parse(JSON.stringify(dataSource));
  const newData = copyDataSource.filter((item) => item.id !== id);
  setData(newData);
}
