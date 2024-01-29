export default function addRow(dataSource, data, setData) {
  const newData = JSON.parse(JSON.stringify(dataSource));
  newData.push(data[0]);
  setData(newData);
}
