import dayjs from "dayjs";

export default function parseTimestamp(data, key) {
  if (Array.isArray(data)) {
    const copyData = JSON.parse(JSON.stringify(data));
    if (Array.isArray(key)) {
      const newData = copyData.map((item) => {
        for (let k of key) {
          item[k] = dayjs.unix(item[k]);
        }
        return item;
      });
      return newData;
    }
    const newData = copyData.map((item) => {
      item[key] = dayjs.unix(item[key]);
      return item;
    });
    return newData;
  }
  const newData = { ...data, [key]: dayjs.unix(data[key]) };
  return newData;
}
