import dayjs from "dayjs";

export default function serializeTimestamp(data, key) {
  if (Array.isArray(key)) {
    const copyData = JSON.parse(JSON.stringify(data));
    for (let k of key) {
      copyData[k] = dayjs(data[k]).unix();
    }
    return copyData;
  }
  const newData = { ...data, [key]: dayjs(data[key]).unix() };
  return newData;
}
