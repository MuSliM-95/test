import dayjs from "dayjs";

export default function serializeDayjs(data, key) {
  const copyData = JSON.parse(JSON.stringify(data));
  if (Array.isArray(key)) {
    for (let k of key) {
      copyData[k] = dayjs(data[k]);
    }
    return copyData;
  } else {
    throw Error("Key should be an Array object");
  }
}
