import { axiosInstance } from "./axiosInstance";
import { message } from "antd";

export const API = {
  pictures: {
    removeImage: (token) => async (id) =>
      await axiosInstance
        .delete(`/pictures/${id}?token=${token}`)
        .then(() => message.success("Изображение было удалено"))
        .catch((err) => Promise.reject(err)),
  },
  crud: {
    create: (token, url) => async (data) => {
      await axiosInstance
        .post(`${url}/?token=${token}`, data)
        .then((result) => {
          message.success("Вы успешно создали строку");
          return result;
        })
        .catch((err) => Promise.reject(err));
    },
    edit: (token, url) => async (data, id) =>
      await axiosInstance
        .patch(`${url}/${data.id || id}?token=${token}`, data)
        .then((result) => {
          message.success("Строка была успешнo отредактирована");
          return result;
        })
        .catch((err) => Promise.reject(err)),
    editTwo: (token, url) => async (data) =>
      await axiosInstance
        .patch(`${url}/?token=${token}`, data)
        .then((result) => {
          message.success("Строка была успешнo отредактирована");
          return result;
        })
        .catch((err) => Promise.reject(err)),
    remove: (token, url) => async (id, data) =>
      await axiosInstance
        .delete(
          `${url}/${id}?token=${token}`,
          !!data ? { data: data } : undefined
        )
        .then(() => message.success("Строка была успешно удалена"))
        .catch((err) => Promise.reject(err)),
    get: (token, url) => async (id, params) =>
      await axiosInstance
        .get(`${url}/${id || ""}?token=${token}`, { params: params || "" })
        .then((item) => item.data)
        .catch((err) => Promise.reject(err)),
  },
  search: (token, url) => async (params) =>
    await axiosInstance
      .get(`${url}/?token=${token}`, { params: params })
      .then((item) => item.data)
      .catch((err) => Promise.reject(err)),
};
