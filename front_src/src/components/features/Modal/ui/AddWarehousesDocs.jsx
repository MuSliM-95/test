import React, { useContext } from "react";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import {
  WarehousesDocsForm,
  serializeTimestamp,
} from "../../../enitities/Form";
import { WarehousesDocsContext } from "../../../shared";
import { API } from "../../Table";

export default function AddWarehousesDocs({ isOpen, setOpen }) {
  const { token, pathname } = useContext(WarehousesDocsContext);
  const [form] = Form.useForm();
  const [formTable] = Form.useForm();

  const handleSubmit = async (data) => {
    const queryCreate = API.crud.create(token, pathname);
    const newData = serializeTimestamp(data[0], "dated");
    try {
      await queryCreate([newData]);
      return true;
    } catch (err) {
      return Promise.reject(err.response?.data.detail[0].msg);
    }
  };

  return (
    <ModalForm
      title={"Добавить документ"}
      width={1800}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={handleSubmit}
    >
      <WarehousesDocsForm formContext={form} formTableContext={formTable} />
    </ModalForm>
  );
}
