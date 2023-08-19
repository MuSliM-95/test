import React, { useContext } from "react";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { PriceForm, serializeTimestamp } from "../../../enitities/Form";
import { PricesContext } from "../../../shared";
import { API } from "../../../shared";

export default function AddCategory({ isOpen, setOpen }) {
  const { token, pathname } = useContext(PricesContext);
  const [form] = Form.useForm();

  const handleSubmit = async (data) => {
    const queryCreate = API.crud.create(token, pathname);
    const newData = serializeTimestamp(data[0], ["date_from", "date_to"]);
    try {
      await queryCreate([newData]);
      return true;
    } catch (err) {
      return Promise.reject(err.message.detail);
    }
  };

  return (
    <ModalForm
      title={"Добавить категорию"}
      width={500}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={handleSubmit}
    >
      <PriceForm formContext={form} />
    </ModalForm>
  );
}
