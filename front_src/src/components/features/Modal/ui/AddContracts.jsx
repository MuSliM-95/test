import React, { useContext } from "react";
import { API } from "../../Table";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { ContractsForm, serializeTimestamp } from "../../../enitities/Form";
import { ContractsContext } from "../../../shared";

export default function AddContracts({ isOpen, setOpen }) {
  const { token, pathname } = useContext(ContractsContext);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    const newData = [serializeTimestamp(data[0], ["dated", "used_from", "used_to"])];
    const querySave = API.crud.create(token, pathname);
    try {
      await querySave(newData);
      return true;
    } catch (err) {
      return Promise.reject(err.message.detail);
    }
  };

  return (
    <ModalForm
      title={"Добавить контракт"}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={handleSubmit}
    >
      <ContractsForm formContext={form} switchButton={true} />
    </ModalForm>
  );
}
