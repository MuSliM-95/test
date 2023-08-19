import React, { useContext } from "react";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { WarehousesForm } from "../../../enitities/Form";
import { WarehousesContext } from "../../../shared/lib/hooks/context/getWarehousesContext";
import { API } from "../../Table";

export default function AddWarehouses({ isOpen, setOpen }) {
  const { token, pathname } = useContext(WarehousesContext);
  const [form] = Form.useForm();

  return (
    <ModalForm
      title={"Добавить склад/магазин/участок"}
      width={800}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={API.crud.create(token, pathname)}
    >
      <WarehousesForm formContext={form} switchButton={true} />
    </ModalForm>
  );
}
