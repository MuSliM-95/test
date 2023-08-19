import React, { useState } from "react";
import { Form, Button } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { WarehousesForm } from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";

export default function EditWarehouses({ record, handleSave }) {
  const [isOpen, setOpen] = useState(false);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    data[0].id = record.id;
    data[0].status = record.status;
    try {
      await handleSave(data[0]);
      return true;
    } catch (err) {
      return Promise.reject(err.response?.data.detail[0].msg);
    }
  };

  return (
    <>
      <Button
        icon={<EditOutlined />}
        onClick={() => setOpen(true)}
        style={{ marginRight: "5px" }}
      />
      <ModalForm
        title={"Добавить магазин/склад/участок"}
        isOpen={isOpen}
        setOpen={setOpen}
        formContext={form}
        handleSubmit={handleSubmit}
      >
        <WarehousesForm record={record} formContext={form} />
      </ModalForm>
    </>
  );
}
