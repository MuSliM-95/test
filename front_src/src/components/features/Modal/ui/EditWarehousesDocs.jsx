/* eslint-disable react-hooks/exhaustive-deps */
import React, { useState } from "react";
import { Form, Button } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import {
  WarehousesDocsForm,
  serializeTimestamp,
} from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";

export default function EditWarehousesDocs({ record, handleSave }) {
  const [isOpen, setOpen] = useState(false);
  const [form] = Form.useForm();
  const [formTable] = Form.useForm();

  const handleSubmit = async (data) => {
    const newData = [{ ...record, ...serializeTimestamp(data[0], "dated") }];
    try {
      await handleSave(newData);
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
        title={"Редактировать документ"}
        isOpen={isOpen}
        setOpen={setOpen}
        formContext={form}
        formTableContext={formTable}
        handleSubmit={handleSubmit}
        width={1800}
      >
        <WarehousesDocsForm
          record={record}
          formContext={form}
          formTableContext={formTable}
        />
      </ModalForm>
    </>
  );
}
