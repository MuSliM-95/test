import React, { useState } from "react";
import { Form, Button } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { OrganizationsForm, serializeTimestamp } from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";

export default function EditCategory({ record, handleSave }) {
  const [isOpen, setOpen] = useState(false);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    const newData = serializeTimestamp(data[0], 'registration_date');
    newData.id = record.id;
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
        isOpen={isOpen}
        setOpen={setOpen}
        formContext={form}
        handleSubmit={handleSubmit}
      >
        <OrganizationsForm formContext={form} record={record} />
      </ModalForm>
    </>
  );
}
