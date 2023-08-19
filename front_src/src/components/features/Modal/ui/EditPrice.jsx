import React, { useState, lazy, Suspense } from "react";
import { Form, Button } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { serializeTimestamp } from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";
const PriceForm = lazy(() =>
  import("../../../enitities/Form").then((modal) => ({
    default: modal.PriceForm,
  }))
);

export default function EditPrice({ record, handleSave }) {
  const [isOpen, setOpen] = useState(false);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    const newData = serializeTimestamp(data[0], ["date_from", "date_to"]);
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
        width={500}
        isOpen={isOpen}
        setOpen={setOpen}
        formContext={form}
        handleSubmit={handleSubmit}
      >
        {isOpen ? (
          <Suspense fallback={<div></div>}>
            <PriceForm formContext={form} record={record} />
          </Suspense>
        ) : null}
      </ModalForm>
    </>
  );
}
