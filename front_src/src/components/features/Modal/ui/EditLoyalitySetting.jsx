import React, { useState, lazy, Suspense } from "react";
import { Form, Button } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { serializeTimestamp } from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";
const LoyalitySettingForm = lazy(() =>
  import("../../../enitities/Form").then((modal) => ({
    default: modal.LoyalitySettingForm,
  }))
);

export default function EditLoyalitySetting({ record, handleSave }) {
  const [isOpen, setOpen] = useState(false);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    const newData = serializeTimestamp(data[0], ["end_period", "start_period"]);
    newData.id = record.id;
    try {
      await handleSave(newData);
      return true;
    } catch (err) {
      return Promise.reject(err.message.detail);
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
        width={600}
        isOpen={isOpen}
        setOpen={setOpen}
        formContext={form}
        handleSubmit={handleSubmit}
      >
        {isOpen ? (
          <Suspense fallback={<div></div>}>
            <LoyalitySettingForm formContext={form} record={record} />
          </Suspense>
        ) : null}
      </ModalForm>
    </>
  );
}
