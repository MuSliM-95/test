import React, { useContext } from "react";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { LoyalitySettingForm, serializeTimestamp } from "../../../enitities/Form";
import { LoyalitySettingContext } from "../../../shared";
import { API } from "../../../shared";

export default function AddLoyalitySetting({ isOpen, setOpen }) {
  const { token, pathname } = useContext(LoyalitySettingContext);
  const [form] = Form.useForm();

  const handleSubmit = async (data) => {
    const queryCreate = API.crud.create(token, pathname);
    const newData = serializeTimestamp(data[0], ["start_period", "end_period"]);
    try {
      await queryCreate(newData);
      return true;
    } catch (err) {
      return Promise.reject(err.message.detail);
    }
  };

  return (
    <ModalForm
      title={"Добавить настройки к карте"}
      width={600}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={handleSubmit}
    >
      <LoyalitySettingForm formContext={form} />
    </ModalForm>
  );
}
