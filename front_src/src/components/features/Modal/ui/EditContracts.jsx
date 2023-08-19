import React, { useCallback, useState } from "react";
import { Form, Button } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import {
  ContractsForm,
  serializeTimestamp,
  serializeDayjs,
} from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";
import { DATE_FIELDS } from "../../../shared";
export default function EditContracts({ record, handleSave }) {
  const [isOpen, setOpen] = useState(false);
  const [submitIsDisabled, setSubmitDisabled] = useState(true);
  const [form] = Form.useForm();
  const newData = useCallback(
    () => serializeDayjs(record, DATE_FIELDS),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [record.dated, record.used_from, record.used_to]
  );
  const handleSubmit = async (data) => {
    const newData = { ...record, ...serializeTimestamp(data[0], DATE_FIELDS) };
    if (typeof newData.contragent !== "number" && newData.contragent !== null)
      newData.contragent = newData.contragent.id;
    if (
      typeof newData.organization !== "number" &&
      newData.organization !== null
    )
      newData.organization = newData.organization.id;
    try {
      await handleSave(newData);
      return true;
    } catch (err) {
      return Promise.reject(err.response?.data.detail[0].msg);
    }
  };

  const handleIsChanges = () => {
    const formDataJSON = JSON.stringify(record);
    const currentDataJSON = JSON.stringify(form.getFieldValue());
    if (formDataJSON !== currentDataJSON) {
      setSubmitDisabled(false);
    } else {
      setSubmitDisabled(true);
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
        submitIsDisabled={submitIsDisabled}
      >
        <ContractsForm
          formContext={form}
          record={newData()}
          handleIsChanges={handleIsChanges}
        />
      </ModalForm>
    </>
  );
}
