import React, { useContext, useState } from "react";
import { Form, Button } from "antd";
import { NomenclatureModal } from "../../../enitities/Modal";
import { NomenclatureForm } from "../../../enitities/Form";
import { EditOutlined } from "@ant-design/icons";
import { NomenclatureContext } from "../../../shared/lib/hooks/context/getNomenclatureContext";

export default function EditNomenclature({
  record,
  handleSave,
  handleDeleteImage,
  handleSaveImage,
}) {
  const { unitsData } = useContext(NomenclatureContext);
  const [isOpen, setOpen] = useState(false);
  const [submitIsDisabled, setSubmitDisabled] = useState(true);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    const unit = unitsData.filter((item) => item.id === data[0].unit);
    data[0].id = record.id;
    data[0].unit_name = unit[0].convent_national_view;
    try {
      await handleSave(data[0], "edit");
      setSubmitDisabled(true);
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
      {isOpen ? (
        <NomenclatureModal
          width={800}
          isOpen={isOpen}
          setOpen={setOpen}
          formContext={form}
          handleSubmit={handleSubmit}
          submitIsDisabled={submitIsDisabled}
        >
          <NomenclatureForm
            record={record}
            formContext={form}
            handleDeleteImage={handleDeleteImage}
            handleSaveImage={handleSaveImage}
            handleIsChanges={handleIsChanges}
          />
        </NomenclatureModal>
      ) : (
        false
      )}
    </>
  );
}
