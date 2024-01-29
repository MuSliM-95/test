import React from "react";
import { Modal } from "antd";

export default function NomenclatureModal({
  formContext,
  children,
  isOpen,
  setOpen,
  handleSubmit,
  submitIsDisabled,
}) {
  const handleOk = () => {
    formContext
      .validateFields()
      .then((values) => {
        // formContext.resetFields();
        handleSubmit([values]);
        setOpen(false);
      })
      .catch((info) => console.log(info));
  };

  return (
    <Modal
      open={isOpen}
      centered
      forceRender
      width={800}
      title="Добавить номенклатуру"
      onOk={() => handleOk()}
      okButtonProps={{
        disabled: submitIsDisabled,
      }}
      onCancel={() => setOpen(false)}
    >
      {children}
    </Modal>
  );
}
