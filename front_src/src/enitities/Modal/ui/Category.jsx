import React from "react";
import { Modal } from "antd";

export default function CategoryModal({
  formContext,
  children,
  isOpen,
  setOpen,
  handleSubmit,
}) {
  const handleOk = () => {
    formContext
      .validateFields()
      .then((values) => {
        formContext.resetFields();
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
      title="Добавить категорию"
      onOk={() => handleOk()}
      onCancel={() => setOpen(false)}
    >
      {children}
    </Modal>
  );
}
