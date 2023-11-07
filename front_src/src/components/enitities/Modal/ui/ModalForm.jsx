import React from "react";
import { Modal, message } from "antd";

export default function ModalForm({
  formContext,
  width,
  children,
  title,
  isOpen,
  setOpen,
  handleSubmit,
  submitIsDisabled,
}) {
  const handleOk = () => {
    formContext
      .validateFields()
      .then(async (values) => {
        await handleSubmit([values]);
        setOpen(false);
      })
      .catch((info) => {
        console.info(info);
        switch (info) {
          case "field required":
            message.error("Вы не заполнили обязательные поля");
            break;
          case "Цена с таким типом уже существует":
            message.error("Цена с таким типом уже существует");
            break;
          case "value is not a valid integer":
          case "value is not a valid float":
            message.error("Значение не является допустимым числом");
            break;
          default:
            message.error(
              "Упс... Произошла необработанная ошибка. Попробуйте ещё раз"
            );
            break;
        }
      });
  };

  return (
    <Modal
      open={isOpen}
      centered={true}
      forceRender
      width={width || 800}
      title={title}
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
