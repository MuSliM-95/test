import React, { useContext } from "react";
import { Form } from "antd";
import { ModalForm } from "src/enitities/Modal";
import { NomenclatureForm } from "src/enitities/Form";
import { NomenclatureContext } from "src/shared/lib/hooks/context/getNomenclatureContext";
import { API } from "../../Table";

export default function AddNomenclature({ isOpen, setOpen }) {
  const { token, pathname } = useContext(NomenclatureContext);
  const [form] = Form.useForm();

  return (
    <ModalForm
      title={"Добавить номенклатуру"}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={API.crud.create(token, pathname)}
    >
      <NomenclatureForm formContext={form} withoutImage={true} />
    </ModalForm>
  );
}
