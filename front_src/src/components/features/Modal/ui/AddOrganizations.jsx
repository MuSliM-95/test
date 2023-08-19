import React, { useContext } from "react";
import { API } from "../../Table";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { OrganizationsForm, serializeTimestamp } from "../../../enitities/Form";
import { OrganizationsContext } from "../../../shared/lib/hooks/context/getOrganizationsContext";

export default function AddOrganizations({ isOpen, setOpen }) {
  const { token, pathname } = useContext(OrganizationsContext);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    const queryCreate = API.crud.create(token, pathname);
    const newData = serializeTimestamp(data[0], "registration_date");
    try {
      await queryCreate(newData);
      return true;
    } catch (err) {
      return Promise.reject(err.message.detail);
    }
  };

  return (
    <ModalForm
      title={"Добавить организацию"}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={handleSubmit}
    >
      <OrganizationsForm formContext={form} />
    </ModalForm>
  );
}
