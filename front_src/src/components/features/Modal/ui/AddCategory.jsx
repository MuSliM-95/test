import React, { useContext, useEffect, useState } from "react";
import { Form } from "antd";
import { ModalForm } from "../../../enitities/Modal";
import { CategoryForm } from "../../../enitities/Form";
import { CategoriesContext } from "../../../shared/lib/hooks/context/getCategoriesContext";
import { API } from "../../Table";

export default function AddCategory({ isOpen, setOpen }) {
  const { token, initialData, pathname } = useContext(CategoriesContext);
  const [parent, setParent] = useState(false);
  const [form] = Form.useForm();

  useEffect(() => {
    const parent = [];
    for (let item of initialData) {
      parent.push({ value: item.id, label: item.name });
    }
    setParent(parent);
  }, [initialData]);

  return (
    <ModalForm
      title={"Добавить категорию"}
      width={500}
      isOpen={isOpen}
      setOpen={setOpen}
      formContext={form}
      handleSubmit={API.crud.create(token, pathname)}
    >
      <CategoryForm formContext={form} parent={parent} />
    </ModalForm>
  );
}
