import React, { useContext, useState, useEffect } from "react";
import { Form, Button } from "antd";
import { CategoryModal } from "../../../enitities/Modal";
import { CategoryForm } from "../../../enitities/Form";
import { CategoriesContext } from "../../../shared/lib/hooks/context/getCategoriesContext";
import { EditOutlined } from "@ant-design/icons";
import axios from "axios";

export default function EditCategory({ record, handleSave }) {
  const { token } = useContext(CategoriesContext);
  const [isOpen, setOpen] = useState(false);
  const [parent, setParent] = useState(false);
  const [form] = Form.useForm();
  const handleSubmit = async (data) => {
    data[0].id = record.id;
    try {
      await handleSave(data[0]);
      return true;
    } catch (err) {
      return Promise.reject(err.response?.data.detail[0].msg);
    }
  };

  useEffect(() => {
    axios
      .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/categories/`, {
        params: { token },
      })
      .then((res) => {
        const parent = [];
        for (let item of res.data) {
          parent.push({ value: item.id, label: item.name });
        }
        setParent(parent);
      });
  }, [token]);

  return (
    <>
      <Button
        icon={<EditOutlined />}
        onClick={() => setOpen(true)}
        style={{ marginRight: "5px" }}
      />
      <CategoryModal
        isOpen={isOpen}
        setOpen={setOpen}
        formContext={form}
        handleSubmit={handleSubmit}
      >
        <CategoryForm formContext={form} parent={parent} record={record} />
      </CategoryModal>
    </>
  );
}
