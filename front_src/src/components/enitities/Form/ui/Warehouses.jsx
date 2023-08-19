import React, { useContext, useEffect, useState } from "react";
import { Form, Input, Select, Switch } from "antd";
import { WarehousesContext } from "../../../shared";

export default function Warehouses({
  formContext,
  record,
  handleIsChanges,
  switchButton,
}) {
  const { initialData } = useContext(WarehousesContext);
  const [parent, setParent] = useState();
  const { TextArea } = Input;
  const formItemLayout = { labelCol: { span: 5 }, wrapperCol: { span: 17 } };

  useEffect(() => {
    if (initialData.length !== 0) {
      const parentSelect = [];
      for (let item of initialData) {
        parentSelect.push({ value: item.id, label: item.name });
      }
      setParent(parentSelect);
    }
  }, [initialData]);

  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={record}
      onChange={handleIsChanges}
      layout={"horizontal"}
      style={{ maxWidth: "100%" }}
    >
      <Form.Item label={"Название"} name={"name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Тип помещения"} name={"type"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Описание"} name={"description"}>
        <TextArea rows={4} />
      </Form.Item>
      <Form.Item label={"Адрес"} name={"address"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Телефон"} name={"phone"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Родитель"} name={"parent"}>
        <Select options={parent} />
      </Form.Item>
      {switchButton ? (
        <Form.Item label={"Статус"} name={"status"}>
          <Switch />
        </Form.Item>
      ) : null}
    </Form>
  );
}
