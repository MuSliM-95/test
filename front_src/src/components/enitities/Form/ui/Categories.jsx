import React from "react";
import { Form, Input, Select } from "antd";

export default function Categories({ formContext, record, parent }) {
  const { TextArea } = Input;
  const formItemLayout = { labelCol: { span: 5 }, wrapperCol: { span: 17 } };
  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={record}
      layout={"horizontal"}
      style={{ maxWidth: 600 }}
    >
      <Form.Item label={"Имя"} name={"name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Описание"} name={"description"}>
        <TextArea rows={4} />
      </Form.Item>
      <Form.Item label={"Код"} name={"code"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Родитель"} name={"parent"}>
        <Select options={parent} />
      </Form.Item>
    </Form>
  );
}
