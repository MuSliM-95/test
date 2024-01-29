import React from "react";
import { DatePicker, Form, Input } from "antd";

export default function Organization({ formContext, record, parent }) {
  const formItemLayout = { labelCol: { span: 5 }, wrapperCol: { span: 17 } };
  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={record}
      layout={"horizontal"}
      style={{ maxWidth: "100%" }}
    >
      <Form.Item label={"Тип"} name={"type"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Тип организации"} name={"org_type"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Короткое имя"} name={"short_name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Полное имя"} name={"full_name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Рабочее имя"} name={"work_name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Префикс"} name={"prefix"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Дата регистрации"} name={"registration_date"}>
        <DatePicker />
      </Form.Item>
      <Form.Item label={"OKVED"} name={"okved"}>
        <Input />
      </Form.Item>
      <Form.Item label={"OKVED2"} name={"okved2"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Тип таксы"} name={"tax_type"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Такса"} name={"tax_percent"}>
        <Input />
      </Form.Item>
      <Form.Item label={"ИНН"} name={"inn"}>
        <Input />
      </Form.Item>
      <Form.Item label={"КПП"} name={"kpp"}>
        <Input />
      </Form.Item>
      <Form.Item label={"OKPO"} name={"okpo"}>
        <Input />
      </Form.Item>
    </Form>
  );
}
