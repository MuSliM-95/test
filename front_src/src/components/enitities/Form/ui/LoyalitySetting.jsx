import React, { useState, useEffect, useContext } from "react";
import { Form, InputNumber, Select, DatePicker } from "antd";
import { LoyalitySettingContext } from "../../../shared";

// eslint-disable-next-line no-template-curly-in-string
const templateRule = "Поле обязательно к заполнению";

export default function LoyalitySetting({ formContext, record }) {
  const formItemLayout = { labelCol: { span: 12 }, wrapperCol: { span: 11 } };
  const { organizationsData } = useContext(LoyalitySettingContext);
  const [organization, setOrganizations] = useState([]);

  useEffect(() => {
    if (organizationsData.length !== 0) {
      const organizationsSelect = [];
      for (let item of organizationsData) {
        const valueObj = { value: item.id, label: item.short_name };
        organizationsSelect.push(valueObj);
      }
      setOrganizations(organizationsSelect);
    }
  }, [organizationsData, record, formContext]);

  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={record}
      layout={"horizontal"}
      style={{ maxWidth: "100%" }}
    >
      <Form.Item
        label={"Организация"}
        name={"organization"}
        initialValue={record?.organization}
        rules={[{ required: true, message: templateRule }]}
      >
        <Select options={organization} />
      </Form.Item>
      <Form.Item
        label={"Процент кэшбека"}
        name={"cashback_percent"}
        rules={[{ required: true, message: templateRule }]}
      >
        <InputNumber style={{ width: "100%" }} />
      </Form.Item>
      <Form.Item
        label={"Минимальная сумма чека"}
        name={"minimal_checque_amount"}
        rules={[{ required: true, message: templateRule }]}
      >
        <InputNumber style={{ width: "100%" }} />
      </Form.Item>
      <Form.Item
        label={"Максимальный процент начисления"}
        name={"max_percentage"}
        rules={[{ required: true, message: templateRule }]}
      >
        <InputNumber style={{ width: "100%" }} />
      </Form.Item>
      <Form.Item
        label={"Максимальный процент списания"}
        name={"max_withdraw_percentage"}
        rules={[{ required: true, message: templateRule }]}
      >
        <InputNumber style={{ width: "100%" }} />
      </Form.Item>
      <Form.Item
        label={"Действует от"}
        name={"start_period"}
        rules={[{ required: true, message: templateRule }]}
      >
        <DatePicker />
      </Form.Item>
      <Form.Item
        label={"Действует до"}
        name={"end_period"}
        rules={[{ required: true, message: templateRule }]}
      >
        <DatePicker />
      </Form.Item>
    </Form>
  );
}
