import React, { useState, useEffect, useContext } from "react";
import { PricesContext } from "../../../shared";
import { Form, InputNumber, Select, DatePicker } from "antd";

// eslint-disable-next-line no-template-curly-in-string
const templateRule = "Поле обязательно к заполнению";

export default function Price({ formContext, record }) {
  const formItemLayout = { labelCol: { span: 7 }, wrapperCol: { span: 15 } };
  const { priceTypesData, nomenclatureData } = useContext(PricesContext);
  const [priceTypes, setPriceType] = useState([]);
  const [nomenclature, setNomenclature] = useState([]);

  useEffect(() => {
    if (priceTypesData.length !== 0) {
      const priceTypeSelect = [];
      for (let item of priceTypesData) {
        const valueObj = { value: item.id, label: item.name };
        if (typeof record !== "undefined" && item.name === record.price_type)
          formContext.setFieldValue("price_type", item.id);
        priceTypeSelect.push(valueObj);
      }
      setPriceType(priceTypeSelect);
    }
    if (nomenclatureData.length !== 0) {
      const nomenclatureSelect = [];
      for (let item of nomenclatureData) {
        const valueObj = { value: item.id, label: item.name };
        nomenclatureSelect.push(valueObj);
      }
      setNomenclature(nomenclatureSelect);
    }
  }, [priceTypesData, nomenclatureData, record, formContext]);

  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={record}
      layout={"horizontal"}
      style={{ maxWidth: "100%" }}
    >
      <Form.Item
        label={"Номенклатура"}
        name={"nomenclature"}
        initialValue={record?.nomenclature_name}
        rules={[{ required: true, message: templateRule }]}
      >
        <Select options={nomenclature} />
      </Form.Item>
      <Form.Item
        label={"Цена"}
        name={"price"}
        rules={[{ required: true, message: templateRule }]}
      >
        <InputNumber style={{ width: "100%" }} />
      </Form.Item>
      <Form.Item
        label={"Тип цены"}
        name={"price_type"}
        rules={[{ required: true, message: templateRule }]}
      >
        <Select options={priceTypes} />
      </Form.Item>
      <Form.Item label={"Действует от"} name={"date_from"}>
        <DatePicker />
      </Form.Item>
      <Form.Item label={"Действует до"} name={"date_to"}>
        <DatePicker />
      </Form.Item>
    </Form>
  );
}
