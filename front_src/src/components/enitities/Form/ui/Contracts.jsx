import React, { useContext, useEffect, useState } from "react";
import { DatePicker, Form, Input, Select, Switch } from "antd";
import { ContractsContext } from "../../../shared";

export default function Contracts({
  formContext,
  record,
  handleIsChanges,
  switchButton,
}) {
  const { organizationsData, conteragentsData } = useContext(ContractsContext);
  const [contragents, setContragents] = useState([]);
  const [organizations, setOrganizations] = useState([]);
  const formItemLayout = { labelCol: { span: 7 }, wrapperCol: { span: 13 } };
  const { organization, contragent, ...rest } = record || [];
  useEffect(() => {
    if (conteragentsData.length !== 0) {
      const contragentsSelect = [];
      for (let item of conteragentsData) {
        contragentsSelect.push({ value: item.id, label: item.name });
      }
      setContragents(contragentsSelect);
    }
    if (organizationsData.length !== 0) {
      const organizationsSelect = [];
      for (let item of organizationsData) {
        organizationsSelect.push({ value: item.id, label: item.short_name });
      }
      setOrganizations(organizationsSelect);
    }
  }, [conteragentsData, organizationsData]);
  console.log(record);
  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={rest}
      onValuesChange={handleIsChanges}
      layout={"horizontal"}
      style={{ maxWidth: "100%" }}
    >
      <Form.Item label={"Номер договора"} name={"number"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Название договора"} name={"name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Название договора для печати"} name={"print_name"}>
        <Input />
      </Form.Item>
      <Form.Item label={"От какой даты договор"} name={"dated"}>
        <DatePicker />
      </Form.Item>
      <Form.Item label={"Действует от"} name={"used_from"}>
        <DatePicker />
      </Form.Item>
      <Form.Item label={"Действует до"} name={"used_to"}>
        <DatePicker />
      </Form.Item>
      <Form.Item
        initialValue={record?.contragent?.id}
        label={"Контрагент"}
        name={"contragent"}
      >
        <Select>
          {contragents.map((item) => {
            return (
              <Select.Option key={item.value} value={item.value}>
                {item.label}
              </Select.Option>
            );
          })}
        </Select>
      </Form.Item>
      <Form.Item
        initialValue={record?.organization?.id}
        label={"Организация"}
        name={"organization"}
      >
        <Select>
          {organizations.map((item) => {
            return (
              <Select.Option key={item.value} value={item.value}>
                {item.label}
              </Select.Option>
            );
          })}
        </Select>
      </Form.Item>
      {switchButton ? (
        <Form.Item label={"Статус"} name={"status"}>
          <Switch />
        </Form.Item>
      ) : null}
    </Form>
  );
}
