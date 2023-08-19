import { useState } from "react"
import { InputNumber } from "antd";
import { RemoveButton } from "../../../../../widgets/Button";

const fields = [
  { label: "Номер", name: "number" },
  {
    label: "От",
    name: "dated",
    isDate: true,
  },
  { label: "Операция", name: "operation" },
  { label: "Комментарий", name: "comment" },
  {
    label: "Клиент",
    name: "client",
    isSelector: true,
  },
  {
    label: "Контрагент",
    name: "contragent",
    isSelector: true,
  },
  {
    label: "Договор",
    name: "contract",
    isSelector: true,
  },
  {
    label: "Организация",
    name: "organization",
    isRequired: true,
    isSelector: true,
  },
  {
    label: "Склад",
    name: "warehouse",
    isSelector: true,
  },
  {
    label: "Кто закупает",
    name: "purchased_by",
    isSelector: true,
  },
];

const goodsFields = [
  {
    label: "Номенклатура",
    name: "nomenclature",
    //isRequired: true,
    isSelector: true,
  },
  {
    label: "Тип цены",
    name: "price_type",
    // isSelector: true,
    isDisabled: true,
  },
  {
    label: "Цена",
    name: "price",
    // isRequired: true,
    // isNumber: true,
    isDisabled: true,
    isNumber: true,
  },
  {
    label: "Количество",
    name: "quantity",
    //isRequired: true,
    isNumber: true,
  },
  {
    label: "Единица",
    name: "unit",
    // isSelector: true,
    isDisabled: true,
  },
];

const prepareForSelector = (arr) =>
  arr?.map(({ id, name, short_name, username }) => ({
    value: id,
    label: name || short_name || username || id,
  })) || [];

const prepareUsersForSelector = (arr) =>
  arr?.map(({ id, last_name, first_name, username }) => ({
    value: id,
    label: (last_name || first_name) ? `${first_name || ''} ${last_name || ""}` : username,
  })) || [];

const InputEditNumber = ({ value, onChange }) => {
  const [active, setActive] = useState(false)
  return <InputNumber value={value} onChange={onChange} bordered={active} controls={active} onFocus={() => setActive(true)} onBlur={() => setActive(false)} />
}

const getGoodsColumns = ({ setEditId, removeGoods, setGoods }) => {
  return [
    {
      title: "Номенклатура",
      dataIndex: "nomenclature",
      key: "nomenclature",
      render: (val) => {
        return val?.label
      }
    },
    {
      title: "Тип цены",
      dataIndex: "price_type",
      key: "price_type",
      render: (val) => val?.label
    },
    {
      title: "Цена",
      dataIndex: "price",
      key: "price",
      render: (val, records) => <InputEditNumber value={val} onChange={(newValue) => {
        const nomenclatureId = records?.nomenclature?.value
        setGoods(prev => {
          return prev.map(item => {
            if (item?.nomenclature?.value === nomenclatureId) {
              return { ...item, price: newValue }
            }
            return item;
          })
        })
      }} />
    },
    {
      title: "Количество",
      dataIndex: "quantity",
      key: "quantity",
      render: (val, records) => <InputEditNumber value={val} onChange={(newValue) => {
        const nomenclatureId = records?.nomenclature?.value
        setGoods(prev => {
          return prev.map(item => {
            if (item?.nomenclature?.value === nomenclatureId) {
              return { ...item, quantity: newValue }
            }
            return item;
          })
        })
      }} />
    },
    {
      title: "Единица",
      dataIndex: "unit",
      key: "unit",
      render: (val) => val?.label
    },
    {
      title: "Действия",
      key: "actions",
      render: (_, __, index) => {

        return (
          <div
            style={{
              display: "flex",
            }}
          >
            <RemoveButton callback={() => removeGoods(index)} />
          </div>
        );
      },
    },
  ];
}

export { fields, goodsFields, getGoodsColumns, prepareForSelector, prepareUsersForSelector };