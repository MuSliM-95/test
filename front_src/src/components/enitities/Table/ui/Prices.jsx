import React, { useCallback, useState, useContext, useEffect } from "react";
import { Table, Calendar, Button, Popconfirm, Space } from "antd";
import { EditPrice } from "../../../features/Modal";
import { DeleteOutlined } from "@ant-design/icons";
import { COL_PRICES } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import { PricesContext } from "../../../shared/index";
import { parseTimestamp } from "../../Form";

export default function Prices({
  loading,
  handleRemove,
  handleSave,
  handleChanges,
  dataSource,
}) {
  const { categoriesData, priceTypesData, unitsData } = useContext(PricesContext);
  const [categories, setCategories] = useState(null);
  const [pirceType, setPriceType] = useState(null);
  const [units, setUnits] = useState(null);
  const parseDate = useCallback(
    (key) => parseTimestamp(dataSource, key),
    [dataSource]
  );
  const newData = parseDate("price_finishes");

  useEffect(() => {
    if (unitsData.length !== 0) {
      const unitsSelect = [];
      for (let item of unitsData) {
        unitsSelect.push({ value: item.id, text: item.name });
      }
      setUnits(unitsSelect);
    }
    if (priceTypesData.length !== 0) {
      const priceTypeSelect = [];
      for (let item of priceTypesData) {
        priceTypeSelect.push({ value: item.id, text: item.name });
      }
      setPriceType(priceTypeSelect);
    }
    if (categoriesData.length !== 0) {
      const categoriesSelect = [];
      for (let item of categoriesData) {
        categoriesSelect.push({ value: item.id, text: item.name });
      }
      setCategories(categoriesSelect);
    }
  }, [categoriesData, priceTypesData, unitsData]);

  const columns = setColumnCellProps(COL_PRICES, {
    unit_name: [
      {
        key: "filters",
        action: units,
      },
      {
        key: "filterSearch",
        action: true,
      },
    ],
    category_name: [
      {
        key: "filters",
        action: categories,
      },
      {
        key: "filterSearch",
        action: true,
      },
    ],
    price: [
      {
        key: 'sorter',
        action: (a, b) => a.price - b.price,
      }
    ],
    price_type: [
      {
        key: "filters",
        action: pirceType,
      },
    ],
    price_finishes: [
      {
        key: "render",
        action: (_, record) => {
          return (
            <p>{new Date(record.price_finishes).toISOString().split("T")[0]}</p>
          );
        },
      },
      {
        key: "filterDropdown",
        action: ({ selectedKeys, setSelectedKeys, confirm, clearFilters }) => (
          <div style={{ width: 300 }}>
            <Calendar
              fullscreen={false}
              mode="month"
              headerRender={() => null}
              onChange={(e) => {
                setSelectedKeys([e]);
                confirm(true);
              }}
              allowClear={true}
            />
            <Space style={{
                width: '100%',
                padding: '7px 8px',
                borderTop: '1px solid #f0f0f0',
                color: 'rgba(0, 0, 0, 0.25)'
              }}>
              <Button
                type="link"
                size="small"
                disabled={selectedKeys.length === 0}
                onClick={() => {
                  clearFilters();
                  confirm({ closeDropdown: false });
                }}
              >
                Reset
              </Button>
            </Space>
          </div>
        ),
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditPrice record={record} handleSave={handleSave} />
            <Popconfirm
              title={"Подтвердите удаление"}
              onConfirm={() => {
                console.log(record.id)
                handleRemove(record.id) }}
            >
              <Button icon={<DeleteOutlined />} />
            </Popconfirm>
          </>
        ),
      },
    ],
  });

  return (
    <>
      <Table
        loading={loading}
        columns={columns}
        rowKey={(record) => record.id}
        dataSource={newData}
        onChange={handleChanges}
        bordered
        size="small"
        style={{ width: "100%" }}
      />
    </>
  );
}
