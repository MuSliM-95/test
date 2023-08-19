import React, { useMemo } from "react";
import { Table, Button, Popconfirm, Switch, Tooltip } from "antd";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow } from "../../../shared";
import { COL_WAREHOUSES } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import { EditWarehouses } from "../../../features/Modal";

export default function Warehouses({ handleRemove, handleSave, dataSource }) {
  const columns = useMemo(() => setColumnCellProps(COL_WAREHOUSES, {
    address: [
      {
        key: "render",
        action: (record) => (
          <>
            <Tooltip placement="topLeft" title={record}>
              {record}
            </Tooltip>
          </>
        ),
      },
    ],
    description: [
      {
        key: "render",
        action: (record) => (
          <>
            <Tooltip placement="topLeft" title={record}>
              {record}
            </Tooltip>
          </>
        ),
      },
    ],
    status: [
      {
        key: "render",
        action: (checked, record) => (
          <>
            <Switch
              checked={checked}
              onChange={(checked) => {
                record.status = checked;
                handleSave(record);
              }}
            />
          </>
        ),
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditWarehouses record={record} handleSave={handleSave} />
            <Popconfirm
              title={"Подтвердите удаление"}
              onConfirm={() => handleRemove(record.id)}
            >
              <Button icon={<DeleteOutlined />} />
            </Popconfirm>
          </>
        ),
      },
    ],
  }), [handleRemove, handleSave]);

  return (
    <>
      <Table
        columns={columns}
        rowKey={(record) => record.id}
        dataSource={dataSource}
        components={{
          body: {
            cell: EditableCell,
            row: EditableRow,
          },
        }}
        bordered
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
