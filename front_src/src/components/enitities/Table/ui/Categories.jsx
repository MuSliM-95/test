import React, { useMemo } from "react";
import { Table, Button, Popconfirm, Switch } from "antd";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow } from "../../../shared";
import { COL_CATEGORIES } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import EditCategory from "../../../features/Modal/ui/EditCategory";

export default function Categories({ handleRemove, handleSave, dataSource }) {
  const columns = useMemo(() => setColumnCellProps(COL_CATEGORIES, {
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
            <EditCategory record={record} handleSave={handleSave} />
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
  }), [handleRemove, handleSave])

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
