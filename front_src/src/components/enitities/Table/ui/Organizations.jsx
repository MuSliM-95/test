import React, { useCallback, useMemo } from "react";
import { Table, Button, Popconfirm, Tooltip } from "antd";
import { EditOrganizations } from "../../../features/Modal";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow } from "../../../shared";
import { COL_ORGANIZATIONS } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import { parseTimestamp } from "../../Form";

export default function Organizations({
  handleRemove,
  handleSave,
  dataSource,
}) {
  const parseDate = useCallback((key) => parseTimestamp(dataSource, key), [dataSource]);
  const newData = parseDate("registration_date");
  const columns = useMemo(() => setColumnCellProps(COL_ORGANIZATIONS, {
    full_name: [
      {
        key: "render",
        action: (record) => (
          <Tooltip placement="topLeft" title={record}>
            {record}
          </Tooltip>
        ),
      },
    ],
    registration_date: [
      {
        key: "render",
        action: (_, record) => {
          return (
            <p>
              {new Date(record.registration_date).toISOString().split("T")[0]}
            </p>
          );
        },
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditOrganizations record={record} handleSave={handleSave} />
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
        dataSource={newData}
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
