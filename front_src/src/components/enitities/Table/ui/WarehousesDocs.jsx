/* eslint-disable react-hooks/exhaustive-deps */
import React, { useContext, useMemo } from "react";
import { Table, Button, Popconfirm } from "antd";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow, WarehousesDocsContext } from "../../../shared";
import { COL_WAREHOUSES_DOCS } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import { EditWarehousesDocs } from "../../../features/Modal";
import { parseTimestamp, searchValueById } from "../../Form";

export default function WarehousesDocs({
  handleRemove,
  handleSave,
  dataSource,
}) {
  const { organizationsData, warehousesData } = useContext(WarehousesDocsContext);
  const parseDate = (key) => parseTimestamp(dataSource, key);
  const parseOrganization = (data) => searchValueById(data, organizationsData, "organization");
  const parseWarehouses = (data) => searchValueById(data, warehousesData, "warehouse");
  const newData = useMemo(
    () => parseWarehouses(parseOrganization(parseDate("dated"))),
    [dataSource, organizationsData, warehousesData]
  )
  const columns = useMemo(() => setColumnCellProps(COL_WAREHOUSES_DOCS, {
    warehouse: [
      {
        key: "render",
        action: (_, record) => <>{record.warehouse?.name}</>,
      },
    ],
    organization: [
      {
        key: "render",
        action: (_, record) => <>{record.organization?.short_name}</>,
      },
    ],
    dated: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <p>{new Date(record.dated).toISOString().split("T")[0]}</p>
          </>
        ),
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditWarehousesDocs record={record} handleSave={handleSave} />
            <Popconfirm
              title={"Подтвердите удаление"}
              onConfirm={() => handleRemove(record.id, [record.id])}
            >
              <Button icon={<DeleteOutlined />} />
            </Popconfirm>
          </>
        ),
      },
    ],
  }), []);

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
