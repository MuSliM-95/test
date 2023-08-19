/* eslint-disable react-hooks/exhaustive-deps */
import React, { useMemo, useContext } from "react";
import { Table, Button, Popconfirm, Switch } from "antd";
import { EditContracts } from "../../../features/Modal";
import { DeleteOutlined } from "@ant-design/icons";
import { ContractsContext, EditableCell, EditableRow } from "../../../shared";
import { COL_CONTRACTS } from "../model/constants";
import { DATE_FIELDS } from "../../../shared";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import { parseTimestamp, searchValueById } from "../../Form";
import { serializeTimestamp } from "../../Form";

export default function Contracts({ handleRemove, handleSave, dataSource }) {
  const { organizationsData, conteragentsData } = useContext(ContractsContext);

  const parseDate = (data, key) => parseTimestamp(data, key);
  const parseOrganization = (data) => searchValueById(data, organizationsData, "organization");
  const parseContragent = (data) => searchValueById(data, conteragentsData, "contragent");

  const newData = useMemo(
    () => parseOrganization(parseContragent(parseDate(dataSource, DATE_FIELDS))),
    [dataSource, organizationsData, conteragentsData]
  );

  const handleCheck = (record, checked) => {
    record.status = checked;
    const parsedDate = serializeTimestamp(record, DATE_FIELDS);
    parsedDate.contragent = parsedDate.contragent?.id || null;
    parsedDate.organization = parsedDate.organization?.id || null;
    handleSave(parsedDate);
  };

  const columns = useMemo(() => setColumnCellProps(COL_CONTRACTS, {
    contragent: [
      {
        key: "render",
        action: (_, record) => {
          return <>{record.contragent?.name}</>;
        },
      },
    ],
    organization: [
      {
        key: "render",
        action: (_, record) => {
          return <>{record.organization?.short_name}</>;
        },
      },
    ],
    used_to: [
      {
        key: "render",
        action: (_, record) => {
          return <p>{new Date(record.used_to).toISOString().split("T")[0]}</p>;
        },
      },
    ],
    used_from: [
      {
        key: "render",
        action: (_, record) => {
          return (
            <p>{new Date(record.used_from).toISOString().split("T")[0]}</p>
          );
        },
      },
    ],
    dated: [
      {
        key: "render",
        action: (_, record) => {
          return <p>{new Date(record.dated).toISOString().split("T")[0]}</p>;
        },
      },
    ],
    status: [
      {
        key: "render",
        action: (checked, record) => (
          <>
            <Switch
              checked={checked}
              onChange={(checked) => handleCheck(record, checked)}
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
            <EditContracts record={record} handleSave={handleSave} />
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
