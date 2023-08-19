import React, { useEffect, useState, useContext, useCallback } from "react";
import { Table, Button, Popconfirm } from "antd";
import { DeleteOutlined } from "@ant-design/icons";
import { COL_LOYALITY_SETTING } from "src/components/enitities/Table/model/constants";
import { EditableCell } from "../lib/EditableCell";
import { EditableRow, LoyalitySettingContext } from "src/components/shared";
import { EditLoyalitySetting } from "src/components/features/Modal";
import { setColumnCellProps } from "src/components/enitities/Table/lib/setCollumnCellProps";
import { parseTimestamp } from "src/components/enitities/Form";

export default function LoyalitySetting({
  loading,
  handleRemove,
  handleSave,
  dataSource,
  handleChanges,
}) {
  const { organizationsData } = useContext(LoyalitySettingContext);
  const [organization, setOrganization] = useState(null);
  const parseDate = useCallback(
    (key) => parseTimestamp(dataSource, key),
    [dataSource]
  );
  const newData = parseDate(["end_period", "start_period"]);

  const editableCell = (cell) => (record, index) => ({
    record,
    index,
    editable: cell.editable,
    dataIndex: cell.dataIndex,
    handleEdit: handleSave,
  });

  useEffect(() => {
    if (organizationsData.length !== 0) {
      const organizationSelect = [];
      for (let item of organizationsData) {
        organizationSelect.push({ value: item.id, text: item.short_name });
      }
      setOrganization(organizationSelect);
    }
  }, [organizationsData]);

  const columns = setColumnCellProps(COL_LOYALITY_SETTING, {
    organization: [
      {
        key: "filters",
        action: organization,
      },
      {
        key: "filterSearch",
        action: true,
      },
      {
        key: "render",
        action: (_, row) => (
          <>
            {
              organization?.filter((item) => item.value === row.organization)[0]
                ?.text
            }
          </>
        ),
      },
    ],
    max_percentage: [
      {
        key: "onCell",
        action: editableCell,
      },
    ],
    max_withdraw_percentage: [
      {
        key: "onCell",
        action: editableCell,
      },
    ],
    minimal_checque_amount: [
      {
        key: "onCell",
        action: editableCell,
      },
    ],
    cashback_percent: [
      {
        key: "onCell",
        action: editableCell,
      },
    ],
    start_period: [
      {
        key: "onCell",
        action: editableCell,
      },
      {
        key: "render",
        action: (_, record) => {
          return (
            <>{new Date(record.start_period).toISOString().split("T")[0]}</>
          );
        },
      },
      // {
      //   key: "filterDropdown",
      //   action: EditableCalendar,
      // },
    ],
    end_period: [
      {
        key: "onCell",
        action: editableCell,
      },
      {
        key: "render",
        action: (_, record) => {
          return <>{new Date(record.end_period).toISOString().split("T")[0]}</>;
        },
      },
      // {
      //   key: "filterDropdown",
      //   action: EditableCalendar,
      // },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditLoyalitySetting record={record} handleSave={handleSave} />
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
  });

  return (
    <>
      <Table
        loading={loading}
        columns={columns}
        rowKey={(record) => record.id}
        dataSource={newData}
        onChange={handleChanges}
        components={{
          body: {
            cell: EditableCell,
            row: EditableRow,
          },
        }}
        bordered
        size="small"
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
