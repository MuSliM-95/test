import React from "react";
import { Table } from "antd";
import { FilterRangePicker } from "src/components/shared";
import { setColumnCellProps } from "src/components/enitities/Table/lib/setCollumnCellProps";
import { COL_ANALYTICS_EXPANDED } from "src/components/enitities/Table/model/constants";

export const LoyalityReportExpandedRow = ({ record, onChange }) => {
  const columns = setColumnCellProps(COL_ANALYTICS_EXPANDED, {
    date: [
      {
        key: "filterDropdown",
        action: FilterRangePicker,
      },
    ],
  });

  return (
    <Table
      columns={columns}
      dataSource={record.result}
      onChange={onChange}
      rowKey={(record) => record.id}
      bordered
      size="small"
      style={{ width: "100%" }}
    />
  );
};
