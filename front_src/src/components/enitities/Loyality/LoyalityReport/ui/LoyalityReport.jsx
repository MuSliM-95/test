import React, { useEffect, useState } from "react";
import { Table } from "antd";
import { LoyalityReportExpandedRow } from "./LoyalityReportExpandedRow";
import { FilterRangePicker } from "src/components/shared";
import { setColumnCellProps } from "src/components/enitities/Table/lib/setCollumnCellProps";
import { COL_ANALYTICS_CARDS } from "src/components/enitities/Table/model/constants";

export default function LoyalityReport({ loading, dataSource, handleChanges }) {
  const [userData, setUserData] = useState([]);

  useEffect(() => {
    if (dataSource.length !== 0) {
      const userSelect = [];
      for (let item of dataSource) {
        userSelect.push({ value: item.user_id, text: item.first_name });
      }
      setUserData(userSelect);
    }
  }, [dataSource]);

  const expandedRowRender = (record) => {
    return (
      <LoyalityReportExpandedRow record={record} onChange={handleChanges} />
    );
  };

  const columns = setColumnCellProps(COL_ANALYTICS_CARDS, {
    first_name: [
      {
        key: "filters",
        action: userData,
      },
      {
        key: "filterSearch",
        action: true,
      },
    ],
    date: [
      {
        key: "filterDropdown",
        action: FilterRangePicker,
      },
    ],
  });

  return (
    <>
      <Table
        loading={loading}
        columns={columns}
        expandable={{ expandedRowRender }}
        rowKey={(record) => record.user_id}
        dataSource={dataSource}
        onChange={handleChanges}
        size="small"
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
