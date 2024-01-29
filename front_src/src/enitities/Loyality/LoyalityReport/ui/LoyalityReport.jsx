import React, { useMemo, useState } from "react";
import { Table, Collapse, Select, DatePicker, Space } from "antd";
import { LoyalityReportExpandedRow } from "./LoyalityReportExpandedRow";
import { COL_ANALYTICS_CARDS } from "src/enitities/Table/model/constants";

const { Panel } = Collapse;
const { RangePicker } = DatePicker;

export default function LoyalityReport({ loading, dataSource, handleChanges }) {
  const [userData, setUserData] = useState([]);

  useMemo(() => {
    if (dataSource.length !== 0) {
      const userSelect = [];
      for (let item of dataSource) {
        userSelect.push({ value: item.user_id, label: item.first_name });
      }
      setUserData(userSelect);
      return userSelect;
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [handleChanges]);

  const expandedRowRender = (record) => {
    return (
      <LoyalityReportExpandedRow record={record} onChange={handleChanges} />
    );
  };

  const handleSearchByUser = (inputValue) => {
    const filters = {};
    filters.user_id = inputValue;
    handleChanges(null, filters, null, { action: "filter" });
  };

  const handleSearchByDate = (inputValue) => {
    const filters = {};
    filters.date = inputValue;
    handleChanges(null, filters, null, { action: "filter" });
  };

  return (
    <>
      <Collapse ghost={true} style={{ marginBottom: "15px" }}>
        <Panel header="Фильтр" key="1">
          <Space>
            <Select
              allowClear={true}
              showSearch={true}
              options={userData}
              filterOption={(input, options) =>
                (options?.label ?? "")
                  .toLowerCase()
                  .includes(input.toLowerCase())
              }
              onSelect={handleSearchByUser}
              onClear={handleSearchByUser}
              style={{ width: "200px" }}
            />
            <RangePicker
              onChange={(input) => handleSearchByDate(input)}
              onClear={(input) => console.log(input)}
              style={{ width: "250px" }}
            />
          </Space>
        </Panel>
      </Collapse>
      <Table
        loading={loading}
        columns={COL_ANALYTICS_CARDS}
        expandable={{ expandedRowRender }}
        rowKey={(record) => record.user_id}
        dataSource={dataSource}
        onChange={handleChanges}
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
