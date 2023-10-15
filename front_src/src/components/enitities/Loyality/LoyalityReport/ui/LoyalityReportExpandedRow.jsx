import React, { useMemo, useState } from "react";
import { Table, Typography } from "antd";
import { COL_ANALYTICS_EXPANDED } from "src/components/enitities/Table/model/constants";
const { Text } = Typography;

export const LoyalityReportExpandedRow = ({ record, onChange }) => {
  const [totalCount, setTotalCount] = useState(0);

  useMemo(() => {
    const totalCount = record.result.reduce((acc, currentValue) => acc + currentValue.day_count, 0);
    setTotalCount(totalCount);
  }, [record.result]);

  return (
    <Table
      columns={COL_ANALYTICS_EXPANDED}
      dataSource={record.result}
      onChange={onChange}
      summary={() => {
        return (
          <Table.Summary.Row>
            <Table.Summary.Cell index={0} />
            <Table.Summary.Cell index={1} align="center">
              Всего за период: <Text>{totalCount}</Text>
            </Table.Summary.Cell>
          </Table.Summary.Row>
        );
      }}
      rowKey={(record) => record.id}
      bordered
      size="small"
      style={{ width: "100%" }}
    />
  );
};
