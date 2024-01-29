import { DatePicker, Space, Button } from "antd";
const { RangePicker } = DatePicker;

export const FilterRangePicker = ({
  selectedKeys,
  setSelectedKeys,
  confirm,
  clearFilters,
}) => {
  return (
    <div style={{ width: 300, padding: 8 }}>
      <Space direction="vertical" style={{ width: "100%" }}>
        <RangePicker
          style={{ width: "100%" }}
          onChange={(e) => {
            setSelectedKeys([e]);
            confirm(true);
          }}
          allowClear={false}
        />
      </Space>
      <Space
        style={{
          width: "100%",
          padding: "7px 0",
          borderTop: "1px solid #f0f0f0",
          color: "rgba(0, 0, 0, 0.25)",
        }}
      >
        <Button
          type="link"
          size="small"
          disabled={selectedKeys.length === 0}
          onClick={() => {
            clearFilters();
            confirm({ closeDropdown: false });
          }}
        >
          Reset
        </Button>
      </Space>
    </div>
  );
};
