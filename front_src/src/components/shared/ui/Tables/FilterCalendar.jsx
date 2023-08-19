import { Calendar, Space, Button } from "antd";

export const FilterCalendar = ({
  selectedKeys,
  setSelectedKeys,
  confirm,
  clearFilters,
}) => {
  return (
    <div style={{ width: 300 }}>
      <Calendar
        fullscreen={false}
        mode="month"
        headerRender={() => null}
        onChange={(e) => {
          setSelectedKeys([e]);
          confirm(true);
        }}
        allowClear={true}
      />
      <Space
        style={{
          width: "100%",
          padding: "7px 8px",
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
