import { useState } from "react";
import { Button, Popconfirm } from "antd";
import { DeleteOutlined } from "@ant-design/icons";

export default function RemoveButton({ callback }){
  const [isVisible, setIsVisible] = useState(false);

  const handleOk = () => {
    callback();
    setIsVisible(false);
  };

  const handleCancel = () => {
    setIsVisible(false);
  };

  return (
    <div style={{marginLeft:8}}>
      <Popconfirm
        title="Вы уверены что хотите удалить закупку?"
        visible={isVisible}
        onConfirm={handleOk}
        onCancel={handleCancel}
        okText="Да"
        cancelText="Нет"
      >
        <Button icon={<DeleteOutlined />} onClick={() => setIsVisible(true)} />
      </Popconfirm>
    </div>
  );
};