import React, { useState } from "react";
import { Menu } from "antd";
import { DollarOutlined, HomeOutlined } from "@ant-design/icons";
import Sider from "antd/es/layout/Sider";
import { Link } from "react-router-dom";

const Sidebar: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false);
  return (
    <Sider
      collapsible
      collapsed={collapsed}
      onCollapse={(value) => setCollapsed(value)}
    >
      <div className="demo-logo-vertical" />
      <Menu theme="dark" defaultSelectedKeys={["1"]} mode="inline">
        <Menu.Item>
          <HomeOutlined />
          <span>Главная</span>
          <Link to="/" />
        </Menu.Item>
        <Menu.Item>
          <DollarOutlined />
          <span>Платежи</span>
          <Link to="/payments" />
        </Menu.Item>
      </Menu>
    </Sider>
  );
};

export default Sidebar;
