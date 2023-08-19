import React from "react";
import { Route, Routes } from "react-router-dom";
import { Home, Payments } from "./pages";
import Sidebar from "./widgets/Sidebar/Sidebar";
import { Layout } from "antd";

const App: React.FC = () => {
  return (
    <>
      <Layout className="layout">
        <Sidebar />
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/payments" element={<Payments />} />
        </Routes>
      </Layout>
    </>
  );
};

export default App;
