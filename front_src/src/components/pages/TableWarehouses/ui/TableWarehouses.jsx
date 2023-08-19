import React from "react";
import { WarehousesContext } from "../../../shared";
import { useLocation } from "react-router-dom";
import { TableWarehouses } from "../../../features/Table";
import { AddWarehousesButton } from "../../../widgets/Button";

export default function TableWarehousesPage({ initialData, websocket, token }) {
  const { pathname } = useLocation();
  return (
    <WarehousesContext.Provider
      value={{ initialData, websocket, token, pathname }}
    >
      <AddWarehousesButton />
      <TableWarehouses />
    </WarehousesContext.Provider>
  );
}
