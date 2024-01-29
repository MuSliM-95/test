import React from "react";
import { WarehousesContext } from "src/shared";
import { useLocation } from "react-router-dom";
import { TableWarehouses } from "src/features/Table";
import { AddWarehousesButton } from "src/widgets/Button";

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
