import React from "react";
import { WarehousesDocsContext } from "../../../shared";
import { TableWarehousesDocs } from "../../../features/Table";
import { AddWarehousesDocsButton } from "../../../widgets/Button";
import { useLocation } from "react-router-dom";

export default function TableWarehousesDocsPage({
  token,
  websocket,
  initialData,
  nomenclatureData,
  organizationsData,
  warehousesData,
  unitsData,
  priceTypesData,
}) {
  const { pathname } = useLocation();
  
  return (
    <WarehousesDocsContext.Provider
      value={{
        initialData: initialData.result,
        websocket,
        token,
        pathname,
        unitsData,
        nomenclatureData,
        organizationsData,
        warehousesData,
        priceTypesData,
      }}
    >
      <AddWarehousesDocsButton />
      <TableWarehousesDocs />
    </WarehousesDocsContext.Provider>
  );
}
