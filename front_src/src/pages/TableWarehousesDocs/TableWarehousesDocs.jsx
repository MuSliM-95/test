import React from "react";
import { WarehousesDocsContext } from "src/shared";
import { TableWarehousesDocs } from "src/features/Table";
import { AddWarehousesDocsButton } from "src/widgets/Button";
import { useLocation } from "react-router-dom";

export default function TableWarehousesDocsPage({
  token,
  websocket,
  initialData,
  docsCount,
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
        initialData,
        websocket,
        token,
        docsCount,
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
