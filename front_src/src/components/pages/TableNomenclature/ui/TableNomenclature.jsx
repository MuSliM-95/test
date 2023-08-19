import React from "react";
import { TableNomenclature } from "../../../features/Table";
import { AddNomenclatureButton } from "../../../widgets/Button";
import { NomenclatureContext } from "../../../shared/lib/hooks/context/getNomenclatureContext";
import { useLocation } from "react-router-dom";

export default function TableCategoriesPage({
  token,
  websocket,
  initialData,
  manufacturersData,
  categoriesData,
  unitsData,
}) {
  const { pathname } = useLocation();
  return (
    <NomenclatureContext.Provider
      value={{
        token,
        websocket,
        initialData,
        manufacturersData,
        categoriesData,
        unitsData,
        pathname,
      }}
    >
      <AddNomenclatureButton />
      <TableNomenclature />
    </NomenclatureContext.Provider>
  );
}
