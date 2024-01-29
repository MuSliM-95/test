//страница "Номенклатура"
import React from "react";
import { TableNomenclature } from "src/features/Table";
import { NomenclatureContext } from "src/shared/lib/hooks/context/getNomenclatureContext";
import { useLocation } from "react-router-dom";

export default function TableCategoriesPage({
  token,
  websocket,
  initialData,
  manufacturersData,
  categoriesData,
  nomenclatureDataCount,
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
        nomenclatureDataCount,
      }}
    >
      <TableNomenclature />
    </NomenclatureContext.Provider>
  );
}
