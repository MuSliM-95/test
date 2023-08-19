import React from "react";
import { PricesContext } from "../../../shared";
import { useLocation } from "react-router-dom";
import { TablePrices } from "../../../features/Table";
import { AddPriceButton } from "../../../widgets/Button";

export default function TablePricesPage({
  websocket,
  token,
  initialData,
  priceTypesData,
  manufacturersData,
  nomenclatureData,
  categoriesData,
  unitsData,
  params
}) {
  const { pathname } = useLocation();
  return (
    <PricesContext.Provider
      value={{
        token,
        params,
        pathname,
        websocket,
        initialData,
        priceTypesData,
        manufacturersData,
        nomenclatureData,
        categoriesData,
        unitsData,
      }}
    >
      <AddPriceButton />
      <TablePrices />
    </PricesContext.Provider>
  );
}
