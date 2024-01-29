import React from "react";
import { PricesHandsontableContext } from "src/shared";
import { useLocation } from "react-router-dom";
import { TablePricesHandsontable } from "src/features/Table";

export default function TablePricesHandsontablePage({ websocket, token }) {
  const { pathname } = useLocation();
  return (
    <PricesHandsontableContext.Provider
      value={{  websocket, token, pathname }}
    >
      <TablePricesHandsontable />
    </PricesHandsontableContext.Provider>
  );
}
