import React from "react";
import { TableLoyalityReport } from "src/components/features/Table";
import { LoyalityReportContext } from "src/components/shared";
import { useLocation } from "react-router-dom";

export default function TableCategoriesPage({
  token,
  websocket,
  initialData,
  params,
}) {
  const { pathname } = useLocation();
  return (
    <>
      <LoyalityReportContext.Provider
        value={{
          token,
          params,
          initialData,
          pathname,
          websocket,
        }}
      >
        <TableLoyalityReport />
      </LoyalityReportContext.Provider>
    </>
  );
}
