import React from "react";
import { AddLoyalitySetting } from "src/components/widgets/Button";
import { TableLoyalitySetting } from "src/components/features/Table";
import { LoyalitySettingContext } from "src/components/shared";
import { useLocation } from "react-router-dom";

export default function TableLoyalitySettingPage({
  token,
  params,
  websocket,
  initialData,
  organizationsData,
}) {
  const { pathname } = useLocation();
  return (
    <>
      <LoyalitySettingContext.Provider
        value={{ token, initialData, organizationsData, pathname, websocket, params }}
      >
        <AddLoyalitySetting />
        <TableLoyalitySetting />
      </LoyalitySettingContext.Provider>
    </>
  );
}
