import React from "react";
import { AddLoyalitySetting } from "src/widgets/Button";
import { TableLoyalitySetting } from "src/features/Table";
import { LoyalitySettingContext } from "src/shared";
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
