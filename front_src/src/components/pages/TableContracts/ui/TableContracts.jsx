import React from "react";
import { useLocation } from "react-router-dom";
import { ContractsContext } from "../../../shared";
import { AddContractsButton } from "../../../widgets/Button";
import { TableContracts } from "../../../features/Table";

export default function TableContractsPage({
  token,
  websocket,
  initialData,
  organizationsData,
  conteragentsData,
}) {
  const { pathname } = useLocation();
  return (
    <ContractsContext.Provider
      value={{
        token,
        websocket,
        initialData,
        pathname,
        organizationsData,
        conteragentsData,
      }}
    >
      <AddContractsButton />
      <TableContracts />
    </ContractsContext.Provider>
  );
}
