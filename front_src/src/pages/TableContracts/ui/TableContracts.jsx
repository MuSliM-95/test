import React from "react";
import { useLocation } from "react-router-dom";
import { ContractsContext } from "src/shared";
import { AddContractsButton } from "src/widgets/Button";
import { TableContracts } from "src/features/Table";

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
