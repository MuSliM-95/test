import React from "react";
import { useLocation } from "react-router-dom";
import { OrganizationsContext } from "src/shared";
import { AddOrganizationButton } from "src/widgets/Button";
import { TableOrganizations } from "src/features/Table";

export default function TableOrganizationsPage({ token, websocket, initialData }) {
  const { pathname } = useLocation();
  return (
    <OrganizationsContext.Provider value={{ token, websocket, initialData, pathname }} >
      <AddOrganizationButton />
      <TableOrganizations />
    </OrganizationsContext.Provider>
  );
}
