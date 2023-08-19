import React from "react";
import { useLocation } from "react-router-dom";
import { OrganizationsContext } from "../../../shared";
import { AddOrganizationButton } from "../../../widgets/Button";
import { TableOrganizations } from "../../../features/Table";

export default function TableOrganizationsPage({ token, websocket, initialData }) {
  const { pathname } = useLocation();
  return (
    <OrganizationsContext.Provider value={{ token, websocket, initialData, pathname }} >
      <AddOrganizationButton />
      <TableOrganizations />
    </OrganizationsContext.Provider>
  );
}
