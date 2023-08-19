import React, { useContext, useState, useEffect } from "react";
import { API } from "../../../shared/api/api";
import { Organizations } from "../../../enitities/Table";
import { OrganizationsContext, addRow } from "../../../shared";
import { saveRow, removeRow } from "../../../shared";

export default function TableOrganizations() {
  const { token, websocket, initialData, pathname } = useContext(OrganizationsContext);
  const [dataSource, setDataSource] = useState(initialData);

  useEffect(() => {
    websocket.onmessage = (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "organizations") {
        if (data.action === 'create') {
          addRow(dataSource, data.result, setDataSource);
        }
        if (data.action === "edit") {
          saveRow(dataSource, data.result, setDataSource);
        }
        if (data.action === "delete") {
          removeRow(dataSource, data.result.id, setDataSource);
        }
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, dataSource, websocket.readyState]);

  return (
    <Organizations
      dataSource={dataSource}
      handleSave={API.crud.edit(token, pathname)}
      handleRemove={API.crud.remove(token, pathname)}
    />
  );
}
