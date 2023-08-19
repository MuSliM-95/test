import React, { useContext, useState, useEffect } from "react";
import { API } from "../../../shared/api/api";
import { WarehousesDocs } from "../../../enitities/Table";
import { addRow, removeRow, saveRow, WarehousesDocsContext } from "../../../shared";

export default function TableWarehousesDocs() {
  const { token, websocket, initialData, pathname } = useContext(WarehousesDocsContext);
  const [dataSource, setDataSource] = useState(initialData);

  useEffect(() => {
    websocket.onmessage = (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "docs_warehouse") {
        if (data.action === "create") {
          addRow(dataSource, data.result, setDataSource);
        }
        if (data.action === "edit") {
          saveRow(dataSource, data.result[0], setDataSource);
        }
        if (data.action === "delete") {
          removeRow(dataSource, data.result[0].id, setDataSource);
        }
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, dataSource]);

  return (
    <WarehousesDocs
      dataSource={dataSource}
      handleSave={API.crud.edit(token, pathname)}
      handleRemove={API.crud.remove(token, pathname)}
    />
  );
}
