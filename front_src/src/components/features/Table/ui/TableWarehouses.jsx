import React, { useContext, useState, useEffect } from "react";
import { API } from "../../../shared/api/api";
import { Warehouses } from "../../../enitities/Table";
import { addRow, removeRow, saveRow, WarehousesContext } from "../../../shared";

export default function TableWarehouses() {
  const { token, websocket, initialData, pathname } = useContext(WarehousesContext);
  const [dataSource, setDataSource] = useState(initialData);

  useEffect(() => {
    websocket.onmessage = (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "warehouses") {
        if (data.action === "create") {
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
  }, [token, dataSource]);

  return (
    <Warehouses
      dataSource={dataSource}
      handleSave={API.crud.edit(token, pathname)}
      handleRemove={API.crud.remove(token, pathname)}
    />
  );
}
