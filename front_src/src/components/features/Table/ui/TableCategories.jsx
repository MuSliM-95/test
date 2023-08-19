import React, { useContext, useState, useEffect } from "react";
import { API } from "../../../shared/api/api";
import { Categories } from "../../../enitities/Table";
import { CategoriesContext } from "../../../shared/lib/hooks/context/getCategoriesContext";
import { addRow, removeRow, saveRow } from "../../../shared";

export default function TableCategories() {
  const { token, websocket, initialData, pathname } = useContext(CategoriesContext);
  const [dataSource, setDataSource] = useState(initialData);

  useEffect(() => {
    websocket.onmessage = (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "categories") {
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
    <Categories
      dataSource={dataSource}
      handleSave={API.crud.edit(token, pathname)}
      handleRemove={API.crud.remove(token, pathname)}
    />
  );
}
