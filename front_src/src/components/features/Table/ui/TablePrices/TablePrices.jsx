import React, { useContext, useState } from "react";
import { Prices } from "../../../../enitities/Table";
import { PricesContext } from "../../../../shared";
import { addRow, removeRow, saveRow } from "../../../../shared";
import { API } from "../../../../shared";
import { getFilterData } from "./lib/getFilterData";

export default function TablePrices() {
  const { token, initialData, pathname, websocket, params } = useContext(PricesContext);
  const [dataSource, setDataSource] = useState(initialData);
  const [loading, setLoading] = useState(false);
  const handleChanges = async (pagination, filters, sorter, extra) => {
    let newData = dataSource;
    setLoading(true);
    switch (extra.action) {
      case "filter":
        newData = await getFilterData(filters, pathname, params);
        break;
      default:
    }
    setLoading(false);
    setDataSource(newData);
  };

  websocket.onmessage = async (message) => {
    const data = JSON.parse(message.data);
    if (data.target === "prices") {
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

  return (
    <Prices
      loading={loading}
      setLoading={setLoading}
      dataSource={dataSource}
      handleSave={API.crud.edit(token, pathname)}
      handleRemove={API.crud.remove(token, pathname)}
      handleChanges={handleChanges}
    />
  );
}
