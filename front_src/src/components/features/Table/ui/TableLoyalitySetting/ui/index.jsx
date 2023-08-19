import React, { useContext, useState } from "react";
import { API } from "src/components/shared";
import { LoyalitySetting } from "src/components/enitities";
import { LoyalitySettingContext } from "src/components/shared";
import { addRow, saveRow, removeRow } from "src/components/shared";
import { getFilterData } from "../lib/getFilterData";

export default function TableLoyalitySetting() {
  const { token, websocket, initialData, pathname, params } = useContext(LoyalitySettingContext);
  const [dataSource, setDataSource] = useState(initialData || []);
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

  websocket.onmessage = (message) => {
    const data = JSON.parse(message.data);
    if (data.target === "categories") {
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
    <LoyalitySetting
      loading={loading}
      dataSource={dataSource}
      handleSave={API.crud.editTwo(token, pathname)}
      handleRemove={API.crud.remove(token, pathname)}
      handleChanges={handleChanges}
    />
  );
}
