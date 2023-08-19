import React, { useContext, useState, useEffect } from "react";
import { Prices } from "../../../enitities/Table";
import { PricesContext, paramsToString } from "../../../shared";
// import { addRow, removeRow, saveRow } from "../../../shared";
import { API } from "../../../shared";
import dayjs from "dayjs";

export default function TablePrices() {
  const { token, initialData, pathname, websocket, params } = useContext(PricesContext);
  const [dataSource, setDataSource] = useState(initialData);
  const [loading, setLoading] = useState(false);
  const queryPrices = API.crud.get(token, pathname);

  const getFilterData = async (filters) => {
    const { token, ...restParams } = params;
    const filterParams = {
      ...restParams,
      unit: filters.unit_name?.join(),
      category: filters.category_name?.join(),
      price_type_id: filters.price_type?.join(),
      date_to: dayjs(filters.price_finishes).unix() || undefined,
    };
    const paramsString = paramsToString(filterParams);
    window.history.pushState( {}, "", `${pathname}?token=${token}${paramsString}`);
    setLoading(true);
    const newData = await queryPrices(undefined, filterParams);
    setLoading(false);
    setDataSource(newData);
  };

  const handleChanges = (pagination, filters, sorter, extra) => {
    switch (extra.action) {
      case "filter":
        getFilterData(filters);
        break;
      default:
    }
  };
  
  useEffect(() => {
    websocket.onmessage = async (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "prices") {
        // if (data.action === "create") {
        //   addRow(dataSource, data.result, setDataSource);
        // }
        // if (data.action === "edit") {
        //   saveRow(dataSource, data.result, setDataSource);
        // }
        // if (data.action === "delete") {
        //   removeRow(dataSource, data.result.id, setDataSource);
        // }
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, dataSource]);

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
