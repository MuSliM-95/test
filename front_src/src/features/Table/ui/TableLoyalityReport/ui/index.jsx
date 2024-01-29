import React, { useContext, useState } from "react";
import { LoyalityReport } from "src/enitities";
import { LoyalityReportContext } from "src/shared";
import { getFilterData } from "../lib/getFilterData";

export default function TableLoyalityReport() {
  const { initialData, pathname } = useContext(LoyalityReportContext);
  const [dataSource, setDataSource] = useState(initialData);
  const [loading, setLoading] = useState(false);

  const handleChanges = async (pagination, filters, sorter, extra) => {
    let newData = dataSource;
    setLoading(true);
    try {
      switch (extra.action) {
        case "filter":
          newData = await getFilterData(filters, pathname);
          break;
        default:
      }
      setLoading(false);
      setDataSource(newData);
    } catch (err) {
      console.info(err);
    }
  };

  return (
    <LoyalityReport
      loading={loading}
      dataSource={dataSource}
      handleChanges={handleChanges}
    />
  );
}
