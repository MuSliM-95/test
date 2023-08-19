import React from "react";
import { Table } from "antd";
import { FilterRangePicker } from "src/components/shared";

import { setColumnCellProps } from "src/components/enitities/Table/lib/setCollumnCellProps";
import { COL_ANALYTICS_CARDS } from "src/components/enitities/Table/model/constants";

export default function LoyalityReport({ loading, dataSource, handleChanges }) {
  // const [tags, setTags] = useState(null);
  // useEffect(() => {
  //   if (newData.length !== 0) {
  //     const tagsSelect = [];
  //     const copyData = JSON.parse(JSON.stringify(newData));
  //     for (let item of copyData) {
  //       if (typeof item.tags !== "string") continue;
  //       const arrayTags = item.tags.split(",");
  //       for (let tag of arrayTags) {
  //         tagsSelect.push({ value: tag, text: tag });
  //       }
  //     }
  //     setTags(tagsSelect);
  //   }
  // }, [newData]);

  const columns = setColumnCellProps(COL_ANALYTICS_CARDS, {
    date: [
      // {
      //   key: "render",
      //   action: (_, record) => {
      //     return <>{new Date(record.date).toISOString().split("T")[0]}</>;
      //   },
      // },
      {
        key: "filterDropdown",
        action: FilterRangePicker,
      },
    ],
    // tags: [
    //   {
    //     key: "filters",
    //     action: tags,
    //   },
    //   {
    //     key: "filterSearch",
    //     action: true,
    //   },
    //   {
    //     key: "render",
    //     action: (tags) => {
    //       if (typeof tags === "string") {
    //         return (
    //           <span>
    //             {tags.split(",").map((tag) => {
    //               return (
    //                 <Tag color={"geekblue"} key={tag}>
    //                   {tag.toUpperCase()}
    //                 </Tag>
    //               );
    //             })}
    //           </span>
    //         );
    //       } else {
    //         return "Тэги не найдены";
    //       }
    //     },
    //   },
    // ],
  });
  return (
    <>
      <Table
        loading={loading}
        columns={columns}
        rowKey={(record) => record.date}
        dataSource={dataSource}
        onChange={handleChanges}
        bordered
        size="small"
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
