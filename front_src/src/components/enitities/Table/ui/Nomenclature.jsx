import React, { useMemo } from "react";
import { Table, Button, Popconfirm, Tooltip } from "antd";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow, PreviewImage } from "../../../shared";
import { EditNomenclatureModal } from "../../../features/Modal/";
import { COL_NOMENCLATURE } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";

export default function Nomenclature({
  handleRemove,
  handleSave,
  handleSaveImage,
  handleDeleteImage,
  dataSource,
}) {
  const columns = useMemo(() => setColumnCellProps(COL_NOMENCLATURE, {
    name: [
      {
        key: "render",
        action: (record) => (
          <Tooltip placement="topLeft" title={record}>
            {record}
          </Tooltip>
        ),
      },
    ],
    description_short: [
      {
        key: "render",
        action: (record) => (
          <Tooltip placement="topLeft" title={record}>
            {record}
          </Tooltip>
        ),
      },
    ],
    description_long: [
      {
        key: "render",
        action: (record) => (
          <Tooltip placement="topLeft" title={record}>
            {record}
          </Tooltip>
        ),
      },
    ],
    pictures: [
      {
        key: "render",
        action: (record) => <PreviewImage items={record} witdh={100} height={190}/>,
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditNomenclatureModal
              record={record}
              handleSave={handleSave}
              handleSaveImage={handleSaveImage}
              handleDeleteImage={handleDeleteImage}
            />
            <Popconfirm
              title={"Подтвердите удаление"}
              onConfirm={() => handleRemove(record.id, dataSource)}
            >
              <Button icon={<DeleteOutlined />} />
            </Popconfirm>
          </>
        ),
      },
    ],
  }), [
    dataSource,
    handleDeleteImage,
    handleRemove,
    handleSave,
    handleSaveImage
  ]);

  return (
    <>
      <Table
        columns={columns}
        rowKey={(record) => record.id}
        dataSource={dataSource}
        components={{
          body: {
            cell: EditableCell,
            row: EditableRow,
          },
        }}
        // loading={loading}
        // pagination={{
        //   pageSize: 20,
        //   onChange: async (page, pageSize) => {
        //     setLoading(true);
        //     const res = await queryOffsetData(page, pageSize);
        //     console.log(res);
        //   },
        // }}
        bordered
        size="small"
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
