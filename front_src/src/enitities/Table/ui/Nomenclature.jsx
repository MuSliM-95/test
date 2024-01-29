import React, { useMemo } from "react";
import { Table, Button, Popconfirm, Tooltip } from "antd";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow, PreviewImage } from "src/shared";
import { EditNomenclatureModal } from "src/features/Modal/";
import { COL_NOMENCLATURE } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";

export default function Nomenclature({
  handleRemove,
  total,
  handleSave,
  handleSaveImage,
  handleDeleteImage,
  queryOffsetData,
  dataSource,
  name,
  page, pageSize,
}) {
  const columns = useMemo(() => setColumnCellProps(COL_NOMENCLATURE, {
    type: [
      {
        key: "render",
        action: (record) => {
          if (!record) {
            return 'Не указано'
          }

          if (record === 'product') {
            return "Товар"
          }

          if (record === 'service') {
            return 'Услуга'
          }

          return record

        }
      }
    ],
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
        action: (record) => <PreviewImage items={record} witdh={100} height={190} />,
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
        pagination={{
          current: page,
          pageSize: pageSize,
          total: total,
          showSizeChanger: true,
          onShowSizeChange: async (_, size) => await queryOffsetData(page, size, name),
          onChange: async (page, pageSize) => await queryOffsetData(page, pageSize, name)
        }}
        bordered
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
