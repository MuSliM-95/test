/* eslint-disable react-hooks/exhaustive-deps */
import React, { useContext, useMemo, useState } from "react";
import { Table, Button, Popconfirm, Switch, Space, DatePicker, message } from "antd";
import { DeleteOutlined, SearchOutlined } from "@ant-design/icons";
import { EditableCell, EditableRow, WarehousesDocsContext } from "../../../shared";
import { COL_WAREHOUSES_DOCS } from "../model/constants";
import { setColumnCellProps } from "../lib/setCollumnCellProps";
import { EditWarehousesDocs } from "../../../features/Modal";
import { parseTimestamp, searchValueById } from "../../Form";
import axios from "axios";

const { RangePicker } = DatePicker;

export default function WarehousesDocs({
  handleRemove,
  handleSave,
  dataSource,
}) {

  const { organizationsData, warehousesData, docsCount, token } = useContext(WarehousesDocsContext);
  const parseDate = (key) => parseTimestamp(dataSource, key);
  const parseOrganization = (data) => searchValueById(data, organizationsData, "organization");
  const parseWarehouses = (data) => searchValueById(data, warehousesData, "warehouse");

  const newData = useMemo(
    () => parseWarehouses(parseOrganization(parseDate("dated"))),
    [dataSource, organizationsData, warehousesData]
  )

  const [dataSourceRes, setDataSource] = useState(newData);
  const [loading, setLoading] = useState(false);
  const [filterTime, setFilterTime] = useState([]);
  const [count, setCount] = useState(docsCount);

  const getStamps = (datesArr) => {

    if (datesArr) {
      let datefrom = datesArr[0].startOf('day').unix()
      let dateto = datesArr[1].startOf('day').unix()

      setFilterTime([datefrom, dateto])
    }

    else {
      setFilterTime([])
      fetchPage(1, false)
    }
  }

  const find = () => {
    fetchPage(1)
  }

  const findRun = () => {

    setLoading(true)

    let params = { token: token, offset: 0, limit: 100000 }
    let dates = {}

    if (filterTime.length > 0) {
      dates['datefrom'] = filterTime[0]
      dates['dateto'] = filterTime[1]
    }

    axios
      .get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`,
        {
          params: Object.assign({}, params, dates),
        }
      )
      .then((res) => {

        let statusesArr = []
        for (let elem of res.data.result) {
          statusesArr.push({ id: elem.id, status: true })
        }

        axios
          .patch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_docs_warehouse/?token=${token}`,
            statusesArr
          )
          .then((res) => {
            setLoading(false)
            message.info("Вы провели документы за промежуток!")
          });

      });

  }

  const onStatusChange = (newStatus, record) => {
    axios
      .patch(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_docs_warehouse/?token=${token}`,
        [
          {
            id: record.id,
            status: newStatus,
          }
        ]
      )
      .then((res) => {

      });
  }

  const fetchPage = (page, withDates = true) => {
    let limit = 10
    let offset = (page - 1) * limit

    let params = { token: token, offset: offset, limit: limit }
    let dates = {}

    if (filterTime.length > 0 && withDates) {
      dates['datefrom'] = filterTime[0]
      dates['dateto'] = filterTime[1]
    }

    axios
      .get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`,
        {
          params: Object.assign({}, params, dates),
        }
      )
      .then((res) => {

        let newDataRes = parseWarehouses(parseOrganization(parseTimestamp(res.data.result, "dated")))
        setDataSource(newDataRes);
        setLoading(false);
        setCount(res.data.count)
      });
  }

  const columns = useMemo(() => setColumnCellProps(COL_WAREHOUSES_DOCS, {
    warehouse: [
      {
        key: "render",
        action: (_, record) => <>{record.warehouse?.name}</>,
      },
    ],
    organization: [
      {
        key: "render",
        action: (_, record) => <>{record.organization?.short_name}</>,
      },
    ],
    status: [
      {
        key: "render",
        action: (_, record) => <Switch checked={record.status} onChange={(status) => onStatusChange(status, record)} />,
      },
    ],
    dated: [
      {
        key: "render",
        action: (date, record) => {
          const dateFormat = new Date(date);
          return `
          ${dateFormat.getDate()}.${(dateFormat.getMonth() + 1)}.${dateFormat.getFullYear()}
          ${dateFormat.getHours()}:${dateFormat.getMinutes()}:${dateFormat.getSeconds()}
          `
        },
      },
    ],
    operation: [
      {
        key: "render",
        action: (_, record) => _ === "outgoing" ? <font style={{ color: "red" }}>Расходная</font> : <font style={{ color: "green" }}>Приходная</font>,
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record) => (
          <>
            <EditWarehousesDocs record={record} handleSave={handleSave} />
            <Popconfirm
              title={"Подтвердите удаление"}
              onConfirm={() => handleRemove(record.id, [record.id])}
            >
              <Button icon={<DeleteOutlined />} />
            </Popconfirm>
          </>
        ),
      },
    ],
  }), []);

  return (
    <>
      <Space direction="horizontal">
        <RangePicker format={"DD.MM.YYYY"} placeholder={[ "Дата начала", "Дата окончания" ]} style={{ marginLeft: 5 }} onChange={(dates) => getStamps(dates)} />

        <Button onClick={() => find()} htmlType="submit" icon={<SearchOutlined />}>Найти</Button>
        <Button onClick={() => findRun()} htmlType="submit" icon={<SearchOutlined />}>Найти и провести</Button>


      </Space>
      <Table
        columns={columns}
        loading={loading}
        rowKey={(record) => record.id}
        dataSource={dataSourceRes}
        pagination={
          {
            total: count,
            onChange: page => {
              setLoading(true);
              fetchPage(page, filterTime);
            },
            pageSize: 10,
            showSizeChanger: false
          }
        }
        components={{
          body: {
            cell: EditableCell,
            row: EditableRow,
          },
        }}
        bordered
        rowClassName={() => "editable-row"}
        style={{ width: "100%" }}
      />
    </>
  );
}
