/* eslint-disable react-hooks/exhaustive-deps */
import React, {
  useContext,
  useState,
  useEffect,
  useCallback,
  useMemo,
} from "react";
import {
  DatePicker,
  Form,
  Input,
  Select,
  Button,
  Table,
  Popconfirm,
  Row,
  Col,
  message,
} from "antd";
import { COL_WAREHOUSES_DOCS_GOODS } from "../../Table/model/constants";
import { WarehousesDocsContext } from "../../../shared";
import { parseTimestamp } from "../../../enitities/Form";
import { API } from "../../../features/Table";
import { setColumnCellProps } from "../../Table/lib/setCollumnCellProps";
import { DeleteOutlined } from "@ant-design/icons";
import { EditableRow, EditableCell } from "../../../shared";
import "./style.css";

export default function WarehousesDocs({
  formContext,
  record,
  handleIsChanges,
  formTableContext,
}) {
  const {
    token,
    pathname,
    unitsData,
    nomenclatureData,
    organizationsData,
    warehousesData,
    priceTypesData,
  } = useContext(WarehousesDocsContext);
  const [newData, setNewData] = useState([]);
  const [units, setUnits] = useState([]);
  const [nomenclatures, setNomenclatures] = useState([]);
  const [organizations, setOrganizations] = useState([]);
  const [warehouses, setWarehouses] = useState([]);
  const [priceType, setPriceType] = useState([]);
  const { TextArea } = Input;
  const formItemLayout = { labelCol: { span: 7 }, wrapperCol: { span: 15 } };
  const editableCell = (options) => (cell) => (record, index) => ({
    record,
    index,
    options: options || [],
    editable: cell.editable,
    dataIndex: cell.dataIndex,
    title: cell.title,
    handleEdit,
  });

  const columns = setColumnCellProps(COL_WAREHOUSES_DOCS_GOODS, {
    price: [
      {
        key: "onCell",
        action: editableCell(),
      },
    ],
    quantity: [
      {
        key: "onCell",
        action: editableCell(),
      },
    ],
    nomenclature: [
      {
        key: "render",
        action: (_, record) => {
          const nomenclature = nomenclatureData.filter(
            (item) => item.id === record.nomenclature
          );
          if (nomenclature.length !== 0) {
            return <span>{nomenclature[0].name}</span>;
          } else {
            return <span>{record.nomenclature}</span>;
          }
        },
      },
    ],
    price_type: [
      {
        key: "onCell",
        action: editableCell(priceType),
      },
      {
        key: "render",
        action: (_, record) => {
          const priceType = priceTypesData.filter(
            (item) => item.id === record.price_type
          );
          if (priceType.length !== 0) {
            return <span>{priceType[0].name}</span>;
          } else {
            return <span>{record.price_type}</span>;
          }
        },
      },
    ],
    unit: [
      {
        key: "render",
        action: (_, record) => {
          const unit = unitsData.filter((item) => item.id === record.unit);
          if (unit.length !== 0) {
            return <span>{unit[0].name}</span>;
          } else {
            return <span>{record.unit}</span>;
          }
        },
      },
    ],
    action: [
      {
        key: "render",
        action: (_, record, index) => (
          <>
            <Popconfirm
              title={"Подтвердите удаление"}
              onConfirm={() => handleRemoveGoods(index)}
            >
              <Button icon={<DeleteOutlined />} />
            </Popconfirm>
          </>
        ),
      },
    ],
  });

  const removeDiff = (data) => {
    const nomeclaturesSelect = [];
    const removeExistGoods = nomenclatureData.filter(
      (item) => !data.goods.some((itemRem) => itemRem.nomenclature === item.id)
    );
    if (removeExistGoods.length !== 0) {
      for (let item of removeExistGoods) {
        nomeclaturesSelect.push({ value: item.id, label: item.name });
      }
    }
    setNomenclatures(nomeclaturesSelect);
  };

  const fetchData = useCallback(async () => {
    if (!!record) {
      const queryData = API.crud.get(token, pathname);
      const dataWithGoods = await queryData(record.id);
      const dataReadableDate = parseTimestamp(dataWithGoods, ["dated"]);
      removeDiff(dataWithGoods);
      formContext.setFieldsValue(dataReadableDate);
      setNewData(dataReadableDate);
    }
  }, [record?.id]);

  useEffect(() => {
    fetchData();
    if (unitsData.length !== 0) {
      const unitsSelect = [];
      for (let item of unitsData) {
        unitsSelect.push({ value: item.id, label: item.name });
      }
      setUnits(unitsSelect);
    }
    if (priceTypesData.length !== 0) {
      const priceTypeSelect = [];
      for (let item of priceTypesData) {
        priceTypeSelect.push({ value: item.id, label: item.name });
      }
      setPriceType(priceTypeSelect);
    }
    if (nomenclatureData.length !== 0) {
      const nomeclaturesSelect = [];
      for (let item of nomenclatureData) {
        nomeclaturesSelect.push({ value: item.id, label: item.name });
      }
      setNomenclatures(nomeclaturesSelect);
    }
    if (organizationsData.length !== 0) {
      const organizationsSelect = [];
      for (let item of organizationsData) {
        organizationsSelect.push({ value: item.id, label: item.short_name });
      }
      setOrganizations(organizationsSelect);
    }
    if (warehousesData.length !== 0) {
      const warehousesSelect = [];
      for (let item of warehousesData) {
        warehousesSelect.push({ value: item.id, label: item.name });
      }
      setWarehouses(warehousesSelect);
    }
  }, [record]);

  const handleAddGoods = (data) => {
    const copyNewData = formContext.getFieldsValue();
    if (copyNewData?.goods !== undefined) {
      copyNewData.goods.push(data);
      formContext.setFieldsValue(copyNewData);
      setNewData(copyNewData);
    } else {
      copyNewData.goods = [data];
      formContext.setFieldsValue(copyNewData);
      setNewData(copyNewData);
    }
    formTableContext.resetFields();
    removeDiff(copyNewData);
  };

  const handleEdit = (data, index) => {
    const copyNewData = formContext.getFieldsValue();
    copyNewData.goods[index] = data;
    formContext.setFieldsValue(copyNewData);
    setNewData(copyNewData);
  };

  const handleRemoveGoods = (index) => {
    const copyNewData = formContext.getFieldsValue();
    copyNewData.goods.splice(index, 1);
    formContext.setFieldsValue(copyNewData);
    setNewData(copyNewData);
    removeDiff(copyNewData);
  };

  const searchPriceById = async (id) => {
    const queryGet = API.crud.get(token, "/prices");
    try {
      const nomenclature = await queryGet(id);
      if (nomenclature?.price) {
        const type = priceTypesData.filter(
          (item) => item.name === nomenclature.price_type
        );
        formTableContext.setFieldValue("price", nomenclature.price);
        formTableContext.setFieldValue("price_type", type[0].id);
      } else {
        formTableContext.setFieldValue("price", "");
        formTableContext.setFieldValue("price_type", "");
        message.warning(
          "Цена по данной номенклатурe не найдена. Введите вручную."
        );
      }
    } catch (err) {
      message.warning("Что-то пошло не так... Попробуйте снова.");
    }
  };

  const fetchNomenclatureList = useMemo(() => {
    const loadOptions = async (input) => {
      const querySearch = API.search(token, "/nomenclature");
      const nomenclatureData = await querySearch({ name: input });
      if (nomenclatureData.length !== 0) {
        const nomeclaturesSelect = [];
        for (let item of nomenclatureData) {
          nomeclaturesSelect.push({ value: item.id, label: item.name });
        }
        setNomenclatures(nomeclaturesSelect);
      }
    };
    return loadOptions;
  }, [nomenclatureData]);

  return (
    <Row>
      <Col flex={"0 0 400px"}>
        <Form
          {...formItemLayout}
          form={formContext}
          initialValues={newData}
          onChange={handleIsChanges}
          layout={"horizontal"}
          style={{ maxWidth: "100%" }}
        >
          <Form.Item label={"Номер"} name={"number"}>
            <Input />
          </Form.Item>
          <Form.Item label={"Операция"} name={"operation"}>
            <Select>
              <Select.Option value="Внутреннее потребление" />
              <Select.Option value="Оприходование излишков" />
              <Select.Option value="Перемещение" />
              <Select.Option value="Списание" />
            </Select>
          </Form.Item>
          <Form.Item label={"Комментарий"} name={"comment"}>
            <TextArea rows={4} />
          </Form.Item>
          <Form.Item label={"Организация"} name={"organization"}>
            <Select options={organizations} />
          </Form.Item>
          <Form.Item label={"Склад"} name={"warehouse"}>
            <Select options={warehouses} />
          </Form.Item>
          <Form.Item label={"От какой даты"} name={"dated"}>
            <DatePicker />
          </Form.Item>
          <Form.Item name={"goods"} initialValue={[]} noStyle></Form.Item>
        </Form>
      </Col>
      <Col flex={"2 0 200px"}>
        <Form
          form={formTableContext}
          onFinish={handleAddGoods}
          layout="inline"
          style={{ width: "100%", marginBottom: "20px" }}
        >
          <Form.Item
            rules={[{ required: true, message: "Заполните поле" }]}
            name={"nomenclature"}
          >
            <Select
              showSearch
              placeholder={"Номенклатура"}
              options={nomenclatures}
              filterOption={false}
              onSearch={fetchNomenclatureList}
              style={{ minWidth: "300px" }}
              onSelect={(id) => searchPriceById(id)}
            />
          </Form.Item>
          <Form.Item
            rules={[{ required: true, message: "Заполните поле" }]}
            name={"quantity"}
          >
            <Input
              placeholder="Количество"
              type={"number"}
              addonAfter={
                <>
                  <Form.Item name={"unit"} noStyle>
                    <Select
                      showSearch
                      filterOption={(input, option) =>
                        (option?.label ?? "")
                          .toLowerCase()
                          .includes(input.toLowerCase())
                      }
                      placeholder={"Единица"}
                      options={units}
                      style={{ width: "150px" }}
                    />
                  </Form.Item>
                </>
              }
            />
          </Form.Item>
          <Form.Item
            rules={[{ required: true, message: "Заполните поле" }]}
            name={"price"}
          >
            <Input
              type={"number"}
              placeholder={"Цена"}
              addonAfter={
                <>
                  <Form.Item name={"price_type"} noStyle>
                    <Select
                      placeholder={"Тип"}
                      options={priceType}
                      style={{ width: "150px" }}
                    />
                  </Form.Item>
                </>
              }
            />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit">
              Добавить продукт
            </Button>
          </Form.Item>
        </Form>
        <Table
          columns={columns}
          dataSource={newData.goods}
          components={{
            body: {
              row: EditableRow,
              cell: EditableCell,
            },
          }}
          bordered
          size="small"
          rowClassName={() => "editable-row"}
        />
      </Col>
    </Row>
  );
}
