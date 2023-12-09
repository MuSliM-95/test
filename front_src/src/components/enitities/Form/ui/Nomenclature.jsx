import React, { useEffect, useContext, useState } from "react";
import { Form, Input, Select, Upload, Button, message, Radio, AutoComplete } from "antd";
import { NomenclatureContext } from "../../../shared/lib/hooks/context/getNomenclatureContext";
import { API } from "src/components/shared";

import debounce from 'lodash.debounce'

const formItemLayout = { labelCol: { span: 5 }, wrapperCol: { span: 17 } };

export default function Nomenclature({
  formContext,
  record,
  handleSaveImage,
  handleDeleteImage,
  handleIsChanges,
  withoutImage = false,
}) {
  const { manufacturersData = [], categoriesData = [], unitsData = [], token } = useContext(NomenclatureContext);
  const [manufacturers, setManufacturers] = useState([]);
  const [categories, setCategories] = useState([]);
  const [pictures, setPictures] = useState([]);
  const [units, setUnits] = useState([]);

  const [nomenclature, setNomenclature] = useState([])
  const [optionsName, setOptionsName] = useState([])

  const { TextArea } = Input;

  const getNomenclature = API.crud.get(token, '/nomenclature')

  const onChangeName = (e) => {
    setOptionsName(nomenclature.filter((n) => n.value.toLowerCase().includes(e.toLowerCase())))
  }

  const debounceOnChangeName = debounce(onChangeName, 500)

  useEffect(() => {
    if (unitsData.length !== 0) {
      const unitsSelect = [];
      for (let item of unitsData) {
        unitsSelect.push({ value: item.id, label: item.name });
      }
      setUnits(unitsSelect);
    }
    if (manufacturersData.length !== 0) {
      const manufacturersSelect = [];
      for (let item of manufacturersData) {
        manufacturersSelect.push({ value: item.id, label: item.name });
      }
      setManufacturers(manufacturersSelect);
    }
    if (categoriesData.length !== 0) {
      const categoriesSelect = [];
      for (let item of categoriesData) {
        categoriesSelect.push({ value: item.id, label: item.name });
      }
      setCategories(categoriesSelect);
    }
    if (record?.pictures !== undefined && record?.pictures.length !== 0) {
      const picturesDefault = [];
      for (let item of record.pictures) {
        picturesDefault.push({
          id: item.id,
          name: item.url,
          status: "done",
          url: `https://${process.env.REACT_APP_APP_URL}/api/v1/${item.url}/`,
        });
      }
      setPictures(picturesDefault);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [manufacturersData, categoriesData, unitsData]);

  console.log(categories, units)

  useEffect(() => {
    const initial = async () => {
      await getNomenclature().then((res) => setNomenclature(res.result.map((el) => ({ value: el.name }))))
    }

    if (!nomenclature.length) {
      initial()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const filterOption = (input, option) =>
    (option?.label ?? '').toLowerCase().includes(input.toLowerCase());

  return (
    <Form
      {...formItemLayout}
      form={formContext}
      initialValues={record}
      onValuesChange={handleIsChanges}
      layout={"horizontal"}
      style={{ maxWidth: "100%" }}
    >
      <Form.Item label={"Имя"} name={"name"}>
        <AutoComplete options={optionsName} onChange={debounceOnChangeName} />
      </Form.Item>
      <Form.Item label={"Тип"} name={"type"}>
        <Radio.Group>
          <Radio.Button value="product"> Товар </Radio.Button>
          <Radio.Button value="service"> Услуга </Radio.Button>
        </Radio.Group>
      </Form.Item>
      <Form.Item label={"Краткое описание"} name={"description_short"}>
        <TextArea rows={4} />
      </Form.Item>
      <Form.Item label={"Длинное описание"} name={"description_long"}>
        <TextArea rows={4} />
      </Form.Item>
      <Form.Item label={"Код"} name={"code"}>
        <Input />
      </Form.Item>
      <Form.Item label={"Единица измерения"} name={"unit"}>
        <Select filterOption={filterOption} showSearch options={units} />
      </Form.Item>
      <Form.Item label={"Категория"} name={"category"}>
        <Select filterOption={filterOption} showSearch options={categories} />
      </Form.Item>
      <Form.Item label={"Производитель"} name={"manufacturer"}>
        <Select filterOption={filterOption} showSearch options={manufacturers} />
      </Form.Item>
      {!withoutImage ? (
        <Form.Item label={"Изображение"}>
          <Upload
            action={`https://${process.env.REACT_APP_APP_URL}/api/v1/pictures/?token=${token}&entity=nomenclature&entity_id=${record.id}`}
            fileList={pictures}
            onChange={({ fileList: newFileList, file: newFile }) => {
              if (newFile.status === "done") {
                handleSaveImage(newFile.response);
                setPictures(newFileList);
                return message.success("Изображение было добавлено");
              }
              setPictures(newFileList);
            }}
            onRemove={(picture) => {
              if (picture.response !== undefined) {
                return handleDeleteImage(picture.response.id);
              }
              handleDeleteImage(picture.id);
            }}
          >
            <Button>Загрузить</Button>
          </Upload>
        </Form.Item>
      ) : null}
    </Form>
  );
}
