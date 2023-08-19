import { useEffect, useMemo, useState, useCallback } from "react";
import { Form, Button, Typography, notification, Spin, Table } from "antd";
import {
    fields,
    getGoodsColumns,
    goodsFields,
} from "../../../features/Table/ui/TableDocsPurchases/utils/index";
import { FormItem } from "../../../widgets/Form";
import { getNewGoodsData } from "../../../features/Table/ui/TableDocsPurchases/utils/table";
import { useGetData } from "../../../features/Table/ui/TableDocsPurchases/utils/modal";

const { Title } = Typography;

export default function CreateModal({ isLoadingCreate, isError, form, token, error, goods, setGoods }){
    const [api, contextHolder] = notification.useNotification();
    const [editId, setEditId] = useState(null);
    const {
        dataForSelector,
        data,
        isLoading,
        isErrorDirectory,
        errors
    } = useGetData({ token })

    const { setFields, resetFields } = form;


    const currentNomenclatureId = Form.useWatch('nomenclature', form);
    const currentQuantity = Form.useWatch('quantity', form);
    const newGoodsData = useMemo(() => getNewGoodsData({ data, currentNomenclatureId, currentQuantity }), [data, currentNomenclatureId, currentQuantity])

    useEffect(() => {
        if (isError)
            api.open({
                key: "createError",
                message: "Ошибка создания",
                description: error?.message,
            });
    }, [api, error?.message, isError]);

    useEffect(() => {
        if (isErrorDirectory)
            errors?.forEach((error) => {
                const errorId = Date.now() + Math.floor(Math.random() * 1000);
                api.open({
                    key: errorId,
                    message: "Ошибка загрузки данных",
                    description: error?.message,
                });
            })
    }, [api, errors, isErrorDirectory]);

    useEffect(() => {
        if (newGoodsData)
            setFields([
                { name: "price_type", value: newGoodsData?.price_type?.label },
                { name: "price", value: newGoodsData?.price },
                { name: "unit", value: newGoodsData?.unit?.label },
            ])
    }, [newGoodsData, setFields]);

    useEffect(() => {
        const currentGoods = typeof editId === "number" ? goods[editId] : null;
        if (currentGoods) {
            setFields([
                { name: "price_type", value: currentGoods?.price_type?.label },
                { name: "price", value: currentGoods?.price },
                { name: "unit", value: currentGoods?.unit?.label },
                { name: "nomenclature", value: currentGoods?.nomenclature?.value },
                { name: "quantity", value: currentGoods?.quantity },
            ])
        }
    }, [editId, goods, setFields]);



    const addGoodsToTable = () => {
        setGoods((prev) => {
            if (prev.some(itemSome => itemSome?.nomenclature.value === newGoodsData?.nomenclature.value)) {
              return prev.map(prevItem => prevItem?.nomenclature.value === newGoodsData?.nomenclature.value ?
                { ...prevItem, quantity: prevItem.quantity + newGoodsData.quantity } :
                prevItem)
            }
            return [...prev, newGoodsData]
          })
        resetFields(["nomenclature", "quantity", "price_type", "price", "unit"])
    }

    const removeGoods = useCallback(id => {
        setGoods(goods.filter((_, index) => index !== id))
    }, [goods, setGoods])

    const saveEditGoods = () => {
        setEditId(null)
        setGoods((prev) => prev.map((item, index) => {
            if (editId === index)
                return newGoodsData
            return item;
        }))
        resetFields(["nomenclature", "quantity", "price_type", "price", "unit"])
    }

    const setEditIdHandler = useCallback(id => {
        setEditId(id)
    }, [])

    const goodsColumns = useMemo(() => getGoodsColumns({ setEditId: setEditIdHandler, removeGoods, setGoods }), [setEditIdHandler, removeGoods, setGoods]);

    return (
        <>
            <Spin spinning={isLoading || isLoadingCreate}>
                <Form
                    name="basic"
                    layout="vertical"
                    form={form}
                    labelCol={{ span: 8 }}
                    wrapperCol={{ span: 16 }}
                    style={{ display: "flex", }}
                    autoComplete="off"
                >
                    <div style={{ display: "flex", flexDirection: "column" }}>
                        {fields.map((field) => (
                            <FormItem data={{ ...field, options: dataForSelector?.[field?.name] }} key={field.name} />
                        ))}
                    </div>
                    <div>
                        <Title level={5}>Товары</Title>
                        <div style={{ display: "flex", flexWrap: "wrap", marginBottom: 16 }}>
                            {goodsFields.map((field) => (
                                <FormItem data={{ ...field, options: dataForSelector?.[field?.name] }} key={field.name} />
                            ))}
                            {editId !== null ? <Button onClick={saveEditGoods} disabled={!(currentNomenclatureId && currentQuantity)} style={{ marginTop: "auto" }}>
                                Сохранить изменения
                            </Button> : <Button onClick={addGoodsToTable} disabled={!(currentNomenclatureId && currentQuantity)} style={{ marginTop: "auto" }}>
                                Добавить товар
                            </Button>}
                        </div>
                        <Table
                            columns={goodsColumns}
                            dataSource={goods}
                            pagination={false}
                            loading={isLoading}
                        />
                    </div>
                </Form>
            </Spin>
            {contextHolder}
        </>
    );
};