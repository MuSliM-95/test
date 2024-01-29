import React, { useContext, useEffect, useRef, useState } from "react";
import {
    Modal,
    Button,
    Form,
    Input,
    message,
    Layout,
    InputNumber,
    Space,
    Table,
    Popconfirm,
    Select,
    DatePicker
} from "antd";

import {
    ContragentAutocomplete,
    NomenclatureAutoComplete,
    WareHousesAutocomplete,
    OrganizationAutoComplete
} from "src/shared"

import dayjs from "dayjs";

import {
    PlusOutlined,
    MinusOutlined,
    // PercentageOutlined,
    // DollarOutlined,
    DeleteOutlined,
    EditOutlined,
} from "@ant-design/icons";
import axios from "axios";

const { Header, Sider, Content } = Layout;

const headerStyle = {
    textAlign: "center",
    color: "#fff",
    height: 64,
    paddingInline: 50,
    lineHeight: "64px",
    backgroundColor: "white",
};

const contentStyle = {
    textAlign: "center",
    lineHeight: "120px",
    color: "#fff",
    backgroundColor: "white",
};

const siderStyle = {
    textAlign: "center",
    backgroundColor: "white",
};

const EditableContext = React.createContext(null);

const EditableRow = ({ index, ...props }) => {
    const [form] = Form.useForm();
    return (
        <Form form={form} component={false}>
            <EditableContext.Provider value={form}>
                <tr {...props} />
            </EditableContext.Provider>
        </Form>
    );
};

const EditableCell = ({
    title,
    editable,
    children,
    dataIndex,
    record,
    handleSave,
    ...restProps
}) => {
    const [editing, setEditing] = useState(false);
    const inputRef = useRef(null);
    const form = useContext(EditableContext);
    useEffect(() => {
        if (editing) {
            inputRef.current.focus();
        }
    }, [editing]);

    const toggleEdit = () => {
        setEditing(!editing);
        form.setFieldsValue({
            [dataIndex]: record[dataIndex],
        });
    };

    const save = async () => {
        try {
            const values = await form.validateFields();
            toggleEdit();
            handleSave({ ...record, ...values });
        } catch (errInfo) {
            console.log("Save failed:", errInfo);
        }
    };

    let childNode = children;
    let ed = editing ? (
        <Form.Item
            style={{
                margin: 0,
            }}
            name={dataIndex}
            rules={[
                {
                    required: dataIndex !== "amount",
                    message: `${title} обязательно для ввода.`,
                },
            ]}
        >
            <Input ref={inputRef} onPressEnter={save} onBlur={save} />
        </Form.Item>
    ) : (
        <div
            className="editable-cell-value-wrap"
            style={{
                paddingRight: 24,
            }}
            onClick={toggleEdit}
        >
            {children}
        </div>
    );

    if (editable) {
        if (record.type !== "transfer") {
            childNode = ed;
        } else {
            if (dataIndex === "tax") {
                childNode = null;
            } else {
                childNode = ed;
            }
        }
    }

    return <td {...restProps}>{childNode}</td>;
};

class EditDocsWarehouse extends React.Component {
    constructor(props) {
        super(props);

        this.formRef = React.createRef();
        this.formRefNom = React.createRef();
        this.finRef = React.createRef();

        this.state = {
            current_contragent: null,
            discount_type: "percent",
            loyality_balance: 0,
            nomenclature_min: 0,
            nomenclature_max: 0,
            amount_without_discount: 0,
            amount_with_discount: 0,
            amount_discount: 0,
            is_transfer: this.props.doc.operation === "transfer",
            nomDS: [],
        };
        this.api = `https://${process.env.REACT_APP_APP_URL}/api/v1/`;

        this.leftBarIV = {
            number: this.props.doc.number,
            comment: this.props.doc.comment,
            dated: dayjs(this.props.doc.dated * 1000),
            operation: this.props.doc.operation
        };

        this.columns = [
            {
                title: "Название товара",
                key: "name",
                dataIndex: "name",
            },
            {
                title: "Сумма",
                key: "amount",
                dataIndex: "amount",
                editable: true,
            },
            {
                title: "Количество",
                key: "count",
                dataIndex: "count",
                editable: true,
            },
            {
                title: "Единица",
                key: "unit",
                dataIndex: "unit",
            },
            {
                title: "Итого",
                key: "final_amount",
                dataIndex: "final_amount",
            },
            {
                title: "Действие",
                key: "action",
                dataIndex: "action",
                render: (_, record) => {
                    return (
                        <Popconfirm
                            title="Подтвердите удаление"
                            onConfirm={() => this.handleDeleteNom(record.id)}
                            cancelText="Отмена"
                            okText="OK"
                        >
                            <Button icon={<DeleteOutlined />} style={{ marginRight: 10 }} />
                        </Popconfirm>
                    );
                },
            },
        ];
    }

    handleSave = (row) => {
        const newData = [...this.state.nomDS];
        const index = newData.findIndex((item) => row.id === item.id);
        const item = newData[index];

        row.amount = parseFloat(row.amount).toFixed(2);
        row.discount = parseFloat(row.discount).toFixed(2);
        row.count = parseFloat(row.count).toFixed(3);
        row.final_amount = parseFloat(row.amount * row.count).toFixed(2);

        if (row.discount > 0) {
            row.final_amount = parseFloat(row.final_amount - row.discount).toFixed(2);
        }

        newData.splice(index, 1, { ...item, ...row });

        this.setState(
            {
                nomDS: newData,
            },
        );

        // this.edit_request(newData.splice(index, 1, { ...item, ...row })[0]);
    };

    findOrg = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/organizations/${id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
            });
    };

    onSelectOrg = (val) => {
        this.findOrg(val).then((res) => {
            this.setState({ current_organization: res });
            this.formRef.current.setFieldsValue({
                organization: res.short_name,
            });
        });
    };

    findWareHouse = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/warehouses/${id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
            });
    };

    onSelectWareHouse = (val) => {
        this.findWareHouse(val).then((res) => {
            this.setState({ current_warehouse: res });
            this.formRef.current.setFieldsValue({
                warehouse: res.name,
            });
        });
    };

    onSelectWareHouseTo = (val) => {
        this.findWareHouse(val).then((res) => {
            this.setState({ current_warehouse_to: res });
            this.formRef.current.setFieldsValue({
                warehouse_to: res.name,
            });
        });
    };

    findNomenclature = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/${id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
            });
    };

    handleDeleteNom = (id) => {
        const newData = [...this.state.nomDS];
        const index = newData.findIndex((item) => id === item.id);

        if (index !== -1) {
            newData.splice(index, 1);
            this.setState(
                {
                    nomDS: newData,
                }
            );
        }
    };

    onSelectNom = (val) => {
        this.findNomenclature(val).then((res) => {
            this.setState({ current_nomenclature: res });
            this.formRefNom.current.setFieldsValue({
                nomenclature: res.name,
            });

            if (res.type === "product") {
                if (!this.state.current_warehouse) {
                    message.error("Вы не выбрали склад!");
                } else {
                    fetch(
                        `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_warehouse_balances/?token=${this.props.token}&nomenclature_id=${res.id}&warehouse_id=${this.state.current_warehouse.id}`
                    )
                        .then((response) => response.json())
                        .then((body) => {
                            if (body.result.length > 0) {
                                this.formRefNom.current.setFieldsValue({
                                    count: 1,
                                });
                                console.log(body.result.current_amount)
                                this.setState({
                                    nomenclature_min: 1,
                                    nomenclature_max: body.result[0].current_amount,
                                    addNomButtonDisabled: false,
                                });
                            } else {
                                this.formRefNom.current.setFieldsValue({
                                    count: 0,
                                });
                                this.setState({
                                    nomenclature_min: 0,
                                    nomenclature_max: 0,
                                    addNomButtonDisabled: false,
                                });
                            }
                        });
                }
            } else {
                this.formRefNom.current.setFieldsValue({
                    count: 1,
                });
                this.setState({
                    nomenclature_min: 1,
                    nomenclature_max: null,
                    addNomButtonDisabled: false,
                });
            }
        });
    };

    findContragent = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
            });
    };

    onSelectCa = (val) => {
        this.findContragent(val).then((res) => {
            this.setState({ current_contragent: res });
            this.formRef.current.setFieldsValue({
                contragent: res.name,
            });
        });
    };

    changeCount = (val) => {
        let value = this.formRefNom.current.getFieldValue("count");
        let nextVal = 0;

        if (val === "plus") {
            nextVal = value + 1;
        } else {
            nextVal = value - 1;
        }

        //if (nextVal >= this.state.nomenclature_min && nextVal <= this.state.nomenclature_max) {
        if (nextVal >= 0) {
            this.formRefNom.current.setFieldValue("count", nextVal);
        }
    };

    changeDiscount = (val) => {
        if (val === "percent") {
            this.setState({ discount_type: "percent" });
        }

        if (val === "rubles") {
            this.setState({ discount_type: "rubles" });
        }
    };

    addNomenclature = (values) => {
        fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_prices/${this.state.current_nomenclature.id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                if (!body.detail) {
                    let item = {
                        id: this.state.current_nomenclature.id,
                        name: values.nomenclature,
                        amount: body.price?.toFixed(2) || 0,
                        discount: 0,
                        count: values.count,
                        unit: this.state.current_nomenclature.unit_name,
                        final_amount: (
                            parseFloat(body.price) * parseFloat(values.count)
                        ).toFixed(2),
                    };

                    if (!body.price) {
                        item.final_amount = 0;
                    }

                    const DS = [...this.state.nomDS];
                    const index = DS.findIndex(
                        (item) => this.state.current_nomenclature.id === item.id
                    );

                    if (index !== -1) {
                        const item = DS[index];
                        //if (item.count < this.state.nomenclature_max) {
                        item.count = parseInt(item.count + values.count);
                        item.final_amount = (
                            parseFloat(item.count) * parseFloat(item.amount) -
                            parseFloat(item.discount)
                        ).toFixed(2);
                        this.setState({ nomDS: DS });
                        //}
                        //else {
                        //    message.error("На складе нет такого количества")
                        //}
                    } else {
                        DS.unshift(item);
                        this.setState({ nomDS: DS });
                    }
                }
                else {
                    let item = {
                        id: this.state.current_nomenclature.id,
                        name: values.nomenclature,
                        amount: 0,
                        count: values.count,
                        unit: this.state.current_nomenclature.unit_name,
                        final_amount: 0,
                    };

                    const DS = [...this.state.nomDS];
                    const index = DS.findIndex(
                        (item) => this.state.current_nomenclature.id === item.id
                    );


                    if (index !== -1) {
                        const item = DS[index];
                        item.count = parseInt(item.count + values.count);
                        item.final_amount = (
                            parseFloat(item.count) * parseFloat(item.amount)
                        ).toFixed(2);
                        this.setState({ nomDS: DS });

                    } else {
                        DS.unshift(item);
                        this.setState({ nomDS: DS });
                    }
                }

            });
    };

    finish = (action) => {
        const { nomDS } = this.state;

        let body = {
            number: this.formRef.current.getFieldValue("number"),
            dated: this.formRef.current.getFieldValue("dated") ? this.formRef.current.getFieldValue("dated").unix() : Math.floor(Date.now() / 1000),
            operation: this.formRef.current.getFieldValue("operation"),
            comment: this.formRef.current.getFieldValue("comment"),
            goods: [],
        };

        body.id = this.props.doc.id

        if (this.state.is_transfer && this.state.current_warehouse_to) {
            body.to_warehouse = this.state.current_warehouse_to.id
        }

        if (this.props.tags !== undefined) {
            body.tags = this.props.tags;
        }

        if (!this.state.current_warehouse || !this.state.current_organization) {
            message.error("Вы не выбрали склад или организацию!");
        } else {
            body.warehouse = this.state.current_warehouse.id;

            if (this.state.current_contragent) {
                if (this.state.current_contragent.contragent) {
                    body.contragent = this.state.current_contragent.contragent;
                } else {
                    body.contragent = this.state.current_contragent.id;
                }
            }

            if (this.state.current_organization) {
                body.organization = this.state.current_organization.id;
            }

            nomDS.map((item) => {
                let good_body = {
                    price: parseFloat(item.amount),
                    price_type: 1,
                    quantity: parseFloat(item.count),
                    // unit: 116,
                    nomenclature: item.id,
                };
                body.goods.push(good_body);
                return 0;
            });

            if (action === "add_proc") {
                body.status = true;
            } else if (action === "only_add") {
                body.status = false;
            }

            axios
                .patch(
                    `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_docs_warehouse/?token=${this.props.token}`,
                    [body]
                )
                .then((response) => {
                    message.success("Вы успешно изменили документ");
                    this.setState({ isModalVisible: false });
                });
        }
    };

    setFinishPrices = (total) => {
        if (this.state.loyality_card) {
            const loyalityBalance = this.state.loyality_card.balance;
            const maxDiscount = this.state.loyality_card.max_percentage;
            const availableDiscount = (loyalityBalance / 100) * maxDiscount;
            const rublesWithDiscount = total - availableDiscount;
            if (rublesWithDiscount <= 0) {
                const rubles = 0;
                const loyality = availableDiscount + rublesWithDiscount;
                this.finRef.current.setFieldValue("lt", loyality);
                this.finRef.current.setFieldValue("rubles", rubles);
                this.setState({
                    max_paid_loyality: availableDiscount,
                });
                return null;
            }
            this.finRef.current.setFieldValue("lt", availableDiscount);
            this.finRef.current.setFieldValue("rubles", rublesWithDiscount);
            this.setState({
                max_paid_loyality: availableDiscount,
            });
        }
    };

    onRecalculateLoyality = (value) => {
        if (this.state.loyality_card) {
            const total = this.state.amount_without_discount;
            const diff = Math.abs(total - value);
            this.finRef.current.setFieldValue("lt", diff);
        }
    };

    onRecalculateRubles = (value) => {
        const total = this.state.amount_without_discount;
        const diff = Math.abs(total - value);
        this.finRef.current.setFieldValue("rubles", diff);
    };

    findDocWarehouse = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/${id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
            });
    };

    showModal = () => {
        this.findDocWarehouse(this.props.doc.id).then((res) => {
            this.onSelectCa(res.contragent);
            this.onSelectWareHouse(res.warehouse);
            if (this.state.is_transfer) {
                this.onSelectWareHouseTo(res.to_warehouse)
            }
            this.onSelectOrg(res.organization);

            let nomDS = [];

            res.goods.forEach(function (arrayItem) {
                console.log(res.goods)
                let item = {
                    id: arrayItem.nomenclature,
                    name: arrayItem.nomenclature_name,
                    amount: arrayItem.price.toFixed(2),
                    discount: arrayItem.discount,
                    count: arrayItem.quantity,
                    unit: arrayItem.unit_name,
                    final_amount: (
                        parseFloat(arrayItem.price) * parseFloat(arrayItem.quantity)
                    ).toFixed(2),
                };
                nomDS.push(item);
            });

            this.setState({ nomDS: nomDS });
        });

        this.setState({ isModalVisible: true });
    };

    render() {

        const handleCancel = () => {
            this.setState({ isModalVisible: false });
        };

        const { nomDS } = this.state;
        const components = {
            body: {
                row: EditableRow,
                cell: EditableCell,
            },
        };
        const columns = this.columns.map((col) => {
            if (!col.editable) {
                return col;
            }

            return {
                ...col,
                onCell: (record) => ({
                    record,
                    editable: col.editable,
                    dataIndex: col.dataIndex,
                    title: col.title,
                    handleSave: this.handleSave,
                }),
            };
        });

        return (
            <>
                <Button
                    style={{ marginRight: 10 }}
                    icon={<EditOutlined />}
                    onClick={this.showModal}
                />
                <Modal
                    width={1800}
                    destroyOnClose={true}
                    footer={null}
                    title="Проведение документа склада"
                    open={this.state.isModalVisible}
                    onCancel={handleCancel}
                >
                    <Layout>
                        <Sider style={siderStyle}>
                            <Form
                                name="basic"
                                ref={this.formRef}
                                initialValues={this.leftBarIV}
                                layout="vertical"
                                style={{ marginLeft: 10, marginRight: 10, marginTop: 10 }}
                            // onFinish={onFinish}
                            >
                                <Form.Item label="Номер" name="number">
                                    <Input />
                                </Form.Item>

                                <Form.Item label="Операция" name="operation">
                                    <Select
                                        onChange={(selected) => {
                                            if (selected === "transfer") {
                                                this.setState({ is_transfer: true })
                                            }
                                            else {
                                                this.setState({ is_transfer: false })
                                            }
                                        }}
                                        options={[
                                            {
                                                value: 'outgoing',
                                                label: 'Расход',
                                            },
                                            {
                                                value: 'incoming',
                                                label: 'Приход',
                                            },
                                            {
                                                value: 'transfer',
                                                label: 'Перемещение',
                                            }
                                        ]}
                                    />
                                </Form.Item>

                                <Form.Item label="Комментарий" name="comment">
                                    <Input />
                                </Form.Item>

                                <Form.Item label="Склад" name="warehouse">
                                    <WareHousesAutocomplete
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectWareHouse}
                                    />
                                </Form.Item>

                                <Form.Item label="Склад зачисления" name="warehouse_to">
                                    <WareHousesAutocomplete
                                        disabled={!this.state.is_transfer}
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectWareHouseTo}
                                    />
                                </Form.Item>

                                <Form.Item label="Контрагент" name="contragent">
                                    <ContragentAutocomplete
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectCa}
                                    />
                                </Form.Item>

                                <Form.Item label="На дату" name="dated">
                                    <DatePicker style={{ width: "100%" }} placeholder="Выберите дату" format={"DD.MM.YYYY"} onChange={(date) => this.setState({ dated: date })} />
                                </Form.Item>

                                <Form.Item label="Организация" name="organization">
                                    <OrganizationAutoComplete
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectOrg}
                                    />
                                </Form.Item>
                            </Form>
                        </Sider>
                        <Layout>
                            <Header style={headerStyle}>
                                <Form
                                    name="basic"
                                    ref={this.formRefNom}
                                    layout="inline"
                                    style={{ marginLeft: 10, marginRight: 10, marginTop: 10 }}
                                    initialValues={{
                                        remember: true,
                                    }}
                                    onFinish={this.addNomenclature}
                                >
                                    <Form.Item label="Товар" name="nomenclature">
                                        <NomenclatureAutoComplete
                                            style={{ width: 300 }}
                                            placeholder={"Выберите товар"}
                                            api={this.api}
                                            token={this.props.token}
                                            onSelect={this.onSelectNom}
                                        />
                                    </Form.Item>

                                    <Form.Item label="Тип номенклатуры">
                                        <Input
                                            disabled
                                            value={
                                                this.state.current_nomenclature
                                                    ? this.state.current_nomenclature.type === "product"
                                                        ? "Товар"
                                                        : "Услуга"
                                                    : "Не выбран"
                                            }
                                        />
                                    </Form.Item>

                                    <Form.Item label="Остаток на складе">
                                        <InputNumber
                                            disabled
                                            precision={3}
                                            value={this.state.nomenclature_max}
                                        />
                                    </Form.Item>

                                    <Space>
                                        <Form.Item label="Количество" name="count" initialValue={0}>
                                            <InputNumber
                                                precision={3}
                                                min={0}
                                            //min={this.state.nomenclature_min}
                                            //max={this.state.nomenclature_max}
                                            />
                                        </Form.Item>
                                        <Button
                                            icon={<MinusOutlined />}
                                            onClick={() => this.changeCount("minus")}
                                        />
                                        <Button
                                            icon={<PlusOutlined />}
                                            onClick={() => this.changeCount("plus")}
                                        />
                                    </Space>

                                    <Form.Item style={{ marginTop: 10 }}>
                                        <Button
                                            disabled={this.state.addNomButtonDisabled}
                                            type="primary"
                                            htmlType="submit"
                                            style={{ marginLeft: 50 }}
                                        >
                                            Добавить
                                        </Button>
                                    </Form.Item>
                                </Form>
                            </Header>
                            <Content style={contentStyle}>
                                <Table
                                    style={{ marginTop: 30 }}
                                    dataSource={nomDS}
                                    components={components}
                                    rowClassName={(record) => record.is_deleted && "disabled-row"}
                                    rowKey={(record) => record.id}
                                    bordered
                                    columns={columns}
                                ></Table>
                            </Content>
                        </Layout>
                        <Sider style={siderStyle}>


                            <Button
                                onClick={() => this.finish("add_proc")}
                                style={{ width: "100%" }}
                            >
                                Изменить
                            </Button>
                        </Sider>
                    </Layout>
                </Modal>
            </>
        );
    }
}

export default EditDocsWarehouse;
