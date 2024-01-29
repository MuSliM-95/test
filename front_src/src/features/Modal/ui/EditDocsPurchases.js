//модальное окно редактирования на странице "Закупки"
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
} from "antd";
import {
    ContragentAutocomplete,
    ContractAutocomplete,
    NomenclatureAutoComplete,
    WareHousesAutocomplete,
    OrganizationAutoComplete
} from "src/shared"
import axios from "axios";
import {
    PlusOutlined,
    MinusOutlined,
    // PercentageOutlined,
    // DollarOutlined,
    DeleteOutlined,
    EditOutlined,
} from "@ant-design/icons";


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
                    required: dataIndex !== 'amount',
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

class EditDocsPurchases extends React.Component {
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
            nomDS: [],
        };
        this.api = `https://${process.env.REACT_APP_APP_URL}/api/v1/`;

        this.leftBarIV = {
            number: this.props.doc.number,
            comment: this.props.doc.comment,
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

    findDocSales = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/${id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
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
            }
        );

        // this.edit_request(newData.splice(index, 1, { ...item, ...row })[0]);
    };

    onSelectCa = (val) => {
        this.findContragent(val).then((res) => {
            this.setState({ current_contragent: res });
            this.formRef.current.setFieldsValue({
                contragent: res.name,
            });
        });
    };

    findContract = async (id) => {
        return fetch(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/contracts/${id || ''}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
                return body;
            });
    };

    onSelectContract = (val) => {
        this.findContract(val).then((res) => {
            this.setState({ current_contract: res });
            this.formRef.current.setFieldsValue({
                contract: res.name,
            });
        });
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
                        `https://${process.env.REACT_APP_APP_URL}/api/v1/warehouse_balances/${this.state.current_warehouse.id}/?token=${this.props.token}&nomenclature_id=${res.id}`
                    )
                        .then((response) => response.json())
                        .then((body) => {
                            if (body > 0) {
                                this.formRefNom.current.setFieldsValue({
                                    count: 1,
                                });
                                this.setState({
                                    nomenclature_min: 1,
                                    nomenclature_max: body,
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
            `https://${process.env.REACT_APP_APP_URL}/api/v1/prices/${this.state.current_nomenclature.id}/?token=${this.props.token}`
        )
            .then((response) => response.json())
            .then((body) => {
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

                if (values.discount > 0) {
                    const { discount_type } = this.state;

                    if (discount_type === "percent") {
                        const amount = parseFloat(item.amount);
                        const onePerc = amount / 100;

                        item.discount = (onePerc * values.discount).toFixed(2);
                        item.final_amount = (item.final_amount - item.discount).toFixed(2);
                    } else {
                        item.discount = values.discount.toFixed(2);
                        item.final_amount = (item.final_amount - item.discount).toFixed(2);
                    }
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
            });
    };

    showModal = () => {
        this.findDocSales(this.props.doc.id).then((res) => {
            console.log(res)
            this.onSelectCa(res.contragent);
            this.onSelectContract(res.contract);
            this.onSelectWareHouse(res.warehouse);
            this.onSelectOrg(res.organization);

            let nomDS = [];

            res.goods.forEach(function (arrayItem) {
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

    finish = (action) => {
        const { nomDS } = this.state;

        let body = {
            number: this.formRef.current.getFieldValue("number"),
            dated: Math.floor(Date.now() / 1000),
            operation: "Заказ",
            comment: this.formRef.current.getFieldValue("comment"),
            tax_included: true,
            tax_active: true,
            goods: [],
        };

        if (this.props.tags !== undefined) {
            body.tags = this.props.tags;
        }

        if (this.state.loyality_card) {
            body.loyality_card_id = this.state.loyality_card.id;
        }

        if (!this.state.current_warehouse || !this.state.current_organization) {
            message.error("Вы не выбрали склад или организацию!");
        } else {
            body.warehouse = this.state.current_warehouse.id;
            if (this.state.current_contragent) {
                body.contragent = this.state.current_contragent.id;
            }
            if (this.state.current_contract) {
                body.contract = this.state.current_contract.id;
            }
            if (this.state.current_organization) {
                body.organization = this.state.current_organization.id;
            }

            nomDS.map((item) => {
                let good_body = {
                    price_type: 1,
                    price: parseFloat(item.amount),
                    quantity: parseFloat(item.count),
                    unit: 116,
                    nomenclature: item.id,
                };
                body.goods.push(good_body);
                return 0;
            });

            body.id = this.props.doc.id;

            axios
                .patch(
                    `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/?token=${this.props.token}`,
                    [body]
                )
                .then((response) => {
                    message.success("Вы успешно изменили документ");
                    this.setState({ isModalVisible: false });
                });
        }
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
                    title="Проведение документа продаж"
                    open={this.state.isModalVisible}
                    onCancel={handleCancel}
                >
                    <Layout>
                        <Sider style={siderStyle}>
                            <Form
                                name="basic"
                                ref={this.formRef}
                                layout="vertical"
                                style={{ marginLeft: 10, marginRight: 10, marginTop: 10 }}
                                initialValues={this.leftBarIV}
                            // onFinish={onFinish}
                            >
                                <Form.Item label="Номер" name="number">
                                    <Input />
                                </Form.Item>
                                <Form.Item label="Комментарий" name="comment">
                                    <Input />
                                </Form.Item>
                                <Form.Item label="Контрагент" name="contragent">
                                    <ContragentAutocomplete
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectCa}
                                    />
                                </Form.Item>

                                <Form.Item label="Договор" name="contract">
                                    <ContractAutocomplete
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectContract}
                                    />
                                </Form.Item>

                                <Form.Item label="Склад отгрузки" name="warehouse">
                                    <WareHousesAutocomplete
                                        api={this.api}
                                        token={this.props.token}
                                        onSelect={this.onSelectWareHouse}
                                    />
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
                            {/* <Button onClick={() => this.finish("only_add")} style={{ marginTop: 10 }}>Только создать</Button> */}
                        </Sider>
                    </Layout>
                </Modal>
            </>
        );
    }
}

export default EditDocsPurchases;
