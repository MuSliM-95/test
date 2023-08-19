import React, { useContext, useEffect, useRef, useState } from 'react';
import { Form, Input, Button, Table, Switch, Popconfirm, message } from 'antd';
import { DeleteOutlined, CloseCircleOutlined, CheckCircleOutlined } from '@ant-design/icons';

import axios from 'axios';
import NewDocsSales from './NewDocsSales';
import EditDocsSales from './EditDocSales';

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
            console.log('Save failed:', errInfo);
        }
    };


    let childNode = children;

    if (editable) {
        childNode = editing ? (
            <Form.Item
                style={{
                    margin: 0,
                }}
                name={dataIndex}
                rules={[
                    {
                        required: true,
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
    }

    return <td {...restProps}>{childNode}</td>;
};

class DocsSales extends React.Component {
    constructor(props) {
        super(props);

        this.columns = [
            {
                title: 'Номер',
                dataIndex: 'number',
                key: 'number',
                // width: 260,
                editable: false,
            },
            {
                title: 'Дата проведения',
                key: 'dated',
                dataIndex: 'dated',
                // width: 160,
                editable: false,
                render: (date) => {
                    const dateFormat = new Date(date * 1000);
                    return `
                    ${dateFormat.getDate()}.${(dateFormat.getMonth() + 1)}.${dateFormat.getFullYear()}
                    ${dateFormat.getHours()}:${dateFormat.getMinutes()}:${dateFormat.getSeconds()}
                    `
                }
            },
            {
                title: 'Статус',
                key: 'status',
                dataIndex: 'status',
                // width: 160,
                editable: false,
                render: (checked, row) => {
                    return <Switch style={{ marginLeft: "13px" }} checked={checked} onClick={(checked) => this.handleChangeStatus(checked, row)} />
                }
            },
            {
                title: 'Оплачен полностью',
                key: 'full_paid',
                dataIndex: 'full_paid',
                // width: 400,
                editable: false,
                render: (checked, row) => {
                    if (row.paid_doc >= row.sum - row.doc_discount) {
                        return <font style={{ color: "green" }}><CheckCircleOutlined /> Оплачено</font>
                    }
                    return <font style={{ color: "red" }}><CloseCircleOutlined /> Не оплачено</font>
                }
            },
            {
                title: 'Сумма',
                key: 'sum',
                dataIndex: 'sum',
                // width: 400,
                editable: false,
            },
        ];

        if (props.amo_widget === undefined || props.amo_widget !== true) {
            this.columns.push(
                {
                    title: 'Скидка (на чек)',
                    key: 'doc_discount',
                    dataIndex: 'doc_discount',
                    // width: 400,
                    editable: false,
                },
                {
                    title: 'Товаров (в чеке)',
                    key: 'nomenclature_count',
                    dataIndex: 'nomenclature_count',
                    // width: 400,
                    editable: false,
                },
                {
                    title: 'Оплачено',
                    key: 'paid_doc',
                    dataIndex: 'paid_doc',
                    // width: 400,
                    editable: false,
                },
                {
                    title: 'Оплачено бонусами',
                    key: 'paid_loyality',
                    dataIndex: 'paid_loyality',
                    // width: 400,
                    editable: false,
                },
                {
                    title: 'Оплачено рублями',
                    key: 'paid_rubles',
                    dataIndex: 'paid_rubles',
                    // width: 400,
                    editable: false,
                }
            );
        }

        this.columns.push(
            {
                title: 'Действие',
                key: 'action',
                // width: 100,
                width: 160,
                render: (_, record) => {
                    return (this.state.dataSource.length >= 1 ? (
                        <>
                            <Popconfirm title="Подтвердите удаление"
                                onConfirm={() => this.handleDelete(record.id)}
                                cancelText="Отмена"
                                okText="OK"
                            >
                                <Button icon={<DeleteOutlined />} style={{ marginRight: 10 }} />
                            </Popconfirm>
                            <EditDocsSales
                                doc={record}
                                token={this.props.query.token}
                                updateRow={this.updateRow}
                                tags={this.props.tags}
                            />
                        </>

                    ) : "Загрузка...")
                }
            }
        );

        this.state = {
            count: this.props.c,
            dataSource: this.props.ds,
            loading: true,
            currentPage: 1,
        };

    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_sales/`)
        const { websocket } = this.props;

        websocket.onmessage = message => {
            const data = JSON.parse(message.data)

            if (data.target === "docs_sales") {
                if (data.action === "create") {

                    data.result.forEach(docs_sale => {

                        if (this.props.tags === undefined || (this.props.tags !== undefined && docs_sale.tags === this.props.tags)) {
                            if (this.state.currentPage === 1) {
                                const DS = [...this.state.dataSource];
                                const C = this.state.count;
                                if (DS.length <= 34) {
                                    DS.unshift(docs_sale);
                                }
                                else {
                                    DS.pop()
                                    DS.unshift(docs_sale);
                                }
                                this.setState({ dataSource: DS, count: C + 1 })
                            }
                        }

                    });
                }

				if (data.action === "edit") {
                    data.result.forEach(docs_sale => {

                        const newData = [...this.state.dataSource];                        
                        const index = newData.findIndex((item) => docs_sale.id === item.id);
    
                        if (index !== -1) {
                            const item = newData[index];
                            newData.splice(index, 1, { ...item, ...docs_sale });    
                            this.setState({ dataSource: newData });
                        }
                        
                    });
				}

				if (data.action === "delete") {

                    data.result.forEach(docs_sale => {                        
                    
                        const newData = [...this.state.dataSource];
                        const index = newData.findIndex((item) => docs_sale === item.id);

                        if (index !== -1) {
                            newData.splice(index, 1);
                            this.setState({ dataSource: newData });
                        }
                        
                    });
				}
            }
        }
    }

    handleChangeStatus = (status, row) => {
        this.edit_request(row.id, row, { status: status })
    }

    fetch = (page, url = {}) => {
        const limit = 35
        const offset = (page * 35) - 35

        let params = { token: this.props.query.token, limit: limit, offset: offset }

        if (this.props.tags !== undefined) {
            params.tags = this.props.tags
        }

        axios.get(url, { params: params })
            .then(response => {

                const newData = response.data.result.map(rowData => (
                    {
                        created_at: Date.now(),
                        updated_at: Date.now(),
                        response_code: 200,
                        response: JSON.stringify(rowData, null, 3),
                        ...rowData,
                    }
                ));

                this.setState({
                    count: response.data.count,
                    dataSource: newData,
                    loading: false
                })

            });
    }

    handleDelete = (id) => {
        const dataSource = [...this.state.dataSource];
        const row_index = dataSource.findIndex((item) => item.id === id)
        const row = dataSource[row_index]
        dataSource.splice(row_index, 1);

        this.setState({
            dataSource: dataSource,
        });

        axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/docs_sales/${row.id}`, { params: { token: this.props.query.token } })
            .then(response => {
                message.success("Вы успешно удалили документ продажи");
            }).catch((err) => {
                message.error("Не удалось удалить документ продажи!");
                console.log('err', err);
            });
    }

    edit_request = (id, payment, row) => {
        let edit_dict = {}

        for (let item in row) {
            if (row[item] !== payment[item]) {
                edit_dict[item] = row[item]
            }
        }

        edit_dict.id = id
        edit_dict.organization = payment.organization

        if (Object.keys(edit_dict).length !== 0) {
            axios.patch(`https://${process.env.REACT_APP_APP_URL}/api/v1/docs_sales/${id}?token=${this.props.query.token}`, [edit_dict])
        } else {
            message.error(<>Вы не сделали никаких изменений!</>);
        }

    }

    handleSave = (row) => {
        const newData = [...this.state.dataSource];
        const index = newData.findIndex((item) => row.id === item.id);
        const item = newData[index];

        newData.splice(index, 1, { ...item, ...row });

        this.setState({
            dataSource: newData,
        });

        this.edit_request(newData.splice(index, 1, { ...item, ...row })[0]);
    };

    updateRow = (data, response) => {
        const id = data.id || response.data.id;

        const newData = this.state.dataSource;

        newData.forEach(row => {
            if (row.id === id) {
                row.response_code = response.status;
                row.response = JSON.stringify(response.data, null, 3);
            }
        });

        this.setState({
            dataSource: newData,
        });
    };

    render() {
        const { dataSource } = this.state;
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
            <div>
                <div style={{ marginBottom: 10 }}>
                    <NewDocsSales
                        // meta={this.props.meta}
                        token={this.props.query.token}
                        updateRow={this.updateRow}
                        tags={this.props.tags}
                        phone={this.props.phone}
                        name={this.props.name}
                    />
                </div>
                {/* <ContragentsTable dataSource={dataSource} /> */}

                <Table
                    components={components}
                    rowClassName={record => record.is_deleted && "disabled-row"}
                    rowKey={record => record.id}
                    bordered
                    // scroll={{
                    //     y: 600,
                    //     x: '85vw',
                    // }}
                    loading={this.state.loading}
                    dataSource={dataSource}
                    columns={columns}
                    pagination={
                        {
                            total: this.state.count,
                            onChange: page => {
                                this.setState({ currentPage: page, loading: true }, this.fetch(page, `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_sales/`))
                            },
                            pageSize: 35,
                            showSizeChanger: false
                        }
                    }
                />
            </div>
        );
    }
}

export default DocsSales;