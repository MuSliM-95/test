import React, { useContext, useEffect, useRef, useState } from 'react';
import { Form, Input, Button, Table, Switch, Popconfirm, message, Space, DatePicker } from 'antd';
import { DeleteOutlined, SearchOutlined } from '@ant-design/icons';

import axios from 'axios';
// import NewDocsSales from './NewDocsSales';
// import EditDocsSales from './EditDocSales';
import NewDocsWarehouse from './NewDocsWarehouse';
import EditDocsWarehouse from './EditDocWarehouse';

const { RangePicker } = DatePicker;

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

class DocsWarehouse extends React.Component {
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
                title: 'От даты',
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
                title: 'Операция',
                key: 'operation',
                dataIndex: 'operation',
                // width: 400,
                editable: false,
                render: (operation) => {
                    if (operation === "incoming") {
                        return <font style={{ color: "green" }}>Приходная</font>
                    }
                    if (operation === "outgoing") {
                        return <font style={{ color: "red" }}>Расходная</font>
                    }
                    if (operation === "transfer") {
                        return <font style={{ color: "blue" }}>Перемещение</font>
                    }
                }
            },
            {
                title: 'Сумма',
                key: 'sum',
                dataIndex: 'sum',
                // width: 400,
                editable: false,
            },
            {
                title: 'Комментарий',
                key: 'comment',
                dataIndex: 'comment',
                // width: 400,
                editable: false,
            },
            {
                title: 'Организация',
                key: 'organization',
                dataIndex: 'organization',
                // width: 400,
                editable: false,
                render: (organization) => {
                    const findedOrganization = this.props.organizationsData.find((item) => item.id === organization)
                    return findedOrganization.full_name
                }
            },
            {
                title: 'Склад',
                key: 'warehouse',
                dataIndex: 'warehouse',
                // width: 400,
                editable: false,
                render: (warehouse) => {
                    const findedWarehouse = this.props.warehousesData.find((item) => item.id === warehouse)
                    return findedWarehouse.name
                }
            },
        ];

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
                            <EditDocsWarehouse
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
            datesFilter: []
        };

    }

    getStamps = (datesArr) => {

        if (datesArr) {
            let datefrom = datesArr[0].startOf('day').unix()
            let dateto = datesArr[1].startOf('day').unix()

            this.setState({ datesFilter: [datefrom, dateto] })
        }

        else {
            this.setState({ datesFilter: [] })
            this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`, false)
        }
    }

    find = () => {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`)
    }

    findRun = () => {

        this.setState({ loading: true })

        let params = { token: this.props.query.token, offset: 0, limit: 100000 }
        let dates = {}

        if (this.state.datesFilter.length > 0) {
            dates['datefrom'] = this.state.datesFilter[0]
            dates['dateto'] = this.state.datesFilter[1]
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
                        `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_docs_warehouse/?token=${this.props.query.token}`,
                        statusesArr
                    )
                    .then((res) => {
                        this.setState({ loading: false })
                        message.info("Вы провели документы за промежуток!")
                    });

            });

    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`)
        const { websocket } = this.props;

        websocket.onmessage = message => {
            const data = JSON.parse(message.data)

            if (data.target === "docs_warehouse") {
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
        this.setState({ loading: true })
        this.edit_request(row.id, row, { status: status })
    }

    fetch = (page, url = {}, withDates = true) => {
        const limit = 35
        const offset = (page * 35) - 35

        let params = { token: this.props.query.token, limit: limit, offset: offset }

        let dates = {}

        if (this.state.datesFilter.length > 0 && withDates) {
            dates['datefrom'] = this.state.datesFilter[0]
            dates['dateto'] = this.state.datesFilter[1]
        }

        if (this.props.tags !== undefined) {
            params.tags = this.props.tags
        }

        axios.get(url, { params: Object.assign({}, params, dates) })
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

        axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/?token=${this.props.query.token}`, { data: [row.id] })
            .then(response => {
                message.success("Вы успешно удалили документ склада");
            }).catch((err) => {
                message.error("Не удалось удалить документ склада!");
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
        // edit_dict.organization = payment.organization

        if (Object.keys(edit_dict).length !== 0) {
            axios.patch(`https://${process.env.REACT_APP_APP_URL}/api/v1/alt_docs_warehouse/?token=${this.props.query.token}`, [edit_dict]).then((res) => { this.setState({ loading: false }) })
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
                    <NewDocsWarehouse
                        // meta={this.props.meta}
                        token={this.props.query.token}
                        updateRow={this.updateRow}
                        tags={this.props.tags}
                        phone={this.props.phone}
                        name={this.props.name}
                    />
                    <Space direction="horizontal">
                        <RangePicker format={"DD.MM.YYYY"} placeholder={["Дата начала", "Дата окончания"]} style={{ marginLeft: 5 }} onChange={(dates) => this.getStamps(dates)} />

                        <Button onClick={() => this.find()} htmlType="submit" icon={<SearchOutlined />}>Найти</Button>
                        <Button onClick={() => this.findRun()} htmlType="submit" icon={<SearchOutlined />}>Найти и провести</Button>


                    </Space>
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
                                this.setState({ currentPage: page, loading: true }, this.fetch(page, `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`, false))
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

export default DocsWarehouse;