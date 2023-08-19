import React, { useContext, useEffect, useRef, useState } from 'react';
import {
    Form,
    Input,
    Button,
    Table,
    Popconfirm, message
} from 'antd';

import { DeleteOutlined } from '@ant-design/icons';

import axios from 'axios';
import NewCAModal from './NewCAModal';
import EditCA from './EditCA';
// import ContragentsTable from './Tables/ContragentsTable/ContragentsTable';
// import VirtualTable from './VirtualTable';

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

class CATable extends React.Component {
    constructor(props) {
        super(props);

        this.columns = [
            {
                title: 'Имя контрагента',
                dataIndex: 'name',
                key: 'name',
                // width: 260,
                editable: true
            },
            {
                title: 'ИНН контрагента',
                key: 'inn',
                dataIndex: 'inn',
                // width: 160,
                editable: true,
            },
            {
                title: 'Телефон контрагента',
                key: 'phone',
                dataIndex: 'phone',
                // width: 160,
                editable: true,
            },
            {
                title: 'Описание',
                key: 'description',
                dataIndex: 'description',
                // width: 400,
                editable: true,
            },
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
                            <EditCA
                                // meta={this.props.meta}
                                ca={record}
                                token={this.props.query.token}
                                updateRow={this.updateRow}
                            />
                        </>

                    ) : "Загрузка...")
                }
            },
        ];

        this.state = {
            count: this.props.c,
            dataSource: this.props.ds,
            loading: true,
            currentPage: 1,
        };

    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/contragents`)
        const { websocket } = this.props;

        websocket.onmessage = message => {
            const data = JSON.parse(message.data)

            if (data.target === "contragents") {
                if (data.action === "create") {
                    if (this.state.currentPage === 1) {
                        const DS = [...this.state.dataSource];
                        const C = this.state.count;
                        if (DS.length <= 34) {
                            DS.unshift(data.result);
                        }
                        else {
                            DS.pop()
                            DS.unshift(data.result);
                        }
                        this.setState({ dataSource: DS, count: C + 1 })
                    }

                }
                if (data.action === "edit") {

                    const newData = [...this.state.dataSource];
                    const index = newData.findIndex((item) => data.result.id === item.id);

                    if (index !== -1) {
                        const item = newData[index];
                        newData.splice(index, 1, { ...item, ...data.result });
                        this.setState({ dataSource: newData });
                    }
                }
            }
        }
    }


    fetch = (page, url = {}) => {
        const limit = 35
        const offset = (page * 35) - 35

        axios.get(url, { params: { token: this.props.query.token, limit: limit, offset: offset } })
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

        axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${row.id}`, { params: { token: this.props.query.token } })
            .then(response => {
                message.success("Вы успешно удалили контрагента");
            }).catch((err) => {
                message.error("Не удалось удалить контрагента контрагента!");
                console.log('err', err);
            });
    }

    edit_request = (row) => {
        let body = Object.assign({}, row);
        delete body['id']
        axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${row.id}?token=${this.props.query.token}`, body)
            .then(response => {
                message.success('Вы успешно изменили контрагента');
            });
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
                    <NewCAModal
                        // meta={this.props.meta}
                        token={this.props.query.token}
                        updateRow={this.updateRow}
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

                                this.setState({ currentPage: page, loading: true }, this.fetch(page, `https://${process.env.REACT_APP_APP_URL}/api/v1/contragents`))
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

export default CATable;