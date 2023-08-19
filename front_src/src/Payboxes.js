import React, { useContext, useEffect, useRef, useState } from 'react';
import {

    DatePicker,
    Form,
    Input,
    Table,
    message

} from 'antd';

import axios from 'axios';
import moment from 'moment';
import NewPboxModal from './NewPboxModal';
// import VirtualTable from './VirtualTable';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
dayjs.extend(utc)

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


class PayboxTable extends React.Component {
    constructor(props) {
        super(props);
        this.columns = [
            {
                title: 'Название счета',
                dataIndex: 'name',
                key: 'name',
                editable: true
            },
            {
                title: 'Баланс',
                key: 'balance',
                dataIndex: 'balance',
            },
            {
                title: 'Начальный остаток',
                key: 'start_balance',
                dataIndex: 'start_balance',
                editable: true,
            },
            {
                title: 'Дата остатка',
                dataIndex: 'balance_date',
                key: 'balance_date',
                render: (date, row) => {
                    return <DatePicker format={`DD.MM.YYYY`}
                        allowClear={false}
                        value={dayjs.unix(date)}
                        onChange={(date) => this.edit_date_request(date, row)}
                    />
                },
            },
            {
                title: 'Дата последнего изменения остатка',
                dataIndex: 'update_start_balance',
                key: 'update_start_balance',
                render: (date) => {
                    return moment.unix(date).format("DD.MM.YYYY")
                },
            },
            {
                title: 'Дата последнего изменения даты остатка',
                dataIndex: 'update_start_balance_date',
                key: 'update_start_balance_date',
                render: (date) => {
                    return moment.unix(date).format("DD.MM.YYYY")
                },
            },
            {
                title: 'Дата создания счета',
                dataIndex: 'created_at',
                key: 'created_at',
                render: (date) => {
                    return moment.unix(date).format("DD.MM.YYYY")
                },
            },
        ];

        this.state = {
            currentPage: 1,
            loading: true
        };

    }

    editReq = (body) => {
        axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${this.props.query.token}`, body)
            .then(response => {
                message.success(<>Вы успешно изменили счет</>);
            });
    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes`)
        console.log(this.state.dataSource)

        const { websocket } = this.props;

        websocket.onmessage = message => {
            const data = JSON.parse(message.data)

            if (data.target === "payboxes") {
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

                this.setState({
                    count: response.data.count,
                    dataSource: response.data.result,
                    loading: false
                })

            });
    }

    handleSave = (row) => {
        const newData = [...this.state.dataSource];
        const index = newData.findIndex((item) => row.id === item.id);
        const item = newData[index];

        newData.splice(index, 1, { ...item, ...row });

        // this.setState({
        //     dataSource: newData,
        // });

        let edit_body = {
            id: row.id
        }

        if (row.name !== item.name) {
            edit_body.name = row.name
        }

        if (row.start_balance !== item.start_balance) {
            edit_body.start_balance = row.start_balance
        }

        if (JSON.stringify(edit_body) !== JSON.stringify({ id: row.id })) {
            this.editReq(edit_body);
        }

    };

    edit_date_request = (date, row) => {
        const date_ts = dayjs(date).utc().unix()

        const edit_balance = {
            id: row.id,
            balance_date: date_ts
        }

        this.editReq(edit_balance)

    }

    render() {
        // const { dataSource } = this.state;
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
                    <NewPboxModal token={this.props.query.token} />
                </div>
                <Table
                    components={components}
                    loading={this.state.loading}
                    rowClassName={record => record.is_deleted && "disabled-row"}
                    rowKey={record => record.id}
                    bordered
                    dataSource={this.state.dataSource}
                    columns={columns}
                    // scroll={{
                    //     y: 590,
                    //     x: '95vw',
                    // }}
                    pagination={
                        {
                            total: this.state.count,
                            onChange: page => {
                                this.setState({ currentPage: page, loading: true }, this.fetch(page, `https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes`))
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

export default PayboxTable;