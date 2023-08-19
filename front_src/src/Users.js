import React, { useContext, useEffect, useRef, useState } from 'react';
import {
    Form,
    Input,
    Switch,
    Alert,
    Avatar, Image, Tooltip, message, Table
} from 'antd';

import { WarningOutlined } from '@ant-design/icons';

import axios from 'axios';
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

class UsersTable extends React.Component {
    constructor(props) {
        super(props);

        this.columns = [
            {
                title: 'Фото',
                dataIndex: 'photo',
                key: 'photo',
                width: 80,
                render: (photo_src) => {
                    return <Avatar src={<Image src={`https://${process.env.REACT_APP_APP_URL}/${photo_src}`} />} size={60} />
                },
            },
            {
                title: 'Роль',
                key: 'is_admin',
                dataIndex: 'is_admin',
                // width: 120,
                width: 20,
                render: (is_owner) => {
                    if (is_owner) {
                        return <b>Админ</b>
                    }
                    else {
                        return <b>Сотрудник</b>
                    }
                }
            },
            {
                title: 'Имя',
                key: 'first_name',
                dataIndex: 'first_name',
                // width: 170,
            },
            {
                title: 'Фамилия',
                key: 'last_name',
                dataIndex: 'last_name',
                // width: 170,
                render: (last_name) => {
                    if (last_name) {
                        return last_name
                    }
                    else {
                        return "Не указана"
                    }
                }
            },
            {
                title: 'Ссылка ТГ',
                key: 'username',
                dataIndex: 'username',
                // width: 100,
                width: 200,
                render: (username) => {
                    if (username) {
                        return <a target={"_blank"} rel="noreferrer" href={`https://t.me/${username}`}>https://t.me/{username}</a>
                    }
                    else {
                        return <Alert message="У этого пользователя нет юзернейма" type="warning" />
                    }
                }
            },
            {
                title: 'Статус',
                dataIndex: 'status',
                key: 'status',
                // width: 50,
                width: 5,
                render: (checked, row) => {
                    if (!row.is_admin) {
                        return <Switch checked={checked} onClick={(checked) => this.handleChangeStatus(checked, row)} />
                    }
                    return <Tooltip title="Нельзя изменить статус админа!">
                        <WarningOutlined />
                    </Tooltip>
                }
            },
        ];

        this.state = {
            count: this.props.c,
            dataSource: this.props.ds,
            currentPage: 1,
            loading: true
        };
    }

    handleChangeStatus = (checked, row) => {
        axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/cashbox_users?token=${this.props.query.token}&user_id=${row.id}&status=${checked}`)
            .then(response => {
                message.success(<>Вы успешно изменили статус</>);
            });
    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/cashbox_users`)
        const { websocket } = this.props;

        websocket.onmessage = message => {
            const data = JSON.parse(message.data)

            if (data.target === "users") {
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

        this.setState({
            dataSource: newData,
        });

        // edit_request(newData.splice(index, 1, { ...item, ...row })[0]);

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
                {this.props.invite ? (<div style={{ marginBottom: 10 }}><b>Ссылка для приглашения:</b> <a rel="noreferrer" target={"_blank"} className="button" href={`https://t.me/tablecrmbot?start=${this.props.invite}`}>{`https://t.me/tablecrmbot?start=${this.props.invite}`}</a></div>) : null}
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
                            pageSize: 100
                        }
                    }

                />
            </div>
        );
    }
}

export default UsersTable;