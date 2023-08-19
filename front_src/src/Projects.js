import React, { useContext, useEffect, useRef, useState } from 'react';

import {
    Tag,
    Form,
    Input,
    message,
    Table
} from 'antd';


import axios from 'axios';
import NewProjModal from './NewProjModal';
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


class ProjectsTable extends React.Component {
    constructor(props) {
        super(props);
        this.columns = [
            {
                title: 'Название проекта',
                dataIndex: 'name',
                key: 'name',
                editable: true
            },
            {
                title: 'Приход',
                key: 'incoming',
                dataIndex: 'incoming',
                render: (proc) => {
                    return <font color="green">{proc} руб.</font>
                }
            },
            {
                title: 'Расход',
                key: 'outgoing',
                dataIndex: 'outgoing',
                render: (proc) => {
                    return <font color="red">{proc} руб.</font>
                }
            },
            {
                title: 'Рентабельность',
                dataIndex: 'profitability',
                key: 'profitability',
                render: (proc) => {
                    if (proc < 0) {
                        return <Tag color="#f50">{proc} %</Tag>
                    }
                    else if (proc >= 0) {
                        return <Tag color="#87d068">{proc} %</Tag>
                    }
                }
            },
            {
                title: 'Сумма проекта',
                dataIndex: 'proj_sum',
                key: 'proj_sum',
                editable: true,
            }
        ];

        this.state = {
            count: this.props.c,
            dataSource: this.props.ds,
            currentPage: 1,
            loading: true
        };

    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/projects`)
        const { websocket } = this.props;

        websocket.onmessage = message => {
            const data = JSON.parse(message.data)

            if (data.target === "projects") {
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

    edit_handler = (row) => {
        const newData = [...this.state.dataSource];
        const index = newData.findIndex((item) => row.id === item.id);

        if (index !== -1) {
            const item = newData[index];
            newData.splice(index, 1, { ...item, ...row });
            this.setState({
                dataSource: newData,
            });
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

        let edit_body = {
            id: row.id
        }

        if (row.name !== item.name) {
            edit_body.name = row.name
        }

        if (row.proj_sum !== item.proj_sum) {
            edit_body.proj_sum = row.proj_sum
        }

        if (JSON.stringify(edit_body) !== JSON.stringify({ id: row.id })) {
            this.edit_request(edit_body);
        }

    };


    edit_request = (body) => {

        // reqwest({
        //     url: `https://tablecrm.com/api/v1/projects?token=${params.token}`,
        //     method: 'put',
        //     type: 'json',
        //     contentType: "application/json",
        //     data: JSON.stringify(body),
        //     success: (data) => {
        //         this.edit_handler(data)
        //     }
        // })
        axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${this.props.query.token}`, body)
            .then(response => {
                message.success(<>Вы успешно изменили проект</>);
            });

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
                    <NewProjModal token={this.props.query.token} />
                </div>
                <Table
                    components={components}
                    rowClassName={record => record.is_deleted && "disabled-row"}
                    rowKey={record => record.id}
                    bordered
                    // scroll={{
                    //     y: 610,
                    //     x: '85vw',
                    // }}
                    loading={this.state.loading}
                    dataSource={this.state.dataSource}
                    columns={columns}
                    pagination={
                        {
                            total: this.state.count,
                            onChange: page => {
                                this.setState({ currentPage: page, loading: true }, this.fetch(page, `https://${process.env.REACT_APP_APP_URL}/api/v1/projects`))

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

export default ProjectsTable;