import {
    Button,
    Form,
    Table,
    Modal,
    Input,
    Alert,
    Select,
    message
} from 'antd';

import React, { useContext, useEffect, useRef, useState } from 'react';

import { ScissorOutlined } from '@ant-design/icons';

import axios from 'axios';

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


let init_values = { raspil_type: "nequals", raspil_amount: 1 }

const validateMessages = {
    /* eslint-disable no-template-curly-in-string */
    required: '${label} обязательно!',
};


class RaspilView extends React.Component {
    formRef = React.createRef();

    state = {
        visible: false,
        columns: [
            {
                title: 'Сумма',
                dataIndex: 'amount',
                key: 'amount',
                width: 30,
                editable: true
            },
            {
                title: 'Проект',
                dataIndex: 'project_id',
                key: 'project_id',
                render: (value, row) => {
                    const defVal = () => {
                        if (value) return value.toString();
                        else return "0"
                    }
                    return (
                        <Select onChange={(val) => this.handleSave(row, 'project_id', val)} defaultValue={defVal} style={{ width: '100%' }}>
                            {this.props.projects_select}
                        </Select>
                    )
                }
            },
            {
                title: 'Контрагент',
                dataIndex: 'contragent',
                key: 'contragent',
                render: (value, row) => {
                    const defVal = () => {
                        if (value) return value.toString();
                        else return "0"
                    }
                    return (
                        <Select onChange={(val) => this.handleSave(row, 'contragent', val)} defaultValue={defVal} style={{ width: '100%' }}>
                            {this.props.caData}
                        </Select>
                    )
                }
            },
        ]
    };

    showModal = () => {
        axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments/${this.props.payment.id}/childs`, { params: { token: this.props.token } })
            .then(response => {

                this.setState({
                    visible: true,
                    dataSource: response.data.result,
                    count: response.data.count
                })
            });
    };



    handleSave = (row, action, value) => {
        const dataSource = [...this.state.dataSource]
        const row_index = dataSource.findIndex((item) => item.id === row.id)
        let item = dataSource[row_index]

        if (action && value) {
            item[action] = value
        }

        dataSource.splice(row_index, 1, { ...item, ...row });
        this.setState({ dataSource: dataSource })

    }

    handleLog = () => {
        const newAm = parseFloat((this.state.dataSource.reduce(function (cnt, o) { return cnt + parseFloat(o.amount); }, 0)).toFixed(2));
        if (newAm !== this.props.payment.amount) {
            message.error("Сумма дочерних платежей должна быть равна сумме родительского платежа!")
        }
        else {
            let body = [...this.state.dataSource]
            let newBody = []
            body.map((item) => {
                newBody.push({ id: item.id, amount: parseFloat(item.amount), project_id: parseFloat(item.project_id), contragent: parseInt(item.contragent) })
                return 0
            })
            axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments_split?token=${this.props.token}`, newBody)
                .then(response => {
                    this.handleCancel();
                })

        }
    }

    handleCancelRaspil = () => {
        axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments_split/${this.props.payment.id}?token=${this.props.token}`)
            .then(response => {
                message.success("Вы успешно отменили распил платежа!")
                this.handleCancel();
            })
    }

    handleCancel = () => {
        this.setState({
            visible: false,
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
        const columns = this.state.columns.map((col) => {
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
                <Button style={{ marginRight: 10 }} icon={<ScissorOutlined />} onClick={this.showModal}>
                </Button>
                <Modal destroyOnClose={true} footer={null} title="Просмотр дочек" open={this.state.visible} onCancel={this.handleCancel}>

                    <Form
                        ref={this.formRef}
                        validateMessages={validateMessages}
                        onFinish={this.handleOk}
                        initialValues={init_values}
                    >

                        <Form.Item
                            label="Сумма платежа"
                        >

                            <Alert message={`${this.props.payment.amount} руб.`} type="success" />

                        </Form.Item>

                        <Table
                            components={components}
                            rowClassName={record => record.is_deleted && "disabled-row"}
                            rowKey={record => record.id}
                            bordered
                            dataSource={dataSource}
                            columns={columns}

                        />

                        <Button onClick={() => this.handleLog()} type="primary" htmlType="submit" style={{ marginTop: 5 }}>
                            Сохранить
                        </Button>
                        <Button onClick={this.handleCancelRaspil} htmlType="submit" style={{ marginTop: 5, marginLeft: 5 }}>
                            Отменить распил
                        </Button>

                    </Form>

                </Modal>
            </>
        );
    }
}

export default RaspilView;