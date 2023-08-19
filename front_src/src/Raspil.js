import {
    Button,
    Form,
    Table,
    Modal,
    Radio,
    InputNumber,
    Alert,
    Select,
    message
} from 'antd';

import { ScissorOutlined } from '@ant-design/icons';

import React from "react";
import axios from 'axios';


const tailLayout = {
    wrapperCol: {
        offset: 12,
        span: 20,
    },
};

let init_values = { raspil_type: "nequals", raspil_amount: 1 }

const validateMessages = {
    /* eslint-disable no-template-curly-in-string */
    required: '${label} обязательно!',
};


class Raspil extends React.Component {
    formRef = React.createRef();

    state = {
        visible: false,
        columns: [
            {
                title: 'Разбить на',
                dataIndex: 'amount',
                key: 'amount',
                width: 30
            },
            {
                title: 'Проект',
                dataIndex: 'project',
                key: 'project',
                render: (value, row) => {
                    return (
                        <Select onChange={(val) => this.handleChangeData(row, 'project', val)} defaultValue={value} style={{ width: '100%' }}>
                            {this.props.projects_select}
                        </Select>
                    )
                }
            },
            {
                title: 'Контрагент',
                dataIndex: 'ca',
                key: 'ca',
                render: (value, row) => {
                    return (
                        <Select onChange={(val) => this.handleChangeData(row, 'ca', val)} defaultValue={value} style={{ width: '100%' }}>
                            {this.props.caData}
                        </Select>
                    )
                }
            },
        ]
    };

    showModal = () => {
        this.setState({
            visible: true,
            type_name: "Сумма разбитых платежей",
            am_min: 1,
            phase: 0
        });


    };

    handleChangeData = (row, action, value) => {
        const dataSource = [...this.state.tableData]
        const row_index = dataSource.findIndex((item) => item.key === row.key)
        let item = dataSource[row_index]
        item[action] = value

        dataSource.splice(row_index, 1, { ...item, ...row });
        this.setState({ tableData: dataSource })
    }

    handleOk = (item) => {
        this.setState({ phase: 1 })
        let pay_count = 1
        let ost = 0
        let last_pay = 0
        let ds = []
        if (item.raspil_type === "nequals") {
            pay_count = Math.floor(this.props.payment.amount / item.raspil_amount)
            last_pay = parseFloat((this.props.payment.amount % item.raspil_amount).toFixed(2))
            for (let i = 0; i < pay_count; i++) {
                ds.push({ amount: item.raspil_amount, project: "0", ca: "0", key: i + 1 })
            }
            if (last_pay !== 0) {
                ds.push({ amount: last_pay, project: "0", ca: "0", key: ds.length + 1 })
            }
        }
        else {
            pay_count = item.raspil_amount
            for (let i = 0; i < pay_count - 1; i++) {
                ds.push({ amount: parseFloat((this.props.payment.amount / item.raspil_amount).toFixed(2)), project: "0", ca: "0", key: i + 1 })
            }
            ost = parseFloat((this.props.payment.amount - ((this.props.payment.amount / item.raspil_amount).toFixed(2) * (pay_count - 1))).toFixed(2))
            ds.push({ amount: ost, project: "0", ca: "0", key: ds.length + 1 })
        }
        this.setState({ tableData: ds })
    };

    handleName = (item) => {
        if (item.target.value === "nequals") {
            this.formRef.current.setFieldsValue({
                raspil_amount: 1,
            })
            this.setState({ type_name: "Сумма разбитых платежей", am_min: 1 })
        }
        else {
            this.setState({ type_name: "Количество платежей", am_min: 1 })
            this.formRef.current.setFieldsValue({
                raspil_amount: 1,
            })
        }
    }

    handleSave = () => {
        let ds = [...this.state.tableData];
        for (let i = 0; i < ds.length; i++) {
            delete ds[i].key
            if (ds[i].project === "0") {
                ds[i].project = null
            }
            else {
                ds[i].project = parseInt(ds[i].project)
            }
            if (ds[i].ca === "0") {
                ds[i].ca = null
            }
            else {
                ds[i].ca = parseInt(ds[i].ca)
            }
        }
        axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments_split/${this.props.payment.id}?token=${this.props.token}`, ds)
            .then(response => {
                message.success(<>Вы успешно создали проект</>);
            });
        this.handleCancel()

    }

    handleCancel = () => {
        this.setState({
            visible: false,
        });
    };

    render() {

        return (
            <>
                <Button style={{ marginRight: 10 }} icon={<ScissorOutlined />} onClick={this.showModal}>
                </Button>
                <Modal destroyOnClose={true} footer={null} title="Разбить платеж" open={this.state.visible} onCancel={this.handleCancel}>

                    {
                        this.state.phase === 0 ?

                            (<Form
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

                                <Form.Item
                                    name="raspil_type"
                                    label="Как разбить"
                                    style={{ width: 500 }}
                                >

                                    <Radio.Group onChange={this.handleName}>
                                        <Radio.Button value="nequals">Разбить на части по ...</Radio.Button>
                                        <Radio.Button value="equals">Разбить на ... равных частей</Radio.Button>
                                    </Radio.Group>

                                </Form.Item>

                                <Form.Item
                                    name="raspil_amount"
                                    label={this.state.type_name}
                                >

                                    <InputNumber min={this.state.am_min} />

                                </Form.Item>

                                <Form.Item {...tailLayout}>
                                    <Button type="primary" htmlType="submit" style={{ marginRight: 5 }}>
                                        Продолжить
                                    </Button>
                                    <Button htmlType="button" onClick={this.handleCancel}>
                                        Отмена
                                    </Button>
                                </Form.Item>

                            </Form>) : (
                                <>
                                    <Table columns={this.state.columns} dataSource={this.state.tableData} />
                                    <Button onClick={() => this.handleSave()} type="primary" htmlType="submit" style={{ marginTop: 5 }}>
                                        Сохранить
                                    </Button>
                                </>
                            )
                    }

                </Modal>
            </>
        );
    }
}

export default Raspil;