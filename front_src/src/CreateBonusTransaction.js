import {
    AutoComplete,
    Button,
    DatePicker,
    Divider,
    Form,
    InputNumber,
    Modal,
    Radio,
    Switch,
    Select,
    Spin,
    message
} from 'antd';

import { PlusOutlined } from '@ant-design/icons';

import React from "react";
import axios from 'axios';

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
dayjs.extend(utc)


const layout = {
    labelCol: {
        span: 8,
    },
    wrapperCol: {
        span: 20,
    },
};
const tailLayout = {
    wrapperCol: {
        offset: 12,
        span: 20,
    },
};

const validateMessages = {
    /* eslint-disable no-template-curly-in-string */
    required: '${label} обязательно!',
};


class CreateBonusTransaction extends React.Component {
    constructor(props) {
        super(props);

        this.state = {
            modalOpened: false,
            isLoaded: false,
        };

        this.api = `https://${process.env.REACT_APP_APP_URL}/api/v1`

    }

    formRef = React.createRef();


    showModal = () => {
        const { cardsData } = this.props

        let options_list = []

        for (var i in cardsData) {
            options_list.push(

                {
                    value: cardsData[i].id,
                    label: cardsData[i].card_number
                }
            )
        }

        this.setState({
            modalOpened: true,
            cardSelect: options_list,
            tagsMeta: this.props.tagsMeta,
            cardsMeta: this.props.cardsMeta
        })
    }

    closeModal = () => {
        this.setState({ modalOpened: false })
    }

    randomInteger = (min, max) => {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    setInitialValues = () => {
        let startValues = {}

        startValues.dated = dayjs()
        startValues.type = "accrual"
        // if (this.props.cardsData.length !== 0) {
        //     startValues.loyality_card_id = this.props.cardsData[0].id
        // }
        startValues.amount = 1
        startValues.status = true

        return startValues
    }

    onFinish = async (values) => {


        values.dated = values.dated.unix()
        if (values.tags && values.tags.length > 0) {
            values.tags = values.tags.map((item) => item.value).join(",")
        }
        else {
            values.tags = ""
        }
        values.loyality_card_number = values.loyality_card_number.label
        

        try {
            await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/?token=${this.props.token}`, values)
            this.closeModal();
        }

        catch (err) {
            message.error(err.response.data.detail, 2)
        }
    }

    fetchCards = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/?token=${this.props.token}&card_number=${name}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result && body.result.length > 0) {
                        let res = body.result.map((payment) => { return { number: payment.card_number, id: payment.id } })
                        let return_cards = res.map((value, i) => {
                            return {
                                label: value.number,
                                value: value.id,
                            }
                        });
                        this.setState({ cardsMeta: return_cards })
                        return return_cards
                    }
                })
                .then((body) => {
                    return body
                })
        }
        else {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards?token=${this.props.token}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result && body.result.length > 0) {
                        let res = body.result.map((payment) => { return { number: payment.card_number, id: payment.id } })
                        let return_cards = res.map((value, i) => {
                            return {
                                label: value.number,
                                value: value.id,
                            }
                        });
                        this.setState({ cardsMeta: return_cards })
                        return return_cards
                    }
                })
                .then((body) => {
                    return body
                })
        }
    }

    fetchTags = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/?token=${this.props.token}&tags=${name}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result && body.result.length > 0) {
                        let res = body.result.map((payment) => payment.tags.split(",")).flat(1)
                        let tags = res.filter((item) => item.toUpperCase().indexOf(name.toUpperCase()) !== -1)
                        let return_tags = tags.map((value, i) => {
                            return {
                                label: value,
                                value: `${this.randomInteger(1, 20000)}`,
                            }
                        });
                        // console.log(return_tags)
                        this.setState({ tagsMeta: return_tags })
                        return return_tags
                    }
                })
                .then((body) => {
                    return body
                })
        }
    }

    render() {

        const {
            modalOpened,

        } = this.state

        return (
            <>
                <Button style={{ marginBottom: 15 }} icon={<PlusOutlined />} type="primary" onClick={this.showModal}>
                    Добавить платеж
                </Button>
                <Modal
                    open={modalOpened}
                    width={550}
                    title={"Создание платежа"}
                    destroyOnClose={true}
                    onCancel={this.closeModal}
                    footer={null}
                >
                    {
                        true ? <Form
                            {...layout}
                            ref={this.formRef}
                            style={{ marginTop: 20 }}
                            validateMessages={validateMessages}
                            onFinish={this.onFinish}
                            initialValues={this.setInitialValues()}
                        >
                            <Form.Item
                                name="name"
                                label="Название платежа"
                            >
                                <AutoComplete
                                    style={{ width: 300 }}
                                    allowClear={true}
                                />
                            </Form.Item>

                            <Form.Item
                                name="description"
                                label="Описание"
                            >
                                <AutoComplete
                                    style={{ width: 300 }}
                                    allowClear={true}
                                />
                            </Form.Item>

                            <Form.Item
                                name="tags"
                                label="Теги платежа"
                            >

                                {/* <Select
                                    style={{ width: 300 }}
                                    allowClear={true}
                                    showSearch
                                    labelInValue
                                    filterOption={false}
                                    mode={"tags"}
                                /> */}
                                <Select
                                    style={{ width: 300 }}
                                    options={this.state.tagsMeta}
                                    placeholder="Введите теги"
                                    allowClear={true}
                                    showSearch
                                    labelInValue
                                    filterOption={false}
                                    mode={"tags"}
                                    onSearch={this.fetchTags}
                                />

                            </Form.Item>

                            <Form.Item
                                name="loyality_card_number"
                                label={"Карта лояльности"}
                            >
                                {/* <Select
                                    placeholder="Выберите карту"
                                    options={this.state.cardSelect}
                                    removeIcon={null}
                                    style={{
                                        width: 300
                                    }}
                                /> */}
                                <Select
                                    style={{ width: 300 }}
                                    options={this.state.cardsMeta}
                                    placeholder="Введите номер карты"
                                    allowClear={true}
                                    showSearch
                                    labelInValue
                                    filterOption={false}
                                    onSearch={this.fetchCards}
                                />

                            </Form.Item>


                            <Form.Item label="Тип платежа" name="type" onChange={(event) => this.setState({ paymentType: event.target.value })}>
                                <Radio.Group>
                                    <Radio.Button value="accrual">Начисление</Radio.Button>
                                    <Radio.Button value="withdraw">Вывод</Radio.Button>
                                </Radio.Group>
                            </Form.Item>

                            <Divider />

                            <Form.Item label="Статус платежа:" name="status" valuePropName="checked">
                                <Switch onChange={(checked) => this.setState({ paymentStatus: checked })} />
                            </Form.Item>

                            <Form.Item label="Дата платежа:" name="dated">

                                <DatePicker
                                    style={{ width: 300 }}
                                    format="DD.MM.YY"
                                    placeholder={"Выберите дату"}
                                />

                            </Form.Item>

                            <Divider />

                            <Form.Item label={"Сумма платежа"} name="amount">
                                <InputNumber
                                    style={{ width: 300 }}
                                    placeholder={"Введите сумму платежа"}
                                    onChange={(amount) => this.setState({ amount: amount, amountIsChanged: true })}
                                    min="1"
                                    step="1"
                                    stringMode
                                />

                            </Form.Item>


                            <Divider />

                            <Form.Item {...tailLayout}>
                                <Button type="primary" htmlType="submit" style={{ marginRight: 5 }}>
                                    Подтвердить
                                </Button>
                                <Button htmlType="button" onClick={this.closeModal}>
                                    Отмена
                                </Button>
                            </Form.Item>

                        </Form>
                            :
                            <Spin tip="Пожалуйста, подождите">
                                <div style={{ padding: 50, borderRadius: 4, background: "rgba(0, 0, 0, 0.05)" }} />
                            </Spin>
                    }

                </Modal>

            </>
        );
    }
}

export default CreateBonusTransaction;