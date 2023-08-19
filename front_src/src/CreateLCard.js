import {
    AutoComplete,
    Button,
    DatePicker,
    Divider,
    Form,
    Input,
    InputNumber,
    Modal,
    Switch,
    Alert,
    Select,
    message,
} from 'antd';

import { PlusOutlined } from '@ant-design/icons';

import React from "react";
// import moment from "moment";

import axios from 'axios';


import NumericAutoComplete from './NumericAutoComplete';
import ContragentAutocomplete from './ContragentAutocomplete';

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
dayjs.extend(utc)


const layout = {
    labelCol: {
        span: 9,
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


let init_values = {
    status_card: true,
    start_period: dayjs(),
    end_period: dayjs().add(10, 'years'),
    cashback_percent: 0,
    minimal_checque_amount: 0,
    max_percentage: 0,
    max_withdraw_percentage: 0,
}

const { TextArea } = Input;


class CreateLCard extends React.Component {
    formRef = React.createRef();

    state = {
        visible: false,
        p_status: true,
        namesMeta: this.props.name_meta,
        tagsMeta: this.props.tags_meta,
        tagsChanged: false,
        paymentType: "incoming",

        current_contragent: null,
        isNewContr: false,
        newContrName: null,
        isContrCleared: true,

        current_article: null,
        isNewArticle: false,
        newArticleName: null,
        isArticleCleared: true,
    };

    api = `https://${process.env.REACT_APP_APP_URL}/api/v1/`

    showModal = () => {
        this.setState({
            selected: [],
            visible: true,
            repeat_switch: null,
            repeat_freq_int: 1,
            cashback_percent: "0",
            minimal_checque_amount: "0",
            max_percentage: "0",
            pbox_label: "Счет",
            pbox_to_label: "Счет",
            disabled: false,
            required: true,
            ca_value: []
        });
    };

    daysInThisMonth = () => {
        let now = new Date();
        return new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    }

    handleOk = async (values) => {
        const {
            isNewContr,
            current_contragent,
            isContrCleared,


            p_status
        } = this.state

        let requestBody = {}

        let body = { name: values.contragent_name, inn: values.contragent_inn, phone: values.contragent_phone, description: values.contragent_desc }
        for (let i in body) {
            if (!body[i]) {
                delete body[i]
            }
        }

        // Если очистили контра
        if (isContrCleared) {
            requestBody.contragent_id = null
        }

        // Если выбран (апдейт)
        if (
            !isContrCleared && !isNewContr &&
            (values.contragent_name !== current_contragent.name ||
                values.contragent_phone !== current_contragent.phone ||
                values.contragent_inn !== current_contragent.inn ||
                values.contragent_desc !== current_contragent.description)
        ) {
            requestBody.contragent_id = current_contragent.id
            await axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${current_contragent.id}?token=${this.props.token}`, body)
        }

        // Если ничего не поменялось
        if (
            !isContrCleared && !isNewContr &&
            values.contragent_name === current_contragent.name &&
            values.contragent_phone === current_contragent.phone &&
            values.contragent_inn === current_contragent.inn &&
            values.contragent_desc === current_contragent.description
        ) {
            requestBody.contragent_id = current_contragent.id
        }

        // Если новый контр
        if (!isContrCleared && isNewContr) {
            const resp = await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${this.props.token}`, body)
            requestBody.contragent_id = resp.data.id
        }

        delete values.contragent_name
        delete values.contragent_phone
        delete values.contragent_inn
        delete values.contragent_desc

        if (values.card_number && values.card_number !== "") {
            requestBody.card_number = values.card_number;
        }

        requestBody.status_card = p_status;
        requestBody.organization_id = parseInt(values.organization_id);
        requestBody.cashback_percent = values.cashback_percent;
        requestBody.minimal_checque_amount = values.minimal_checque_amount;
        requestBody.start_period = values.start_period.unix();
        requestBody.end_period = values.end_period.unix();
        requestBody.max_percentage = values.max_percentage;
        requestBody.is_deleted = false;

        if (!requestBody.contragent_id) {
            message.error({
                content: "Контрагент обязателен!"
            })
        }
        else {
            try {
                await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/?token=${this.props.token}`, [requestBody])
                this.handleCancel()
            }
            catch (err) {
            }
        }
    };


    handleCancel = () => {
        this.setState({
            visible: false,
            selected: [],
            amount: 1.00,
            ca_alert_name: null,
            isNewContr: false,
            isContrCleared: true
        });
    };


    findContragent = async (id) => {
        return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${id}?token=${this.props.token}`)
            .then((response) => response.json())
            .then((body) => {
                return body
            })
    }

    contrUnselect = () => {
        this.setState({ isNewContr: false, isContrCleared: true })
        this.formRef.current.setFieldsValue({
            contragent_name: "",
            contragent_phone: "",
            contragent_inn: "",
            contragent_desc: "",
        })
    }

    onSelectCa = (val) => {
        // this.setState({ isContrCleared: false })
        this.findContragent(val).then(res => {
            this.setState({ current_contragent: res, isNewContr: false, isContrCleared: false })
            this.formRef.current.setFieldsValue({
                contragent_name: res.name,
                contragent_phone: res.phone,
                contragent_inn: res.inn,
                contragent_desc: res.description,
            })
        });
    }

    onChangeCaName = (val) => {
        const { current_contragent } = this.state
        if (current_contragent) {
            if (val !== current_contragent.name) {
                this.setState({ isNewContr: true, newContrName: val })
            }
            else {
                this.setState({ isNewContr: false, newContrName: val })
                this.formRef.current.setFieldsValue({
                    contragent_name: current_contragent.name,
                    contragent_phone: current_contragent.phone,
                    contragent_inn: current_contragent.inn,
                    contragent_desc: current_contragent.description,
                })
            }
        }
        else {
            this.setState({ isNewContr: true, newContrName: val, isContrCleared: false })
        }
    }

    onChangeCaPhone = (val) => {
        const { current_contragent, isNewContr, isContrCleared } = this.state

        if (current_contragent) {

            if (!isNewContr && !isContrCleared) {
                // Если выбранный контр
                if (val !== current_contragent.phone) {
                    // Если введенный номер не соответсвует телефону выбранного
                    this.setState({ isNewContr: false })
                }
            }
            else {
                this.setState({ isNewContr: true, newContrName: "Без имени", isContrCleared: false })
                this.formRef.current.setFieldsValue({
                    contragent_name: "Без имени",
                    contragent_phone: val,
                })
            }
        }
        else {
            this.setState({ isNewContr: true, newContrName: "Без имени", isContrCleared: false })
            this.formRef.current.setFieldsValue({
                contragent_name: "Без имени",
                contragent_phone: val,
            })
        }
    }

    onChangeCaInn = (val) => {
        const { current_contragent, isNewContr, isContrCleared } = this.state

        if (current_contragent) {

            if (!isNewContr && !isContrCleared) {
                // Если выбранный контр
                if (val !== current_contragent.inn) {
                    // Если введенный номер не соответсвует телефону выбранного
                    this.setState({ isNewContr: false })
                }
            }
            else {
                this.setState({ isNewContr: true, newContrName: "Без имени", isContrCleared: false })
                this.formRef.current.setFieldsValue({
                    contragent_name: "Без имени",
                    contragent_inn: val,
                })
            }
        }
        else {
            this.setState({ isNewContr: true, newContrName: "Без имени", isContrCleared: false })
            this.formRef.current.setFieldsValue({
                contragent_name: "Без имени",
                contragent_inn: val,
            })
        }
    }

    render() {
        const {
            visible,
            current_contragent,
            isNewContr,
            newContrName,
            innsMeta,
            phonesMeta,
            isContrCleared,
        } = this.state;

        return (
            <>
                <Button style={{ marginBottom: 15 }} icon={<PlusOutlined />} type="primary" onClick={this.showModal}>
                    Добавить карту
                </Button>
                <Modal
                    open={visible}
                    width={700}
                    title="Создание новой карты лояльности"
                    destroyOnClose={true}
                    onCancel={this.handleCancel}
                    footer={null}
                >

                    <Form {...layout}
                        ref={this.formRef}
                        style={{ marginTop: 20 }}
                        validateMessages={validateMessages}
                        onFinish={this.handleOk}
                        initialValues={init_values}
                    >

                        <Form.Item
                            name="card_number"
                            label="Номер карты"
                        >
                            <AutoComplete
                                style={{ width: 300 }}
                                placeholder={"Введите номер карты"}

                                // options={namesMeta}
                                allowClear={true}
                            // onSearch={this.fetchNames}
                            />
                        </Form.Item>


                        <Form.Item label={"Процент кэшбека"} name="cashback_percent">
                            <InputNumber
                                style={{ width: 300 }}
                                placeholder={"Введите процент кэшбека"}
                                min="0"
                                step="1"
                                stringMode
                            />

                        </Form.Item>

                        <Form.Item label={"Минимальная сумма чека"} name="minimal_checque_amount">
                            <InputNumber
                                style={{ width: 300 }}
                                placeholder={"Введите минимальную сумму чека"}
                                min="0"
                                step="1"
                                stringMode
                            />

                        </Form.Item>

                        <Form.Item label={"Максимальный процент начисления"} name="max_percentage">
                            <InputNumber
                                style={{ width: 300 }}
                                placeholder={"Введите максимальный процент начисления"}
                                min="0"
                                step="1"
                                stringMode
                            />

                        </Form.Item>

                        <Form.Item label={"Максимальный процент списания"} name="max_withdraw_percentage">
                            <InputNumber
                                style={{ width: 300 }}
                                placeholder={"Введите максимальный процент списания"}
                                min="0"
                                step="1"
                                stringMode
                            />

                        </Form.Item>

                        <Divider />

                        <Form.Item
                            label={"Организация"}
                            name="organization_id"
                        >
                            <Select
                                options={this.props.organisations}
                            />

                        </Form.Item>

                        <Divider />

                        {
                            !isContrCleared ?
                                <>
                                    {
                                        !isNewContr ?
                                            <div>
                                                <Alert
                                                    type="success"
                                                    message={<div>Выбран <b>{current_contragent.name}</b></div>}
                                                    showIcon
                                                    action={
                                                        <Button size="small" type="text" onClick={this.contrUnselect}>
                                                            Очистить
                                                        </Button>
                                                    } />
                                                <br />
                                            </div>
                                            :
                                            <div>
                                                <Alert
                                                    type="info"
                                                    message={<div>Вы создаете <b>{newContrName}</b></div>}
                                                    showIcon
                                                    action={
                                                        <Button size="small" type="text" onClick={this.contrUnselect}>
                                                            Очистить
                                                        </Button>
                                                    } />
                                                <br />
                                            </div>
                                    }
                                </> : null
                        }

                        <Form.Item
                            label="Имя контрагента"
                            name="contragent_name"
                        >
                            <ContragentAutocomplete
                                api={this.api}
                                token={this.props.token}
                                onChange={this.onChangeCaName}
                                onSelect={this.onSelectCa}
                            />
                        </Form.Item>

                        <Form.Item
                            label="Телефон контрагента"
                            name="contragent_phone"
                            rules={[
                                {
                                    pattern: "^\\d+$",
                                    message: "Телефон не должен содержать символы кроме цифр"
                                }
                            ]}
                        >
                            <NumericAutoComplete
                                api={this.api}
                                token={this.props.token}
                                options={phonesMeta}
                                by={"phone"}
                                onChange={this.onChangeCaPhone}
                                onSelect={this.onSelectCa}
                            />
                        </Form.Item>

                        <Form.Item
                            label="ИНН контрагента"
                            name="contragent_inn"
                            rules={[
                                {
                                    pattern: "^\\d+$",
                                    message: "ИНН не должен содержать символы кроме цифр"
                                }
                            ]}
                        >
                            <NumericAutoComplete
                                api={this.api}
                                token={this.props.token}
                                options={innsMeta}
                                by={"inn"}
                                onChange={this.onChangeCaInn}
                                onSelect={this.onSelectCa}
                            />
                        </Form.Item>

                        <Form.Item
                            label="Примечание"
                            name="contragent_desc"
                        >
                            <TextArea style={{ width: 320 }} disabled={this.state.disabled} rows={3} />
                        </Form.Item>

                        <Divider />

                        <Form.Item label="Статус карты:" name="status_card" valuePropName="checked">
                            <Switch onChange={(checked) => this.setState({ p_status: checked })} />
                        </Form.Item>


                        <Form.Item label="Начало действия карты:" name="start_period">

                            <DatePicker
                                // presets={rangePresets}
                                showTime
                                allowClear={false}
                                format="DD.MM.YY HH:mm:ss"
                            // onChange={onRangeChange}
                            />

                        </Form.Item>

                        <Form.Item label="Конец действия карты:" name="end_period">

                            <DatePicker
                                // presets={rangePresets}
                                showTime
                                allowClear={false}
                                format="DD.MM.YY HH:mm:ss"
                            // onChange={onRangeChange}
                            />

                        </Form.Item>


                        <Divider />



                        <Form.Item {...tailLayout}>
                            <Button type="primary" htmlType="submit" style={{ marginRight: 5 }}>
                                Подтвердить
                            </Button>
                            <Button htmlType="button" onClick={this.handleCancel}>
                                Отмена
                            </Button>
                        </Form.Item>


                    </Form>

                </Modal>
            </>
        );
    }
}

export default CreateLCard;