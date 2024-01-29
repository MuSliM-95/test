//модальное окно добавления контрагента на странице "Контрагенты"
import {
    Button,
    Form,
    Input,
    Modal,
    Alert,
    message,
} from 'antd';

import { PlusOutlined } from '@ant-design/icons';

import React from "react";
// import moment from "moment";

import axios from 'axios';


import { NumericAutoComplete } from 'src/shared'
import { ContragentAutocomplete } from 'src/shared'

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


class NewCAModal extends React.Component {
    formRef = React.createRef();

    state = {
        visible: false,
        p_status: true,
        namesMeta: this.props.name_meta,
        tagsMeta: this.props.tags_meta,
        tagsChanged: false,

        current_contragent: null,
        isNewContr: false,
        newContrName: null,
        isContrCleared: true,
    };

    api = `https://${process.env.REACT_APP_APP_URL}/api/v1/`

    showModal = () => {
        this.setState({
            selected: [],
            visible: true,
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
        } = this.state


        let body = { name: values.contragent_name, inn: values.contragent_inn, phone: values.contragent_phone, description: values.contragent_desc }
        // for (let i in body) {
        //     if (!body[i]) {
        //         delete body[i]
        //     }
        // }

        // Если выбран (апдейт)
        if (
            !isContrCleared && !isNewContr &&
            (values.contragent_name !== current_contragent.name ||
                values.contragent_phone !== current_contragent.phone ||
                values.contragent_inn !== current_contragent.inn ||
                values.contragent_desc !== current_contragent.description)
        ) {
            axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${current_contragent.id}/?token=${this.props.token}`, body)
                .then((res) => {
                    message.success('Вы успешно изменили контрагента');
                    this.props.updateRow(body, res);
                })
                .catch(err => {
                    if (err.response) {
                        this.props.updateRow(this.props.ca, err.response);
                    } else {
                        this.props.updateRow(this.props.ca, { status: 500, data: err.message });
                    }
                });
        }

        // Если ничего не поменялось
        if (
            !isContrCleared && !isNewContr &&
            values.contragent_name === current_contragent.name &&
            values.contragent_phone === current_contragent.phone &&
            values.contragent_inn === current_contragent.inn &&
            values.contragent_desc === current_contragent.description
        ) {

        }

        // Если новый контр
        if (!isContrCleared && isNewContr) {
            axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/?token=${this.props.token}`, body)
                .then((res) => {
                    message.success('Вы успешно создали контрагента');
                    this.props.updateRow(body, res);
                })
                .catch(err => {
                    if (err.response) {
                        this.props.updateRow(this.props.ca, err.response);
                    } else {
                        this.props.updateRow(this.props.ca, { status: 500, data: err.message });
                    }
                });
        }

    };


    handleCancel = () => {
        this.setState({
            visible: false,
            selected: [],
            ca_alert_name: null,
            isNewContr: false,
            isContrCleared: true
        });
    };


    findContragent = async (id) => {
        return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${id}/?token=${this.props.token}`)
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
                    Создать контрагента
                </Button>
                <Modal
                    open={visible}
                    title="Создание нового контрагента"
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

export default NewCAModal;