import {
    AutoComplete,
    Button,
    DatePicker,
    Divider,
    Form,
    Input,
    InputNumber,
    Modal,
    Radio,
    Switch,
    Alert,
    Select,
} from 'antd';

import { PlusOutlined } from '@ant-design/icons';

import React from "react";
// import moment from "moment";

import axios from 'axios';

import TextBlock from './textblock';

import DebounceSelect from './DebFetch';
import NumericAutoComplete from './NumericAutoComplete';
import ContragentAutocomplete from './ContragentAutocomplete';
import ArticleAutocomplete from './ArticleAutocomplete';

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
    type: "incoming",
    status: true,
    date: dayjs(),
    description: null,
    repeat_switch: false,
    repeat_freq_int: 1,
    amount: 1,
    tax: 0,
    tax_type: "internal",
    project: "0"
}

const { TextArea } = Input;


class CreatePayment extends React.Component {
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
            tax: "0",
            pbox_label: "Счет",
            pbox_to_label: "Счет",
            disabled: false,
            required: true,
            ca_value: []
        });
        init_values.paybox = parseInt(this.props.payboxes[0].key)
        init_values.paybox_to = parseInt(this.props.payboxes[0].key)
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
            isNewArticle,
            current_article,
            isArticleCleared,
            p_status
        } = this.state

        let requestBody = {}


        values.date = values.date.unix()

        if (values.tags) {
            values.tags = values.tags.map((item) => item.label).join(",")
        }
        else {
            delete values.tags
        }

        for (const [key, value] of Object.entries(values)) {
            if (value) {
                requestBody[key] = value
            }
        }

        let artBody = { name: values.article_id, emoji: "🛍️" }

        if (isArticleCleared) {
            requestBody.article_id = null
        }

        // Если выбран (апдейт)
        if (
            !isArticleCleared && !isNewArticle &&
            (values.article_id !== current_article.name)

        ) {
            requestBody.article_id = current_article.id
            await axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles/${current_article.id}?token=${this.props.token}`, artBody)
        }

        // Если ничего не поменялось
        if (
            !isArticleCleared && !isNewArticle &&
            values.article_id === current_article.name
        ) {
            requestBody.article_id = current_article.id
        }

        // Если новый контр
        if (!isArticleCleared && isNewArticle) {
            const resp = await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles?token=${this.props.token}`, artBody)
            requestBody.article_id = resp.data.id
        }

        requestBody.stopped = true
        requestBody.amount_without_tax = Number(values.amount).toFixed(2)
        requestBody.amount = () => {
            if (this.state.paymentType !== "transfer") {
                const tax = values.tax
                const tax_type = values.tax_type
                const amount = Number(values.amount).toFixed(2)

                if (tax === 0) return Number(amount)

                else {
                    if (tax_type === "internal") {
                        return Number(amount).toFixed(2)
                    }
                    else {
                        return Number((amount * 100) / (100 - Number(tax))).toFixed(2)
                    }
                }
            }
            else {
                return Number(values.amount).toFixed(2)
            }
        }

        requestBody.amount = requestBody.amount()
        requestBody.repeat_freq = 0

        if (this.state.paymentType !== "transfer") {
            let body = { name: values.contragent_name, inn: values.contragent_inn, phone: values.contragent_phone, description: values.contragent_desc }
            for (let i in body) {
                if (!body[i]) {
                    delete body[i]
                }
            }

            // Если очистили контра
            if (isContrCleared) {
                requestBody.contragent = null
            }

            // Если выбран (апдейт)
            if (
                !isContrCleared && !isNewContr &&
                (values.contragent_name !== current_contragent.name ||
                    values.contragent_phone !== current_contragent.phone ||
                    values.contragent_inn !== current_contragent.inn ||
                    values.contragent_desc !== current_contragent.description)
            ) {
                requestBody.contragent = current_contragent.id
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
                requestBody.contragent = current_contragent.id
            }

            // Если новый контр
            if (!isContrCleared && isNewContr) {
                const resp = await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${this.props.token}`, body)
                requestBody.contragent = resp.data.id
            }

            delete values.contragent_name
            delete values.contragent_phone
            delete values.contragent_inn
            delete values.contragent_desc
        }


        else {
            delete values.contragent_name
            delete values.contragent_phone
            delete values.contragent_inn
            delete values.contragent_desc

            requestBody.paybox_to = values.paybox_to;
            requestBody['contragent'] = null
        }

        requestBody.status = p_status;
        
        await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments?token=${this.props.token}`, requestBody)

        this.handleCancel()
    };


    handleCancel = () => {
        this.setState({
            p_status: true,
            paymentType: "incoming",
            visible: false,
            repeat_freq: undefined,
            repeat_freq_int: 1,
            selected: [],
            tax: "0",
            tax_type: "internal",
            amount: 1.00,
            ca_alert_name: null,
            current_article: null,
            isNewArticle: false,
            newArticleName: null,
            isArticleCleared: true,
        });
    };

    onChangeArt = (val) => {
        const { current_article } = this.state
        if (current_article) {
            if (val !== current_article.name) {
                this.setState({ isNewArticle: true, newArticleName: val, isArticleCleared: false })
            }
            else {
                this.setState({ isNewArticle: false, newArticleName: val })
                this.formRef.current.setFieldsValue({
                    article_id: current_article.name,
                })
            }
        }
        else {
            this.setState({ isNewArticle: true, newArticleName: val, isArticleCleared: false })
        }
    }

    onSelectArt = (val) => {
        // this.setState({ isContrCleared: false })
        this.findArticle(val).then(res => {
            this.setState({ current_article: res, isNewArticle: false, isArticleCleared: false })
            this.formRef.current.setFieldsValue({
                article_id: res.name
            })
        });
    }

    articleUnselect = () => {
        this.setState({ isNewArticle: false, isArticleCleared: true })
        this.formRef.current.setFieldsValue({
            article_id: ""
        })
    }

    findArticle = async (id) => {
        return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles/${id}?token=${this.props.token}`)
            .then((response) => response.json())
            .then((body) => {
                return body
            })
    }


    fetchNames = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments_meta?token=${this.props.token}&name=${name}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body) {
                        let res = body.map((payment) => (
                            {
                                label: `${payment.name}`,
                                value: payment.id,
                            }
                        ))
                        // console.log(res)
                        this.setState({ namesMeta: res })
                        return res
                    }
                })
                .then((body) => {
                    return body
                })
        }
    }

    fetchTags = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments_meta?token=${this.props.token}&tags=${name}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body) {
                        let res = body.map((payment) => payment.tags.split(",")).flat(1)
                        let tags = res.filter((item) => item.toUpperCase().indexOf(name.toUpperCase()) !== -1)
                        let return_tags = tags.map((value, i) => {
                            return {
                                label: value,
                                value: i,
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

    fetchProjects = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${this.props.token}&name=${name}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result) {
                        return body.result.map((payment) => ({
                            label: payment.name,
                            value: payment.id,
                        }))

                    }
                }
                )
                .then((body) => {
                    return body
                })
        }
        else {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${this.props.token}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result) {
                        return body.result.map((payment) => ({
                            label: payment.name,
                            value: payment.id,
                        }))

                    }
                }
                )
                .then((body) => {
                    return body
                })
        }
    }

    

    fetchPayboxes = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${this.props.token}&name=${name}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result) {
                        return body.result.map((payment) => {
                            let color = "red"
                            if (payment.balance > 0) {
                                color = "green"
                            }
                            return {
                                label: <>{payment.name}: <font color={color}>{payment.balance} руб.</font></>,
                                value: payment.id,
                            }
                        })

                    }
                }
                )
                .then((body) => {
                    return body
                })
        }
        else {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${this.props.token}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    if (body.result) {
                        return body.result.map((payment) => {
                            let color = "red"
                            if (payment.balance > 0) {
                                color = "green"
                            }
                            return {
                                label: <>{payment.name}: <font color={color}>{payment.balance} руб.</font></>,
                                value: payment.id,
                            }
                        })

                    }
                }
                )
                .then((body) => {
                    return body
                })
        }
    }

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
            namesMeta,
            tagsMeta,

            isArticleCleared,
            isNewArticle,
            newArticleName,
            current_article
        } = this.state;

        const { Option } = Select;

        return (
            <>
                <Button style={{ marginBottom: 15 }} icon={<PlusOutlined />} type="primary" onClick={this.showModal}>
                    Добавить платеж
                </Button>
                <Modal
                    open={visible}
                    width={550}
                    title="Создание нового платежа"
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
                            name="name"
                            label="Название платежа"
                        >
                            <AutoComplete
                                style={{ width: 300 }}
                                placeholder={"Введите название платежа"}
                                options={namesMeta}
                                allowClear={true}
                                onSearch={this.fetchNames}
                            />
                        </Form.Item>

                        {
                            !isArticleCleared ?
                                <>
                                    {
                                        !isNewArticle ?
                                            <div>
                                                <Alert
                                                    type="success"
                                                    message={<div>Выбран <b>{current_article.name}</b></div>}
                                                    showIcon
                                                    action={
                                                        <Button size="small" type="text" onClick={this.articleUnselect}>
                                                            Очистить
                                                        </Button>
                                                    } />
                                                <br />
                                            </div>
                                            :
                                            <div>
                                                <Alert
                                                    type="info"
                                                    message={<div>Вы создаете <b>{newArticleName}</b></div>}
                                                    showIcon
                                                    action={
                                                        <Button size="small" type="text" onClick={this.articleUnselect}>
                                                            Очистить
                                                        </Button>
                                                    } />
                                                <br />
                                            </div>
                                    }
                                </> : null
                        }

                        <Form.Item
                            name="article_id"
                            label="Статья платежа"
                        >
                            <ArticleAutocomplete
                                api={this.api}
                                token={this.props.token}
                                placeholder="Введите название проекта"
                                onChange={this.onChangeArt}
                                onSelect={this.onSelectArt}
                                // fetchOptions={this.fetchArticles}
                                style={{
                                    width: 300
                                }}
                            />
                        </Form.Item>

                        <Form.Item
                            name="tags"
                            label="Теги платежа"
                        >

                            <Select
                                style={{ width: 300 }}
                                options={tagsMeta}
                                allowClear={true}
                                showSearch
                                placeholder={"Введите теги платежа"}
                                labelInValue
                                filterOption={false}
                                mode={"tags"}
                                onSearch={this.fetchTags}
                            />

                        </Form.Item>

                        <Form.Item
                            name="project_id"
                            label="Проект платежа"
                        >
                            <DebounceSelect
                                placeholder="Введите название проекта"
                                fetchOptions={this.fetchProjects}
                                removeIcon={null}
                                style={{
                                    width: 300
                                }}
                            />
                        </Form.Item>

                        <Form.Item
                            name="paybox"
                            label={this.state.pbox_label}
                        >
                            <DebounceSelect
                                placeholder="Выберите счет"
                                fetchOptions={this.fetchPayboxes}
                                removeIcon={null}
                                style={{
                                    width: 300
                                }}
                            />

                        </Form.Item>

                        <Form.Item label="Тип платежа" name="type" onChange={(event) => {
                            this.setState({ paymentType: event.target.value })
                            if (event.target.value === "transfer") {
                                this.setState({ pbox_label: "Счет списания", pbox_to_label: "Счет зачисления" })
                            }
                            else {
                                this.setState({ pbox_label: "Счет", pbox_to_label: "Счет" })
                            }
                        }}>
                            <Radio.Group>
                                <Radio.Button value="incoming">Приход</Radio.Button>
                                <Radio.Button value="outgoing">Расход</Radio.Button>
                                <Radio.Button value="transfer">Перевод</Radio.Button>
                            </Radio.Group>
                        </Form.Item>

                        <Divider />

                        <Form.Item label="Статус платежа:" name="status" valuePropName="checked">
                            <Switch onChange={(checked) => this.setState({ p_status: checked })} />
                        </Form.Item>

                        {this.state.p_status === false && this.state.paymentType !== "transfer" ? (
                            this.state.paymentType === "incoming" ? (
                                <Form.Item label="Дебиторка" name="deb_cred" valuePropName="checked">
                                    <Switch />
                                </Form.Item>
                            ) :
                                <Form.Item label="Кредиторка" name="deb_cred" valuePropName="checked">
                                    <Switch />
                                </Form.Item>
                        ) : null}

                        <Form.Item label="Дата платежа:" name="date">

                            <DatePicker
                                style={{ width: 300 }}
                                placeholder={"Выберите дату"}
                            />

                        </Form.Item>

                        <Divider />

                        <Form.Item label={"Сумма платежа"} name="amount">
                            <InputNumber
                                style={{ width: 300 }}
                                placeholder={"Введите сумму платежа"}
                                onChange={(amount) => this.setState({ amount: amount })}
                                precision={2}
                                min="0.01"
                                step="0.01"
                                stringMode
                            />

                        </Form.Item>

                        {this.state.paymentType === "transfer" ? (
                            <Form.Item
                                name="paybox_to"
                                label={this.state.pbox_to_label}
                            >
                                <DebounceSelect
                                    placeholder="Выберите счет"
                                    fetchOptions={this.fetchPayboxes}
                                    removeIcon={null}
                                    style={{
                                        width: 300
                                    }}
                                />

                            </Form.Item>
                        ) : (
                            <>
                                <Form.Item label={"Налог"} name="tax">
                                    <InputNumber
                                        min={0}
                                        max={100}
                                        step={0.01}
                                        formatter={value => `${value}%`}
                                        parser={value => value.replace('%', '')}
                                        onChange={(tax) => {
                                            let new_tax = tax
                                            if (new_tax === 0) {
                                                new_tax = "0"
                                            }
                                            this.setState({ tax: new_tax })
                                        }}
                                        style={{ width: 300 }}
                                    />
                                </Form.Item>

                                <Form.Item label="Тип налога" name="tax_type">
                                    <Radio.Group onChange={(event) => this.setState({ tax_type: event.target.value })}>
                                        <Radio.Button value="internal">Внутренний</Radio.Button>
                                        <Radio.Button value="external">Внешний</Radio.Button>
                                    </Radio.Group>
                                </Form.Item>

                                <TextBlock
                                    tax={this.state.tax ? this.state.tax : init_values.tax}
                                    tax_type={this.state.tax_type ? this.state.tax_type : init_values.tax_type}
                                    amount={this.state.amount ? this.state.amount : init_values.amount}
                                />

                            </>
                        )}



                        <Divider />

                        {/* <Form.Item label="Повтор операции:" name="repeat_switch" valuePropName="checked">
                            <Switch onChange={(checked) => this.setState({repeat_switch: checked, repeat_freq: "year", selected: []})}/>
                        </Form.Item> */}

                        {this.state.repeat_switch ? (
                            <Form.Item label="Каждые:" name="repeat">

                                <Input.Group>
                                    <InputNumber
                                        style={{ width: '58%', marginRight: 5 }}
                                        onChange={(val) => this.setState({ repeat_freq_int: val })}
                                        defaultValue={1}
                                        min={1}
                                    >
                                    </InputNumber>

                                    <Select defaultValue="year"
                                        style={{ width: '40%' }}
                                        onChange={(event) => {
                                            this.setState({
                                                repeat_freq: event,
                                                selected: [],
                                            })
                                        }}
                                    >
                                        <Option value="year">лет</Option>
                                        <Option value="month">месяцев</Option>
                                        <Option value="week">недель</Option>
                                        <Option value="day">дней</Option>
                                        <Option value="hour">часов</Option>
                                        <Option value="minute">минут</Option>
                                        <Option value="second">секунд</Option>
                                    </Select>

                                </Input.Group>

                            </Form.Item>
                        ) : null}

                        <Divider />

                        <Form.Item label="Примечание:" name="description">
                            <TextArea rows={3} />
                        </Form.Item>

                        <Divider />

                        {this.state.paymentType !== "transfer" ? <>
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

                        </> : null}

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

export default CreatePayment