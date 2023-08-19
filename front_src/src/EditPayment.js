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
    Spin
} from 'antd';

import { EditOutlined, SwapOutlined, ExclamationCircleOutlined } from '@ant-design/icons';

import React from "react";
import axios from 'axios';
// import moment from "moment";
import TextBlock from './textblock';
// import DebounceSelect from './DebFetch';
import ContragentAutocomplete from './ContragentAutocomplete'
import ArticleAutocomplete from './ArticleAutocomplete'
import NumericAutoComplete from './NumericAutoComplete'

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import DebounceSelect from './DebFetch';
dayjs.extend(utc)

const { confirm } = Modal;


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

const { TextArea } = Input;


class EditPayment extends React.Component {
    constructor(props) {
        super(props);

        this.state = {
            modalOpened: false,
            isLoaded: false,
            namesMeta: this.props.name_meta,
            tagsMeta: this.props.tags_meta,
            tagsChanged: false,
            paymentType: this.props.payment.type,
            current_contragent: null,
            isNewContr: false,
            newContrName: null,
            findedContr: null,
            isContrCleared: true,
            isNewArticle: false
        };

        this.api = `https://${process.env.REACT_APP_APP_URL}/api/v1/`

    }

    formRef = React.createRef();

    swap_payboxes = () => {
        const values = this.formRef.current.getFieldValue();

        this.formRef.current.setFieldsValue({
            paybox: values.paybox_to,
            paybox_to: values.paybox,
        })
    }


    showModal = () => {
        const { payment, caMeta } = this.props

        const caPhones = caMeta.map((item) => { return { label: item.phone, value: item.id } }).filter((item) => item.label)
        const caInns = caMeta.map((item) => { return { label: item.inn, value: item.id } }).filter((item) => item.label)

        if (payment.contragent) {
            axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${payment.contragent}`, { params: { token: this.props.token } })
                .then(response => {
                    this.setState({
                        modalOpened: true,
                        isNewContr: false,
                        isContrLoaded: true,
                        isContrCleared: false,
                        current_contragent: response.data,
                        phonesMeta: caPhones,
                        innsMeta: caInns
                    })
                });
        }

        else {
            this.setState({
                modalOpened: true,
                isContrLoaded: true,
                isNewContr: false,
                isContrCleared: true,
                current_contragent: null,
                phonesMeta: caPhones,
                innsMeta: caInns
            })
        }

        if (payment.article_id) {
            axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles/${payment.article_id}`, { params: { token: this.props.token } })
                .then(response => {
                    this.setState({
                        modalOpened: true,
                        isNewArticle: false,
                        isArticleLoaded: true,
                        current_article: response.data,
                    })
                });
        }

        else {
            this.setState({
                modalOpened: true,
                isArticleLoaded: true,
                isNewArticle: false,
                current_article: null,
            })
        }
    }

    closeModal = () => {
        this.setState({ modalOpened: false })
    }

    randomInteger = (min, max) => {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    setInitialValues = () => {
        const { current_contragent, current_article } = this.state

        let startValues = Object.assign({}, this.props.payment)
        // Разбиение тегов платежа
        startValues.tags = () => {
            if (this.props.payment.tags) {
                return this.props.payment.tags.split(",").map((item, i) => {
                    return {
                        label: item,
                        value: this.randomInteger(10000, 15000)
                    }
                });
            }
            else {
                return [];
            }
        }
        startValues.tags = startValues.tags();

        startValues.date = dayjs.unix(startValues.date)

        if (current_contragent) {
            startValues.contragent_name = current_contragent.name
            startValues.contragent_phone = current_contragent.phone
            startValues.contragent_inn = current_contragent.inn
            startValues.contragent_desc = current_contragent.description
        }

        if (current_article) {
            startValues.article_id = current_article.name
        }

        return startValues
    }

    getAmount = (item) => {
        if (item.type === "transfer") return item.amount_without_tax
        else {

            const tax = item.tax
            const tax_type = item.tax_type
            const amount = Number(item.amount_without_tax).toFixed(2)


            if (tax === "0") return Number(amount)

            else {
                if (tax_type === "internal") {
                    return Number(amount).toFixed(2)
                }
                else {
                    return Number((amount * 100) / (100 - Number(tax))).toFixed(2)
                    // return (Number(amount) + Number(amount * (Number(tax) / 100))).toFixed(2)
                }
            }
        }
    }

    onFinish = async (values) => {

        const {
            isNewContr,
            current_contragent,
            isContrCleared,

            isNewArticle,
            current_article,
            isArticleCleared
        } = this.state

        // console.log(values.tags)

        let requestBody = {}

        values.date = values.date.unix()
        values.tags = values.tags.map((item) => item.label).join(",")

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

        let artBody = { name: values.article_id, emoji: "🛍️" }

        if (isArticleCleared) {
            requestBody.article_id = null
        }

        // Если выбран (апдейт)
        if (
            !isArticleCleared && !isNewArticle && current_article &&
            (values.article_id !== current_article.name)

        ) {
            requestBody.article_id = current_article.id
            await axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles/${current_article.id}?token=${this.props.token}`, artBody)
        }

        // Если ничего не поменялось
        if (
            !isArticleCleared && !isNewArticle && current_article && 
            values.article_id === current_article.name
        ) {
            requestBody.article_id = current_article.id
        }

        // Если новый контр
        if (!isArticleCleared && isNewArticle) {
            const resp = await axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles?token=${this.props.token}`, artBody)
            requestBody.article_id = resp.data.id
        }


        for (var key in values) {

            if (key === "project_id" || key === "paybox") {
                if (parseInt(values[key]) !== this.props.payment[key]) {
                    requestBody[key] = values[key]
                }
                delete values[key]
            }

            if (key === "tax") {
                if (parseInt(values[key]) !== this.props.payment[key]) {
                    requestBody[key] = values[key]
                }
            }

        }

        for (const [key, value] of Object.entries(values)) {
            if (value !== this.props.payment[key]) {
                if (key !== "article_id") {
                    requestBody[key] = value
                }
            }

        }



        if (values.tax === 0) {
            requestBody.tax = "0"
        }

        if (requestBody.amount || requestBody.tax || requestBody.tax_type || requestBody.amount_without_tax) {
            requestBody.amount = this.getAmount(values)
        }

        if (values.type === "transfer") {
            delete requestBody.contragent
        }

        if (parseFloat(requestBody.tax) === parseFloat(this.props.payment.tax)) {
            delete requestBody.tax
        }

        if (parseFloat(requestBody.amount) === parseFloat(this.props.payment.amount)) {
            delete requestBody.amount
        }

        if (requestBody.project_id === "0" && this.props.payment.project_id === null) {
            delete requestBody.project_id
        }


        if (this.props.payment.raspilen && requestBody.project_id) {
            confirm({
                title: 'Подтверждение',
                icon: <ExclamationCircleOutlined />,
                content: 'Вы уверены? При смене проекта родительского платежа слетят проекты на всех дочерних',

                onOk: async () => {
                    await axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments/${this.props.payment.id}?token=${this.props.token}`, requestBody)
                },

                onCancel() {
                },
            });
        }

        else {
            await axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments/${this.props.payment.id}?token=${this.props.token}`, requestBody)
        }

        this.closeModal();
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

    // fetchArticles = async (name) => {
    //     if (name) {
    //         return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles?token=${this.props.token}&name=${name}`)
    //             .then((response) => response.json())
    //             .then((body) => {
    //                 return body
    //             })
    //             .then((body) => {
    //                 if (body.result) {
    //                     return body.result.map((payment) => ({
    //                         label: `${payment.name} ${payment.emoji}`,
    //                         value: payment.id,
    //                     }))

    //                 }
    //             }
    //             )
    //             .then((body) => {
    //                 return body
    //             })
    //     }
    //     else {
    //         return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles?token=${this.props.token}`)
    //             .then((response) => response.json())
    //             .then((body) => {
    //                 return body
    //             })
    //             .then((body) => {
    //                 if (body.result) {
    //                     return body.result.map((payment) => ({
    //                         label: `${payment.name} ${payment.emoji}`,
    //                         value: payment.id,
    //                     }))

    //                 }
    //             }
    //             )
    //             .then((body) => {
    //                 return body
    //             })
    //     }
    // }

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

    findArticle = async (id) => {
        return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/articles/${id}?token=${this.props.token}`)
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

    articleUnselect = () => {
        this.setState({ isNewArticle: false, isArticleCleared: true })
        this.formRef.current.setFieldsValue({
            article_id: ""
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

    onSelectArt = (val) => {
        // this.setState({ isContrCleared: false })
        this.findArticle(val).then(res => {
            this.setState({ current_article: res, isNewArticle: false, isArticleCleared: false })
            this.formRef.current.setFieldsValue({
                article_id: res.name
            })
        });
    }

    onChangeCaName = (val) => {
        const { current_contragent } = this.state
        if (current_contragent) {
            if (val !== current_contragent.name) {
                this.setState({ isNewContr: true, newContrName: val, isContrCleared: false })
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
            modalOpened,
            isContrLoaded,
            isArticleLoaded,
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
        } = this.state

        return (
            <>
                <Button style={{ marginRight: 10 }} icon={<EditOutlined />} onClick={() => this.showModal(this.props.payment)} />
                <Modal
                    open={modalOpened}
                    width={550}
                    title={"Редактирование платежа"}
                    destroyOnClose={true}
                    onCancel={this.closeModal}
                    footer={null}
                >
                    {
                        isContrLoaded && isArticleLoaded ? <Form
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
                                                <>
                                                    {current_article ? <div>
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
                                                    </div> : null}
                                                </>
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
                                    fetchOptions={this.fetchProjects}
                                    placeholder="Введите название проекта"
                                    removeIcon={null}
                                    style={{
                                        width: 300
                                    }}
                                />

                                {/* <ContragentAutocomplete
                                    api={this.api}
                                    token={this.props.token}
                                    onChange={this.onChangeCaName}
                                    onSelect={this.onSelectCa}
                                /> */}
                            </Form.Item>

                            <Form.Item
                                name="paybox"
                                label={this.state.paymentType === "transfer" ? "Счет списания" : "Счет"}
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

                            {this.state.paymentType === "transfer" ? (
                                <>
                                    <Button onClick={this.swap_payboxes} style={{ marginLeft: 100, marginBottom: 20 }} icon={<SwapOutlined />}>Поменять счета местами</Button>
                                    <Form.Item
                                        name="paybox_to"
                                        label={"Счет зачисления"}
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
                                </>
                            ) : null}


                            <Form.Item label="Тип платежа" name="type" onChange={(event) => this.setState({ paymentType: event.target.value })}>
                                {this.state.paymentType === "transfer" ? (
                                    <Radio.Group>
                                        <Radio.Button value="transfer">Перевод</Radio.Button>
                                    </Radio.Group>
                                ) : (
                                    <Radio.Group>
                                        <Radio.Button value="incoming">Приход</Radio.Button>
                                        <Radio.Button value="outgoing">Расход</Radio.Button>
                                    </Radio.Group>
                                )}
                            </Form.Item>

                            <Divider />

                            <Form.Item label="Статус платежа:" name="status" valuePropName="checked">
                                <Switch onChange={(checked) => this.setState({ paymentStatus: checked })} />
                            </Form.Item>

                            {this.state.paymentStatus === false && this.state.paymentType !== "transfer" ? (
                                this.props.payment.type === "incoming" ? (
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

                            <Form.Item label={"Сумма платежа"} name="amount_without_tax">
                                <InputNumber
                                    style={{ width: 300 }}
                                    placeholder={"Введите сумму платежа"}
                                    onChange={(amount) => this.setState({ amount: amount, amountIsChanged: true })}
                                    precision={2}
                                    min="0.01"
                                    step="0.01"
                                    stringMode
                                />

                            </Form.Item>
                            {this.state.p_type === "transfer" ? (null) : (
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
                                                this.setState({ tax: new_tax, tax_is_changed: true })
                                            }}
                                            style={{ width: 250 }}
                                        />
                                    </Form.Item>

                                    <Form.Item label="Тип налога" name="tax_type">
                                        <Radio.Group onChange={(event) => this.setState({ tax_type: event.target.value, tax_t_is_changed: true })}>
                                            <Radio.Button value="internal">Внутренний</Radio.Button>
                                            <Radio.Button value="external">Внешний</Radio.Button>
                                        </Radio.Group>
                                    </Form.Item>

                                    <TextBlock
                                        tax={this.state.tax ? this.state.tax : this.props.payment.tax}
                                        tax_type={this.state.tax_type ? this.state.tax_type : this.props.payment.tax_type}
                                        amount={this.state.amount ? this.state.amount : this.props.payment.amount_without_tax}
                                    />
                                </>
                            )}

                            <Divider />

                            <Form.Item label="Примечание:" name="description">
                                <TextArea rows={3} />
                            </Form.Item>

                            <Divider />

                            {this.props.payment.type !== "transfer" ? <>
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
                                    rules={[
                                        {
                                            required: this.state.required
                                        }
                                    ]}
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

export default EditPayment;