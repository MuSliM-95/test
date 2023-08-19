import {
    AutoComplete,
    Button,

    Form,
    Input,

    Modal,

    message
} from 'antd';

import { EditOutlined,  } from '@ant-design/icons';

import React from "react";
import axios from 'axios';


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

const { TextArea } = Input;

class EditCA extends React.Component {
    formRef = React.createRef();

    state = {
    };


    showModal = () => {

        this.setState({
            visible: true
        });

    };

    handleOk = (item) => {
        axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/${this.props.ca.id}?token=${this.props.token}`, item)
            .then(response => {
                message.success('Вы успешно изменили контрагента');
                this.props.updateRow(this.props.ca, response);
            })
            .catch(err => {
                if (err.response) {
                    this.props.updateRow(this.props.ca, err.response);
                } else {
                    this.props.updateRow(this.props.ca, { status: 500, data: err.message });
                }
            });
        this.handleCancel();
    };


    handleCancel = () => {
        this.setState({
            visible: false,
        });
    };



    render() {
        const { visible } = this.state;

        return (
            <>
                <Button style={{ marginRight: 10 }} icon={<EditOutlined />} onClick={this.showModal}>
                </Button>
                <Modal
                    open={visible}
                    width={450}
                    title="Редактирование платежа"
                    destroyOnClose={true}
                    onCancel={this.handleCancel}
                    footer={null}
                >

                    <Form {...layout}
                        ref={this.formRef}
                        name="control-ref"
                        validateMessages={validateMessages}
                        onFinish={this.handleOk}
                        initialValues={this.props.ca}
                    >

                        <Form.Item
                            name="name"
                            label="Имя контрагента"
                            rules={[
                                {
                                    required: true,
                                },
                            ]}
                        >
                            <AutoComplete
                                style={{ width: 250 }}
                                // options={this.props.meta.names}
                                allowClear={false}
                                filterOption={(inputValue, option) =>
                                    option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
                            />
                        </Form.Item>

                        <Form.Item
                            name="inn"
                            label="ИНН контрагента"
                        >
                            <AutoComplete
                                style={{ width: 250 }}
                                // options={this.props.meta.inns}
                                allowClear={false}
                                filterOption={(inputValue, option) =>
                                    option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
                            />
                        </Form.Item>

                        <Form.Item
                            name="phone"
                            label="Телефон контрагента"
                        >
                            <AutoComplete
                                style={{ width: 250 }}
                                // options={this.props.meta.phones}
                                allowClear={false}
                                filterOption={(inputValue, option) =>
                                    option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
                            />
                        </Form.Item>

                        <Form.Item
                            name="description"
                            label="Описание"
                        >
                            <TextArea rows={3} />
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

export default EditCA;