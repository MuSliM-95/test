import React from 'react';
import { Modal, Button, Form, AutoComplete, Input, message } from 'antd';
import axios from 'axios';

const { TextArea } = Input;

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
        span: 15,
    },
};

const validateMessages = {
    /* eslint-disable no-template-curly-in-string */
    required: '${label} обязательно!',
};

class NewCAModal extends React.Component {
    formRef = React.createRef();

    constructor(props) {
        super(props);

        this.state = {
            isModalVisible: false,
            init_values: {
            }
        };

    }
    render() {

        const showModal = () => {
            this.setState({ isModalVisible: true });
        };

        const handleOk = (data) => {
            this.setState({ isModalVisible: false });

            axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${this.props.token}`, data)
                .then(response => {
                    message.success('Вы успешно создали контрагента');
                    this.props.updateRow(data, response);
                })
                .catch(err => {
                    if (err.response) {
                        this.props.updateRow(this.props.ca, err.response);
                    } else {
                        this.props.updateRow(this.props.ca, { status: 500, data: err.message });
                    }
                });

        };

        const handleCancel = () => {
            this.setState({ isModalVisible: false });
        };

        return (
            <>
                <Button onClick={showModal}>
                    Добавить контрагента
                </Button>
                <Modal destroyOnClose={true} footer={null} title="Добавить нового контрагента" open={this.state.isModalVisible} onCancel={handleCancel}>

                    <Form {...layout}
                        ref={this.formRef}
                        name="control-ref"
                        validateMessages={validateMessages}
                        onFinish={handleOk}
                        initialValues={this.state.init_values}
                    >

                        <Form.Item
                            label="Имя контрагента"
                            name="name"
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
                            label="Телефон контрагента"
                            name="phone"
                            rules={[
                                {
                                    pattern: "^\\d+$",
                                    message: "Телефон не должен содержать символы кроме цифр"
                                }
                            ]}
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
                            label="ИНН контрагента"
                            name="inn"
                            rules={[
                                {
                                    pattern: "^\\d+$",
                                    message: "ИНН не должен содержать символы кроме цифр"
                                }
                            ]}
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
                            label="Примечание"
                            name="description"
                        >
                            <TextArea rows={3} />
                        </Form.Item>

                        <Form.Item {...tailLayout}>
                            <Button type="primary" htmlType="submit" style={{ marginRight: 5 }}>
                                Подтвердить
                            </Button>
                            <Button htmlType="button" onClick={handleCancel}>
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