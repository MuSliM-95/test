import React from 'react';
import { Modal, Button, Form, Input, InputNumber, message } from 'antd';
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
        span: 15,
    },
};

const validateMessages = {
    /* eslint-disable no-template-curly-in-string */
    required: '${label} обязательно!',
};

class NewProjModal extends React.Component {
    formRef = React.createRef();

    constructor(props) {
        super(props);

        this.state = {
            isModalVisible: false,
            init_values: {
                name: "Проект",
                proj_sum: 0
            }
        };

    }
    render() {

        const showModal = () => {
            this.setState({ isModalVisible: true });
        };

        const handleOk = (data) => {
            this.setState({ isModalVisible: false });

            axios.post(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${this.props.token}`, data)
                .then(response => {
                    message.success(<>Вы успешно создали проект</>);
                });

        };

        const handleCancel = () => {
            this.setState({ isModalVisible: false });
        };

        return (
            <>
                <Button onClick={showModal}>
                    Добавить проект
                </Button>
                <Modal destroyOnClose={true} footer={null} title="Добавить новый проект" open={this.state.isModalVisible} onCancel={handleCancel}>

                    <Form {...layout}
                        ref={this.formRef}
                        name="control-ref"
                        validateMessages={validateMessages}
                        onFinish={handleOk}
                        initialValues={this.state.init_values}
                    >

                        <Form.Item
                            name="name"
                            label="Название проекта"
                            rules={[
                                {
                                    required: true,
                                },
                            ]}
                        >
                            <Input style={{ width: 250 }}></Input>
                        </Form.Item>

                        <Form.Item
                            name="proj_sum"
                            label="Бюджет проекта"
                        >

                            <InputNumber step={0.01} precision={2} min={0} style={{ width: 250 }} />

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

export default NewProjModal;