import React from 'react';
import axios from 'axios';
import {Button, Modal, Input} from "antd";


class IntegrationModal extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            isModalVisible: false,
        };
    }

    modal = (body, title) => {
        Modal.info({
            title: title,
            width: 350,
            maskClosable: true,
            destroyOnClose: true,
            content: body,
            onOk() {},
        });
    }

    render() {


        const showModal = () => {
            this.setState({isModalVisible: true});
        };

        const handleOk = () => {
            axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/integration_pair?token=${this.props.query.token}&amo_token=${this.state.token_in}`)
            .then((data) => {
                // if (data.result === "incorrect pair token!") {
                //     this.modal("Некорректный токен! Проверьте правильность и попробуйте еще раз", "Ошибка")
                // }

                    this.modal("Виджет успешно установлен", "Успех")
            })
            .catch(err => this.modal("Некорректный токен! Проверьте правильность и попробуйте еще раз", "Ошибка"));
            this.setState({isModalVisible: false});
        };

        const handleCancel = () => {
            this.setState({isModalVisible: false});
        };

        return (
            <>
                <Button onClick={showModal}>
                    {this.props.button_name}
                </Button>
                <Modal destroyOnClose={true} title="Установка виджета" open={this.state.isModalVisible} onOk={handleOk} onCancel={handleCancel}>
                    Пожалуйста, введите ваш токен из виджета в <a href={"https://amocrm.ru"}>amocrm.ru</a>.<br/>В поиске виджетов вбейте <a href={"https://tablecrm.com"}>tablecrm.com</a>
                    <br/>
                    <br/>
                    <Input placeholder="Ваш токен" onChange={(dat) => this.setState({token_in: dat.target.value})}/>
                </Modal>
            </>
        );
    }
}

export default IntegrationModal;