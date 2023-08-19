import React, { Component } from 'react';

import { CheckOutlined, StopOutlined } from '@ant-design/icons';
import { Card, Modal } from 'antd';
import IntegrationModal from './IntegrationsModal';
import axios from 'axios';

const { Meta } = Card;

class Integrations extends Component {
    constructor(props) {
        super(props);
        this.state = {

        };
        // this.ws_connect();
    }

    componentDidMount() {
        axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/check_pair?token=${this.props.query.token}`)
            .then((data) => {

                if (data.data.result !== "paired") {
                    this.setState({ actions: [<IntegrationModal query={this.props.query} button_name={"Установить виджет"} />] })
                }

                else if (data.data.integration_status === "need_to_refresh") {
                    this.setState({ actions: [<IntegrationModal query={this.props.query} button_name={"Обновить токен"} />], status: <p style={{ color: "red" }}>Необходимо обновить токен</p> })
                }

                else {
                    if (data.data.integration_status === true) {
                        this.setState({
                            status: <p style={{ color: "green" }}>Работает</p>,
                            actions: [<StopOutlined onClick={() => this.disconnect()} />]
                        })
                    }
                    else if (data.data.integration_status === false) {
                        this.setState({
                            status: <p style={{ color: "red" }}>Отключено</p>,
                            actions: [<CheckOutlined onClick={() => this.connect()} />]
                        })
                    }
                }
            })
    }

    modal = (body, title) => {
        Modal.info({
            title: title,
            width: 350,
            maskClosable: true,
            content: body,
            onOk() { },
        });
    }

    ws_connect = () => {
        this.websocket = new WebSocket(`wss://${process.env.REACT_APP_APP_URL}/ws/${this.props.query.token}`)

        this.websocket.onmessage = message => {
            const data = JSON.parse(message.data)
            if ("integration_status" in data) {
                if (data.integration_status === true) {
                    this.setState({
                        status: <p style={{ color: "green" }}>Работает</p>,
                        actions: [<StopOutlined onClick={() => this.disconnect()} />]
                    })
                }
                else if (data.integration_status === false) {
                    this.setState({
                        status: <p style={{ color: "red" }}>Отключено</p>,
                        actions: [<CheckOutlined onClick={() => this.connect()} />]
                    })
                }
                else if (data.integration_status === "need_to_refresh") {
                    this.setState({ actions: [<IntegrationModal query={this.props.query} button_name={"Обновить токен"} />], status: <p style={{ color: "red" }}>Необходимо обновить токен</p> })
                }
            }
        }

    }

    disconnect = () => {
        axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/integration_unpair?token=${this.props.query.token}`)
            .then(() => this.modal("Интеграция отключена", "Успех"))
            .catch(err => console.log(err));
    }

    connect = () => {
        axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/integration_on?token=${this.props.query.token}`)
            .then(() => this.modal("Интеграция включена", "Успех"))
            .catch(err => console.log(err));
    }


    render() {

        return (
            <Card
                style={{ width: 250 }}
                cover={
                    <img
                        alt="amo_logo"
                        src="https://www.amocrm.ru/views/pages/landing/images/about_us/press_stuff/amocrm-logo-dark.png"
                    />
                }
                actions={this.state.actions}
            >
                <Meta
                    title="AmoCRM"
                    description={this.state.status}
                />
            </Card>
        );
    }
}

export default Integrations;