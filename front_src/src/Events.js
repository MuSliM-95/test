import React from 'react';
import { Button, Modal, Table } from 'antd';

import axios from 'axios';
import ReactJson from "react18-json-view";
import 'react18-json-view/src/style.css';
// import VirtualTable from './VirtualTable';

class Events extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            dataSource: [],
            currentPage: 1,
            loading: true
        };
        this.columns = [
            {
                title: 'Идентификатор',
                dataIndex: 'id',
                key: 'id',
            },
            {
                title: 'Тип события',
                dataIndex: 'type',
                key: 'type',
            },
            {
                title: 'Название',
                dataIndex: 'name',
                key: 'name',
            },
            {
                title: 'Метод',
                dataIndex: 'method',
                key: 'method',
            },
            {
                title: 'Ссылка',
                dataIndex: 'url',
                key: 'url',
                // render: (url) => <CopyLink link={url}/>
                render: (url) => <a href={url} target={"_blank"} rel={"noopener noreferrer"}>{url}</a>
            },
            {
                title: 'IP-адрес',
                dataIndex: 'ip',
                key: 'ip',
            },
            {
                title: 'Дата события',
                dataIndex: 'created_at',
                key: 'created_at',
                render: (ts) => {
                    const date = new Date(ts);
                    const strDate = ('0' + date.getDate()).slice(-2) + '.'
                        + ('0' + (date.getMonth() + 1)).slice(-2) + '.'
                        + date.getFullYear();
                    const strTime = ('0' + date.getHours()).slice(-2) + ':'
                        + ('0' + date.getMinutes()).slice(-2) + ':'
                        + ('0' + date.getSeconds()).slice(-2)
                    return strDate + " " + strTime
                }
            },
            {
                title: 'Тело запроса',
                dataIndex: 'payload',
                key: 'payload',
                render: (body, row) => {
                    if (row.method === "GET") {
                        return "GET-запрос не имеет тела"
                    }
                    else {
                        return <Button onClick={() => { this.modalRequest(body, "Тело запроса") }}>Просмотр</Button>
                    }
                }
                // render: (body) => <Button onClick={() => { this.modalRequest(JSON.parse(body), "Тело запроса") }}>Просмотр</Button>
            },
        ];
    }

    componentDidMount() {
        this.fetch(1, `https://${process.env.REACT_APP_APP_URL}/api/v1/events/${this.props.token}`);
    }

    modalRequest = (body, title) => {
        Modal.info({
            title: title,
            width: 750,
            maskClosable: true,
            content: (
                <div>
                    <ReactJson src={body} displayDataTypes={false} />
                </div>
            ),
            onOk() { },
        });
    }


    fetch = (page, url = {}) => {
        axios({
            method: "get",
            url: url,
            params: this.getPage(page)
        })
            .then(data => {
                this.setState({
                    dataSource: data.data.result,
                    count: data.data.count,
                    loading: false
                });
            });
    }

    getPage = (page) => {
        return {
            limit: 20,
            offset: (page * 20) - 20,
        }
    }

    render() {

        return (
            <div>
                <Table
                    components={[]}
                    columns={this.columns}
                    dataSource={this.state.dataSource}
                    loading={this.state.loading}
                    // scroll={{
                    //     y: 620,
                    //     x: '85vw',
                    // }}
                    style={{ marginRight: 15 }}
                    rowKey={record => record.id}
                    pagination={
                        {
                            total: this.state.count,
                            onChange: page => {
                                this.setState({ currentPage: page, loading: true }, this.fetch(page, `https://${process.env.REACT_APP_APP_URL}/api/v1/events/${this.props.token}`))
                            },
                            pageSize: 20
                        }
                    }
                />
            </div>

        );
    }
}

export default Events;