import React, { Component } from "react";
import { Table, DatePicker, Button } from "antd";
import DebounceSelect from "./DebFetch";
import axios from "axios";

const { RangePicker } = DatePicker;

class WarehousesBalances extends Component {
    constructor(props) {
        super(props);

        this.columns = [
            {
                title: 'Наименование номенклатуры',
                dataIndex: 'name',
                key: 'name',
            },
            {
                title: 'Наименование организации',
                dataIndex: 'organization_name',
                key: 'organization_name',
            },
            {
                title: 'Наименование склада',
                dataIndex: 'warehouse_name',
                key: 'warehouse_name',
            },
            {
                title: 'Начальный остаток',
                dataIndex: 'start_ost',
                key: 'start_ost',
            },
            {
                title: 'Поступление',
                dataIndex: 'plus_amount',
                key: 'plus_amount',
            },
            {
                title: 'Расход',
                dataIndex: 'minus_amount',
                key: 'minus_amount',
            },
            {
                title: 'Остаток',
                dataIndex: 'current_amount',
                key: 'current_amount',
            },
        ];

        this.state = {
            dataSource: null,
            currWarehouse: null,
            datesArr: null
        }
    }

    fetchWarehouse = async (name) => {
        if (name) {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/warehouses/?name=${name}&token=${this.props.token}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    // if (body.result) {
                    return body.result.map((payment) => ({
                        label: payment.name,
                        value: payment.id,
                    }))

                    // }
                })
                .then((body) => {
                    return body
                })
        }
        else {
            return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/warehouses/?token=${this.props.token}`)
                .then((response) => response.json())
                .then((body) => {
                    return body
                })
                .then((body) => {
                    // if (body.result) {
                    return body.result.map((payment) => ({
                        label: payment.name,
                        value: payment.id,
                    }))

                    // }
                })
                .then((body) => {
                    return body
                })
        }
    }

    onWarehouseSelect = (params) => {
        axios
            .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_warehouse_balances/`,
                {
                    params: params,
                }
            )
            .then((res) => {

                this.setState({
                    dataSource: res.data.result,
                });

            });
    }

    getBalance = () => {
        const { currWarehouse, datesArr } = this.state

        let params = { token: this.props.token, warehouse_id: currWarehouse }

        if (datesArr) {
            params.date_from = datesArr[0].unix()
            params.date_to = datesArr[1].unix()
        }

        this.onWarehouseSelect(params)

    }



    render() {
        return (
            <>
                <DebounceSelect
                    // mode="multiple"
                    style={{ marginBottom: 10 }}
                    placeholder="Введите имя склада"
                    fetchOptions={this.fetchWarehouse}
                    removeIcon={null}
                    onSelect={(user) => this.setState({ currWarehouse: user })}

                />
                <RangePicker style={{ marginLeft: 10 }} onChange={(dates) => this.setState({ datesArr: dates })} />
                <Button onClick={this.getBalance} style={{ marginLeft: 10 }}>Найти</Button>
                <Table dataSource={this.state.dataSource} columns={this.columns} />
            </>
        );
    }
}

export default WarehousesBalances;
