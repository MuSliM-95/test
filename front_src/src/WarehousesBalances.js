import React, { Component } from "react";
import { Table } from "antd";
import DebounceSelect from "./DebFetch";
import axios from "axios";

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
                title: 'Остаток',
                dataIndex: 'current_amount',
                key: 'current_amount',
            },
        ];

        this.state = {
            dataSource: null
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

    onWarehouseSelect = (warehouse) => {
        axios
            .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/alt_warehouse_balances/`,
                {
                    params: { token: this.props.token, warehouse_id: warehouse },
                }
            )
            .then((res) => {

                this.setState({
                    dataSource: res.data.result,
                });

            });
    }

    render() {
        return (
            <>
                <DebounceSelect
                    // mode="multiple"
                    style={{ width: "100%", marginBottom: 10 }}
                    placeholder="Введите имя склада"
                    fetchOptions={this.fetchWarehouse}
                    removeIcon={null}
                    onSelect={(user) => this.onWarehouseSelect(user)}

                />
                <Table dataSource={this.state.dataSource} columns={this.columns} />
            </>
        );
    }
}

export default WarehousesBalances;