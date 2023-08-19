import React, { useState } from 'react'

import DebounceSelectFil from '../../DebFetchFilters';
import { SearchOutlined } from '@ant-design/icons';
import {
	Button,
	Form,
	Select,
	Collapse,
    DatePicker,

} from 'antd';

const { Option } = Select;
const { RangePicker } = DatePicker;
const { Panel } = Collapse;

const Filter = ({query, onChange}) => {
    const [state] = useState({
        relship:"all",
        payment_type:"all",
        name: '',
        tags:'',
        datefrom: null,
        dateto:null,
        project:'',
        paybox:'',
        contragent:'',
    })
    const [openFilter, setOpenFilter] = useState(false)
    const formRef = React.createRef();

    const fetchNames = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments?token=${query.token}&name=${name}`)
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
				})
				.then((body) => {
					return body
				})
		}
		else {

            if(openFilter){
                return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments?token=${query.token}`)
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
                    })
                    .then((body) => {
                        return body
                    })
            }
		}
	}

    const fetchProjects = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${query.token}&name=${name}`)
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

            if(openFilter){
                return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${query.token}`)
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
	}

	const fetchPaybox = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${query.token}&name=${name}`)
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
            if(openFilter){
                return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${query.token}`)
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
	}

	const fetchContr = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${query.token}&name=${name}`)
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
            if(openFilter){
                return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${query.token}`)
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
				})
				.then((body) => {
					return body
				})
            }
			
		}
	}

    const onFinish = (values) => {
		Object.keys(values).forEach(key => (values[key] === undefined || values[key] === null || values[key].length === 0) && delete values[key])
		Object.keys(values).forEach(key => {
			if (key === "name" || key === "project" || key === "paybox" || key === "contragent") {
				values[key] = values[key]['label']
			}
		})
		if ("dates" in values) {
			values['datefrom'] = values['dates'][0].format("DD-MM-YYYY")
			values['dateto'] = values['dates'][1].format("DD-MM-YYYY")
			delete values['dates']
		}

		if ("tags" in values) {
			values['tags'] = values.tags.map((item) => item.label).join(",")
		}

		if (values.payment_type === "all") {
			delete values['payment_type']
		}

        if(openFilter){
            onChange(1, values)
        }
		
    };


    return (
        <Collapse
        onChange={() => setOpenFilter(!openFilter)}
        ghost
        style={{ marginBottom: 15, marginTop: -5 }}>
        <Panel header="Фильтры поиска платежей..." key="1">


            <Form
                ref={formRef}
                layout={"inline"}
                style={{ marginBottom: 10 }}
                onFinish={onFinish}
            >

                <Form.Item
                    name="relship"
                    initialValue={state.relship}
                >
                    <Select style={{ width: 150, marginTop: 10 }}>
                        <Option value="all">Все платежи</Option>
                        <Option value="parents">Родительские</Option>
                        <Option value="childs">Дочерние</Option>
                    </Select>
                </Form.Item>

                <Form.Item
                    name="payment_type"
                    initialValue={state.payment_type}
                >
                    <Select style={{ width: 200, marginTop: 10 }}>
                        <Option value="all">Все типы платежей</Option>
                        <Option value="incoming">Приход</Option>
                        <Option value="outgoing">Расход</Option>
                        <Option value="transfer">Перевод</Option>
                    </Select>
                </Form.Item>

                <Form.Item
                    name="name"
                    initialValue={state.name}
                >
                    <DebounceSelectFil
                        placeholder="Введите название платежа"
                        fetchOptions={fetchNames}
                        removeIcon={null}
                        style={{
                            width: 250, marginTop: 10
                        }}
                    />
                </Form.Item>
                <Form.Item
                    name="dates"
                >
                    <RangePicker style={{ width: 300, marginTop: 10 }}
                        placeholder={['Начальная дата', 'Конечная дата']}
                        allowEmpty={[false, false]}
                        initialValue={[
                            state.datefrom,
                            state.dateto
                        ]}
                        format={"DD-MM-YYYY"}
                    />

                </Form.Item>

                <Form.Item
                    name="project"
                    initialValue={state.project}
                >

                    <DebounceSelectFil
                        placeholder="Введите название проекта"
                        fetchOptions={fetchProjects}
                        removeIcon={null}
                        style={{
                            width: 250, marginTop: 10
                        }}
                    />
                </Form.Item>

                <Form.Item
                    name="paybox"
                    initialValue={state.paybox}
                >
                    <DebounceSelectFil
                        placeholder="Название счета"
                        fetchOptions={fetchPaybox}
                        removeIcon={null}
                        style={{
                            width: 250, marginTop: 10
                        }}
                    />
                </Form.Item>

                <Form.Item
                    name="contragent"
                    initialValue={state.contragent}
                >

                    <DebounceSelectFil
                        placeholder="Введите контрагента"
                        fetchOptions={fetchContr}
                        removeIcon={null}
                        style={{
                            width: 250, marginTop: 10
                        }}
                    />
                </Form.Item>

                <Button style={{ marginTop: 10 }} htmlType="submit" icon={<SearchOutlined />}>Поиск</Button>

            </Form>
        </Panel>
    </Collapse>
    )
}

export default Filter