import React, { useContext, useEffect, useRef, useState } from 'react';
import {
	Button,
	DatePicker,
	Form,
	Input,
	Popconfirm,
	Switch,
	// AutoComplete,
	message,
	Table,
	Space,
	Collapse,
	InputNumber,
	Select
} from 'antd';
import { DeleteOutlined, SearchOutlined } from '@ant-design/icons';


// import moment from 'moment';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import _ from "lodash";
import axios from 'axios';
import CreateLCard from './CreateLCard';
import DebounceSelectFil from './DebFetchFilters';

const { RangePicker } = DatePicker;
const { Panel } = Collapse;


dayjs.extend(utc)

const EditableContext = React.createContext(null);


const EditableRow = ({ index, ...props }) => {
	const [form] = Form.useForm();
	return (
		<Form form={form} component={false}>
			<EditableContext.Provider value={form}>
				<tr {...props} />
			</EditableContext.Provider>
		</Form>
	);
};

const EditableCell = ({
	title,
	editable,
	children,
	dataIndex,
	record,
	handleSave,
	...restProps
}) => {
	const [editing, setEditing] = useState(false);
	const inputRef = useRef(null);
	const form = useContext(EditableContext);
	useEffect(() => {
		if (editing) {
			inputRef.current.focus();
		}
	}, [editing]);

	const toggleEdit = () => {
		setEditing(!editing);
		form.setFieldsValue({
			[dataIndex]: record[dataIndex],
		});
	};

	const save = async () => {
		try {
			const values = await form.validateFields();
			toggleEdit();
			handleSave({ ...record, ...values });
		} catch (errInfo) {
			console.log('Save failed:', errInfo);
		}
	};


	let childNode = children;

	let ed = editing ? (
		<Form.Item
			style={{
				margin: 0,
			}}
			name={dataIndex}
			rules={[
				{
					required: true,
					message: `${title} обязательно для ввода.`,
				},
			]}
		>
			<Input ref={inputRef} onPressEnter={save} onBlur={save} />
		</Form.Item>
	) : (
		<div
			className="editable-cell-value-wrap"
			style={{
				paddingRight: 24,
			}}
			onClick={toggleEdit}
		>
			{children}
		</div>
	);

	if (editable) {
		if (record.type !== "transfer") {
			childNode = ed
		} else {
			if (dataIndex === "tax") {
				childNode = null;
			} else {
				childNode = ed
			}
		}
	}

	return <td {...restProps}>{childNode}</td>;
};

class LoyalityCards extends React.Component {
	constructor(props) {
		super(props);

		this.formRef = React.createRef();

		this.columns = [
			{
				title: 'Номер карты',
				dataIndex: 'card_number',
				// width: 230,
				key: 'card_number',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Баланс',
				key: 'balance',
				dataIndex: 'balance',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),

			},
			{
				title: 'Сумма входящих',
				dataIndex: 'income',
				// width: 230,
				key: 'income',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Сумма исходящих',
				key: 'outcome',
				dataIndex: 'outcome',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),

			},
			{
				title: 'Процент кэшбека',
				dataIndex: 'cashback_percent',
				// width: 230,
				key: 'cashback_percent',
				editable: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Минимальная сумма чека',
				key: 'minimal_checque_amount',
				dataIndex: 'minimal_checque_amount',
				editable: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),

			},
			{
				title: 'Начало действия карты',
				dataIndex: 'start_period',
				// width: 230,
				key: 'start_period',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (date, row) => {
					return (
						<DatePicker
							// presets={rangePresets}
							value={dayjs.unix(date)}
							onChange={(date) => this.handleChangeDateStart(dayjs(date).utc().unix(), row)}
							showTime
							allowClear={false}
							format="DD.MM.YY HH:mm:ss"
						// onChange={onRangeChange}
						/>
					)

				}
			},
			{
				title: 'Конец действия карты',
				key: 'end_period',
				dataIndex: 'end_period',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (date, row) => {
					return (
						<DatePicker
							// presets={rangePresets}
							value={dayjs.unix(date)}
							onChange={(date) => this.handleChangeDateEnd(dayjs(date).utc().unix(), row)}
							showTime
							allowClear={false}
							format="DD.MM.YY HH:mm:ss"
						// onChange={onRangeChange}
						/>
					)

				}

			},
			{
				title: 'Максимальный процент начисления',
				dataIndex: 'max_percentage',
				// width: 230,
				key: 'max_percentage',
				editable: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Максимальный процент списания',
				dataIndex: 'max_withdraw_percentage',
				// width: 230,
				key: 'max_withdraw_percentage',
				editable: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Клиент',
				key: 'contragent',
				dataIndex: 'contragent',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),

			},
			{
				title: 'Организация',
				dataIndex: 'organization',
				// width: 230,
				key: 'organization',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Статус',
				dataIndex: 'status_card',
				// width: 230,
				key: 'status_card',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (checked, row) => {
					return <Switch style={{ marginLeft: "13px" }} checked={checked} onClick={(checked) => this.handleChangeStatus(checked, row)} />
				}
			},
			{
				title: 'Действие',
				key: 'action',
				width: 160,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (_, record) => {
					return (this.state.dataSource.length >= 1 ? (
						<Popconfirm title="Подтвердите удаление"
							onConfirm={() => this.handleDelete(record.id)}
							cancelText="Отмена"
							okText="OK">
							<Button icon={<DeleteOutlined />} style={{ marginRight: 10 }} />
						</Popconfirm>

					) : "Загрузка...")
				}
			},
		];

		this.state = {
			currentPage: 1,
			count: 0,
			dataSource: [],
		};
	}

	edit_request = (id, payment, row) => {
		let edit_dict = {}

		for (let item in row) {
			if (row[item] !== payment[item]) {
				edit_dict[item] = row[item]
			}
		}

		if (Object.keys(edit_dict).length !== 0) {
			axios.patch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/${id}?token=${this.props.query.token}`, edit_dict)
			const newData = [...this.state.dataSource];
			const index = newData.findIndex((item) => item.id === id);

			if (index !== -1) {
				const item = newData[index];
				newData.splice(index, 1, { ...item, ...edit_dict });
				this.setState({ dataSource: newData });
			}
		} else {
			message.error(<>Вы не сделали никаких изменений!</>);
		}

	}

	delete_request = (row_id) => {
		axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/${row_id}?token=${this.props.query.token}`)
		const newData = [...this.state.dataSource];
		const index = newData.findIndex((item) => item.id === row_id);

		if (index !== -1) {
			newData.splice(index, 1);
			this.setState({ dataSource: newData });
		}

	}

	handleSave = (row) => {
		const newData = [...this.state.dataSource];
		const index = newData.findIndex((item) => row.id === item.id);
		const item = newData[index];

		newData.splice(index, 1, { ...item, ...row });

		this.setState({
			dataSource: newData,
		});

		const finded_row = newData.splice(index, 1, { ...item, ...row })[0];

		this.edit_request(finded_row.id, item,
			{
				cashback_percent: finded_row.cashback_percent,
				minimal_checque_amount: finded_row.minimal_checque_amount,
				max_percentage: parseInt(finded_row.max_percentage) <= 100 ? parseInt(finded_row.max_percentage) : 100,
				max_withdraw_percentage: parseInt(finded_row.max_withdraw_percentage) <= 100 ? parseInt(finded_row.max_withdraw_percentage) : 100,
			});

	};

	handleChangeDateStart = (date, row) => {
		this.edit_request(row.id, row, { start_period: date })
	}

	handleChangeDateEnd = (date, row) => {
		this.edit_request(row.id, row, { end_period: date })
	}

	handleChangeStatus = (date, row) => {
		this.edit_request(row.id, row, { status_card: date })
	}

	handleDelete = (row_id) => {
		this.delete_request(row_id)
	}

	handleChange = (pagination, filters, sorter) => {
		window.scrollTo({ top: 0, behavior: 'smooth' });

		if (sorter.hasOwnProperty("column")) {
			if (sorter.column) {
				let ord = "desc"
				if (sorter.order === "ascend") ord = "asc"
				const sort_param = `${sorter.field}:${ord}`
				this.setState({ currentPage: pagination.current, loading: true, sorted: true, sort_param: sort_param }, () => {
					this.fetch(pagination.current, this.state.filters, sort_param)
				})
			}
			else {
				this.setState({ currentPage: pagination.current, loading: true, sorted: false, sort_param: null }, () => {
					this.fetch(pagination.current, this.state.filters)
				})
			}
		}
		else {
			this.setState({ currentPage: pagination.current, loading: true, sorted: false, sort_param: null }, () => {
				this.fetch(pagination.current, this.state.filters)
			})
		}
	};


	fetch = (page, pars, sort = null) => {
		this.setState({ loading: true })
		const limit = 35
		const offset = (page * 35) - 35
		let filters = pars
		if (this.state.filtered) {
			filters = this.state.filters
		}

		let params = {
			params: {
				token: this.props.query.token,
				limit: limit,
				offset: offset,
				...filters,
			}
		}

		if (sort) {
			params.params.sort = sort
		}

		axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/`, params)
			.then(response => {

				this.setState({
					count: response.data.count,
					dataSource: response.data.result,
					loading: false
				})

				if (response.data.result && response.data.result.length > 0) {
					let res = response.data.result.map((payment) => { return { number: payment.card_number, id: payment.id } })
					let return_cards = res.map((value, i) => {
						return {
							label: value.number,
							value: value.id,
						}
					});
					this.setState({ cardsMeta: return_cards })
				}

			});

		const serialize = (obj) => {
			var str = [];
			for (var p in obj)
				if (obj.hasOwnProperty(p)) {
					str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
				}
			return str.join("&");
		}

		var newurl = window.location.protocol + "//" + window.location.host + window.location.pathname + "?" + serialize({ ...this.props.query, ...filters });
		window.history.pushState({ path: newurl }, "", newurl);
	}

	fetchCards = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/?token=${this.props.query.token}&card_number=${name}`)
				.then((response) => response.json())
				.then((body) => {
					return body
				})
				.then((body) => {
					if (body.result && body.result.length > 0) {
						let res = body.result.map((payment) => { return { number: payment.card_number, id: payment.id } })
						let return_cards = res.map((value, i) => {
							return {
								label: value.number,
								value: value.id,
							}
						});
						this.setState({ cardsMeta: return_cards })
						return return_cards
					}
				})
				.then((body) => {
					return body
				})
		}
		else {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards?token=${this.props.query.token}`)
				.then((response) => response.json())
				.then((body) => {
					return body
				})
				.then((body) => {
					if (body.result && body.result.length > 0) {
						let res = body.result.map((payment) => { return { number: payment.card_number, id: payment.id } })
						let return_cards = res.map((value, i) => {
							return {
								label: value.number,
								value: value.id,
							}
						});
						this.setState({ cardsMeta: return_cards })
						return return_cards
					}
				})
				.then((body) => {
					return body
				})
		}
	}

	fetchContr = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${this.props.query.token}&name=${name}`)
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
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents?token=${this.props.query.token}`)
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

	fetchManager = async (name) => {

		return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/cashbox_users?token=${this.props.query.token}`)
			.then((response) => response.json())
			.then((body) => {
				return body
			})
			.then((body) => {
				if (body.result) {
					return body.result.map((payment) => ({
						label: `${payment.first_name} ${payment.last_name}`,
						value: payment.id,
					}))

				}
			})
			.then((body) => {
				return body
			})

	}

	filterHandler = async (values) => {
		// console.log(values)
		Object.keys(values).forEach(key => (values[key] === undefined || values[key] === null || values[key].length === 0) && delete values[key])

		if ("contragent_name" in values) {
			values['contragent_name'] = values['contragent_name'].label
		}

		if ("card_number" in values) {
			values['card_number'] = values['card_number'].label
		}

		if ("created_by_id" in values) {
			values['created_by_id'] = values['created_by_id'].value
		}

		if ("start_period" in values) {
			values['start_period_from'] = values['start_period'][0].unix()
			values['start_period_to'] = values['start_period'][1].unix()
			delete values['start_period']
		}

		if ("end_period" in values) {
			values['end_period_from'] = values['end_period'][0].unix()
			values['end_period_to'] = values['end_period'][1].unix()
			delete values['end_period']
		}

		this.setState({ filters: values })
		this.fetch(1, values, this.state.sorted ? this.state.sort_param : null)
	}

	componentDidMount() {
		// this.state.filtered ? this.fetch(1, this.state.filters) : this.fetch(1)
		this.fetch(1)
		const { websocket } = this.props;

		websocket.onmessage = message => {
			const data = JSON.parse(message.data)

			if (data.target === "loyality_cards") {
				if (data.action === "create") {
					if (this.state.currentPage === 1) {
						const DS = [...this.state.dataSource];
						let C = this.state.count;

						for (let index = 0; index < data.result.length; index++) {
							C += 1

							const finded = DS.find((item) => item.id === data.result[index].id);

							if (finded) {
								continue
							}
							else {
								if (DS.length <= 34) {
									DS.unshift(data.result[index]);
								}
								else {
									DS.pop()
									DS.unshift(data.result[index]);
								}
							}
						}


						this.setState({ dataSource: DS, count: C })
					}

				}
				if (data.action === "edit") {

					const newData = [...this.state.dataSource];
					const index = newData.findIndex((item) => data.result.id === item.id);

					if (index !== -1) {
						const item = newData[index];
						newData.splice(index, 1, { ...item, ...data.result });
						this.setState({ dataSource: newData });
					}
				}

				if (data.action === "delete") {

					const newData = [...this.state.dataSource];
					const index = newData.findIndex((item) => data.result === item.id);

					if (index !== -1) {
						newData.splice(index, 1);
						this.setState({ dataSource: newData });
					}
				}
			}
		}
	}

	render() {
		const { dataSource } = this.state;
		const components = {
			body: {
				row: EditableRow,
				cell: EditableCell,
			},
		};
		const columns = this.columns.map((col) => {
			if (!col.editable) {
				return col;
			}

			return {
				...col,
				onCell: (record) => ({
					record,
					editable: col.editable,
					dataIndex: col.dataIndex,
					title: col.title,
					handleSave: this.handleSave,
				}),
			};
		});

		return (

			<div>

				<CreateLCard
					token={this.props.query.token}
					organisations={this.props.orgsData}
				/>

				<Space direction='horizontal'>
					<Collapse
						onChange={(arr) => {
							if (arr.length === 0) { this.setState({ filterCollapsed: false }) }
							else this.setState({ filterCollapsed: true })
						}
						}
						ghost
						style={{ marginBottom: 15, marginTop: -5 }}>
						<Panel header="Фильтры поиска платежей..." key="1">


							<Form
								ref={this.formRef}
								layout={"inline"}
								style={{ marginBottom: 10 }}
								onFinish={this.filterHandler}
							>

								<Form.Item
									name="card_number"
								>
									{/* <InputNumber
										// mode="multiple"
										placeholder="Введите номер карты"
										style={{
											width: 250, marginTop: 10
										}}
									/> */}
									<Select
										style={{ width: 250, marginTop: 10 }}
										options={this.state.cardsMeta}
										placeholder="Введите номер карты"
										allowClear={true}
										showSearch
										labelInValue
										filterOption={false}
										onSearch={this.fetchCards}
									/>
								</Form.Item>

								<Form.Item
									name="balance"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите баланс"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="income"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите входящие"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="outcome"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите исходящие"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="cashback_percent"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите процент кешбека"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="minimal_checque_amount"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите минимальную сумму чека"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="start_period"
								>
									<RangePicker style={{ width: 300, marginTop: 10 }}
										placeholder={['Начальная дата', 'Конечная дата']}
										allowEmpty={[false, false]}
										showTime
										allowClear={true}
										format="DD.MM.YY HH:mm:ss"
									/>
								</Form.Item>

								<Form.Item
									name="end_period"
								>
									<RangePicker style={{ width: 300, marginTop: 10 }}
										placeholder={['Начальная дата', 'Конечная дата']}
										allowEmpty={[false, false]}
										showTime
										allowClear={true}
										format="DD.MM.YY HH:mm:ss"
									/>
								</Form.Item>

								<Form.Item
									name="max_percentage"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите максимальный процент"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="contragent_name"
								>
									{/* <Input
										// mode="multiple"
										placeholder="Введите имя контрагента"
										style={{
											width: 250, marginTop: 10
										}}
									/> */}
									<DebounceSelectFil
										// mode="multiple"
										placeholder="Введите имя контрагента"
										fetchOptions={this.fetchContr}
										removeIcon={null}
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="phone_number"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите телефон контрагента"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="organization_name"
								>
									<Input
										// mode="multiple"
										placeholder="Введите название организации"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>


								<Form.Item
									name="created_by_id"
								>
									<DebounceSelectFil
										// mode="multiple"
										placeholder="Введите имя менеджера"
										fetchOptions={this.fetchManager}
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
				</Space>

				<Table
					components={components}
					loading={this.state.loading}
					rowClassName={record => record.is_deleted && "disabled-row"}
					rowKey={record => record.id}
					dataSource={dataSource}
					onChange={this.handleChange}
					columns={columns}
					pagination={
						{
							total: this.state.count,
							pageSize: 35,
							showSizeChanger: false
						}
					}

				/>
			</div>
		);
	}
}

export default LoyalityCards;