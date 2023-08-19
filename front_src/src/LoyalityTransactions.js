import React, { useContext, useEffect, useRef, useState } from 'react';
import {
	Button,
	DatePicker,
	InputNumber,
	Form,
	Input,
	Popconfirm,
	Switch,
	Tag,
	message,
	Space,
	Table,
	Collapse,
	Select
} from 'antd';
import { DeleteOutlined, SearchOutlined } from '@ant-design/icons';


// import moment from 'moment';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import _ from "lodash";
import axios from 'axios';
import EditBonusTransaction from './EditBonusTransaction';
import CreateBonusTransaction from './CreateBonusTransaction';

const { Panel } = Collapse;
const { RangePicker } = DatePicker;

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

class LoyalityTransactions extends React.Component {
	constructor(props) {
		super(props);

		this.formRef = React.createRef();

		this.columns = [
			{
				title: 'Название операции',
				dataIndex: 'name',
				// width: 230,
				key: 'name',
				editable: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Описание',
				dataIndex: 'description',
				// width: 230,
				key: 'description',
				editable: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Номер карты',
				dataIndex: 'loyality_card_number',
				// width: 230,
				key: 'loyality_card_number',
				editable: false,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
			},
			{
				title: 'Теги',
				key: 'tags',
				dataIndex: 'tags',
				editable: false,
				// width: 250,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (tags) => {
					if (tags) {
						return <>
							{tags.split(',').map(tag => {
								return (
									<Tag color='green' key={tag}>
										{tag.toUpperCase()}
									</Tag>
								);
							})}
						</>
					} else return "Не указаны"
				},
			},
			{
				title: 'Сумма',
				dataIndex: 'amount',
				key: 'amount',
				editable: true,
				// width: 130,
				width: 50,
				sorter: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (text, row) => {
					if (row.type === "accrual") {
						return <font color="green">+{text}</font>
					} else {
						return <font color="red">-{text}</font>
					}
				},
				// sorter: (a, b) => a.amount - b.amount,
			},
			{
				title: 'Дата операции',
				dataIndex: 'dated',
				key: 'dated',
				width: 150,
				sorter: true,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (date, row) => {
					return <DatePicker format={`DD.MM.YYYY`}
						allowClear={false}
						value={dayjs.unix(date)}
						onChange={(date) => this.handleChangeDate(dayjs(date).utc().unix(), row)}
					// onChange={(date, dateString) => this.handleChangeDate(date.utc().startOf("day").unix(), row)}
					// console.log(date.utc().startOf("day").unix())
					/>
				},
				// sorter: (a, b) => new Date(a.date) - new Date(b.date),
			},
			{
				title: 'Тип операции',
				dataIndex: 'type',
				key: 'type',
				// width: 110,
				sortDirections: ["ascend", "descend"],
				width: 5,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				// sorter: (record) => {
				// 	return record.type.localeCompare(["incoming", "outgoing"])
				// },
				sorter: true,
				render: (paymentType, row) => {
					if (paymentType === 'accrual') {
						return <font color="green">Пополнение</font>
					}
					if (paymentType === 'withdraw') {
						return <font color="red">Вывод</font>
					}
				}
			},
			{
				title: 'Статус операции',
				dataIndex: 'status',
				key: 'status',
				// width: 100,
				width: 5,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				// sorter: (a, b) => a.status - b.status,
				sorter: true,
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
						<>
							<Popconfirm title="Подтвердите удаление"
								onConfirm={() => this.handleDelete(record.id)}
								cancelText="Отмена"
								okText="OK">
								<Button icon={<DeleteOutlined />} style={{ marginRight: 10 }} />
							</Popconfirm>
							<EditBonusTransaction
								payment={record}
								cardsData={this.props.loyalityCardsData}
								token={this.props.query.token}
							// payboxes={this.props.PBData}
							// payboxesList={this.props.PBData}

							// // caData={this.props.caSel}
							// caMeta={this.props.caData}

							// name_meta={this.props.paymentsMeta.names}
							// tags_meta={this.props.paymentsMeta.tags}
							// art_meta={this.props.paymentsMeta.articles}
							// projects_select={this.props.PRData}
							/>
						</>

					) : "Загрузка...")
				}
			},
		];

		this.state = {
			count: this.props.loyalityTransactionsData.c,
			dataSource: this.props.loyalityTransactionsData.ds,
			currentRel: "all",
			currentPage: 1,
			filtered: false,
			filters: {},
			loading: true,
			filterCollapsed: false,
			sorted: false,
			sort_param: null,
		};
	}

	randomInteger = (min, max) => {
		return Math.floor(Math.random() * (max - min + 1)) + min;
	}

	fetchTags = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/?token=${this.props.query.token}&tags=${name}`)
				.then((response) => response.json())
				.then((body) => {
					return body
				})
				.then((body) => {
					if (body.result && body.result.length > 0) {
						let res = body.result.map((payment) => payment.tags.split(",")).flat(1)
						let tags = res.filter((item) => item.toUpperCase().indexOf(name.toUpperCase()) !== -1)
						let return_tags = tags.map((value, i) => {
							return {
								label: value,
								value: this.randomInteger(10000, 20000),
							}
						});
						// console.log(return_tags)
						this.setState({ tagsMeta: return_tags })
						return return_tags
					}
				})
				.then((body) => {
					return body
				})
		}
	}

	filterHandler = async (values) => {
		Object.keys(values).forEach(key => (values[key] === undefined || values[key] === null || values[key].length === 0) && delete values[key])


		if ("dated" in values) {
			values['dated_from'] = values['dated'][0].unix()
			values['dated_to'] = values['dated'][1].unix()
			delete values['dated']
		}

		if ("loyality_card_number" in values) {
			values['loyality_card_number'] = values['loyality_card_number'].label
		}

		if ("tags" in values) {
			values['tags'] = values.tags.map((item) => item.label ? item.label : item.value).join(",")
		}

		if (values.type === "all") {
			delete values['type']
		}

		this.setState({ filters: values })
		this.fetch(1, values, this.state.sorted ? this.state.sort_param : null)
	}

	handleDelete = (id) => {
		const dataSource = [...this.state.dataSource];
		const row_index = dataSource.findIndex((item) => item.id === id)
		const row = dataSource[row_index]
		dataSource.splice(row_index, 1);

		this.setState({
			dataSource: dataSource,
		});

		axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/${row.id}`, { params: { token: this.props.query.token } })
			.then(response => {
				message.success("Вы успешно удалили платеж");
			});
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


	componentDidMount() {
		// this.state.filtered ? this.fetch(1, this.state.filters) : this.fetch(1)
		this.fetch(1)
		const { websocket } = this.props;

		websocket.onmessage = message => {
			const data = JSON.parse(message.data)

			if (data.target === "loyality_transactions") {
				if (data.action === "create") {
					if (this.state.currentPage === 1) {
						const DS = [...this.state.dataSource];
						const C = this.state.count;

						if (DS.length <= 34) {
							DS.unshift(data.result);
						}
						else {
							DS.pop()
							DS.unshift(data.result);
						}
						this.setState({ dataSource: DS, count: C + 1 })
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

	handleChangeDate = (date, row) => {
		this.edit_request(row.id, row, { dated: date })
	}

	handleChangeStatus = (status, row) => {
		this.edit_request(row.id, row, { status: status })
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

		if (finded_row.tax === 0 && parseFloat(item['amount']) !== parseFloat(finded_row.amount)) {
			this.edit_request(finded_row.id, item, {
				description: finded_row.description,
				amount: finded_row.amount,
				name: finded_row.name
			});
		} else {
			this.edit_request(finded_row.id, item, { description: finded_row.description, amount: finded_row.amount, name: finded_row.name });
		}

	};

	edit_request = (id, payment, row) => {
		let edit_dict = {}
		for (let item in row) {
			if (row[item] !== payment[item]) {
				edit_dict[item] = row[item]
			}
		}

		if (Object.keys(edit_dict).length !== 0) {
			axios.patch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/${id}?token=${this.props.query.token}`, edit_dict)
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

		fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/?token=${this.props.query.token}`)
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

		axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/`, params)
			.then(response => {
				if (response.data.result && response.data.result.length > 0) {

					let res = response.data.result.map((payment) => payment.tags.split(",")).flat(1)
					let return_tags = res.filter((item) => {
						if (item) {
							return true;
						}
						return false;
					}).map((value, i) => {
						return {
							label: value,
							value: `${this.randomInteger(10000, 20000)}`,
						}

					});

					this.setState({ tagsMeta: return_tags })

				}

				this.setState({
					count: response.data.count,
					dataSource: response.data.result,
					loading: false,
				})

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
				<Space direction='horizontal'>
					<CreateBonusTransaction
						cardsData={this.props.loyalityCardsData}
						token={this.props.query.token}
						tagsMeta={this.state.tagsMeta}
						cardsMeta={this.state.cardsMeta}
					/>
				</Space>

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
									name="type"
								>
									<Select
										options={[
											{
												value: 'all',
												label: 'Все',
											},
											{
												value: 'withdraw',
												label: 'Вывод',
											},
											{
												value: 'accrual',
												label: 'Пополнение',
											},
										]}
										// mode="multiple"
										placeholder="Выберите тип"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="amount"
								>
									<InputNumber
										// mode="multiple"
										placeholder="Введите сумму"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="loyality_card_number"
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
									name="tags"
								>
									{/* <Select
										mode="tags"
										placeholder="Введите тэги"
										style={{
											width: 250, marginTop: 10
										}}
									/> */}
									<Select
										style={{ width: 250, marginTop: 10 }}
										options={this.state.tagsMeta}
										placeholder="Введите теги"
										allowClear={true}
										showSearch
										labelInValue
										filterOption={false}
										mode={"tags"}
										onSearch={this.fetchTags}
									/>
								</Form.Item>

								<Form.Item
									name="name"
								>
									<Input
										// mode="multiple"
										placeholder="Введите название транзакции"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="description"
								>
									<Input
										// mode="multiple"
										placeholder="Введите описание транзакции"
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="dated"
								>
									<RangePicker style={{ width: 300, marginTop: 10 }}
										placeholder={['Начальная дата', 'Конечная дата']}
										allowEmpty={[false, false]}
										allowClear={true}
										format="DD.MM.YY"
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

export default LoyalityTransactions;