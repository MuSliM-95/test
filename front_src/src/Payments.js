import React, { useContext, useEffect, useRef, useState } from 'react';
import {
	Button,
	DatePicker,
	Form,
	Input,
	Popconfirm,
	Popover,
	Switch,
	Tag,
	Select,
	// AutoComplete,
	message,
	Collapse,
	Space,
	Table
} from 'antd';
import { DeleteOutlined, FrownOutlined, RedoOutlined, SearchOutlined } from '@ant-design/icons';

import EditPayment from './EditPayment';

// import moment from 'moment';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import _ from "lodash";
import axios from 'axios';
import CreatePayment from './CreatePayment';
import Raspil from './Raspil';
import RaspilView from './RaspilView';
// import VirtualTable from './VirtualTable';
// import DebounceSelect from './DebFetch';
import DebounceSelectFil from './DebFetchFilters';


dayjs.extend(utc)
const { Option } = Select;
const { RangePicker } = DatePicker;
const { Panel } = Collapse;

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

class EditableTable extends React.Component {
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
				title: 'Теги',
				key: 'tags',
				dataIndex: 'tags',
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
					if (row.type === "incoming") {
						return <font color="green">+{text}</font>
					} else {
						return <font color="red">-{text}</font>
					}
				},
				// sorter: (a, b) => a.amount - b.amount,
			},
			{
				title: 'Дата операции',
				dataIndex: 'date',
				key: 'date',
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
					if (paymentType === 'incoming') {
						var content = null
						var pbox_from_name = null
						for (var i in this.props.PBData) {
							if (this.props.PBData[i].key === row.paybox.toString()) {
								pbox_from_name = this.props.PBData[i].props.children[0]
							}
						}
						content = <>Зачисление на счет <b>{pbox_from_name}</b> <br />На сумму: <b>{row.amount} руб.</b></>
						return <Popover title={"Счет зачисления"} content={content}>
							<font color="green">Приход</font>
						</Popover>
					}
					if (paymentType === 'outgoing') {
						content = null
						pbox_from_name = null
						for (var j in this.props.PBData) {
							if (this.props.PBData[j].key === row.paybox.toString()) {
								pbox_from_name = this.props.PBData[j].props.children[0]
							}
						}
						content = <>Списание с счета <b>{pbox_from_name}</b> <br />На сумму: <b>{row.amount} руб.</b></>
						return <Popover title={"Счет списания"} content={content}>
							<font color="red">Расход</font>
						</Popover>
					} else {
						var pbox_to_name = null
						pbox_from_name = null
						content = null
						for (var k in this.props.PBData) {

							if (this.props.PBData[k].key === row.paybox_to.toString()) {
								pbox_to_name = this.props.PBData[k].props.children[0]
							}

							if (this.props.PBData[k].key === row.paybox.toString()) {
								pbox_from_name = this.props.PBData[k].props.children[0]
							}
						}
						if (pbox_from_name && pbox_to_name) {
							content = <>Перевод со счета <b>{pbox_from_name}</b> на счет <b>{pbox_to_name}</b><br />На
								сумму: <b>{row.amount} руб.</b></>
						}
						return <Popover title={"Перевод со счета на счет"} content={content}>
							<font color="blue">Перевод</font>
						</Popover>;
					}
				}
			},
			{
				title: 'Повтор операции',
				dataIndex: 'repeat_freq',
				key: 'repeat_freq',
				// width: 100,
				width: 5,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: () => {

					return (
						<Popover content={<>Повторы в разработке <FrownOutlined /></>}>
							<Button style={{ marginLeft: "13px" }} disabled shape="circle"
								icon={<RedoOutlined />}
							/>
						</Popover>
					)

				},
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
				title: 'Кред./Деб.',
				dataIndex: 'deb_cred',
				key: 'deb_cred',
				// width: 110,
				width: 5,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				// sorter: (a, b) => a.status - b.status,
				sorter: true,
				render: (checked, row) => {
					if (row.status === false && new Date(row.date) >= new Date() && row.type !== "transfer") {
						return (
							<Popover content={() => {
								if (row.type === "incoming") {
									return "Дебиторка"
								} else return "Кредиторка"
							}}>
								<Switch checked={checked} onClick={(checked) => this.handleChangeDebCred(checked, row)} />
							</Popover>
						)
					}
				}
			},
			{
				title: 'Контрагент',
				dataIndex: 'contragent',
				key: 'contragent',
				// width: 100,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				render: (contragent, row) => {
					if (contragent) {
						return row.contragent_name
					} else {
						return "Не указан"
					}
				}
			},
			{
				title: 'Налог',
				dataIndex: 'tax',
				key: 'tax',
				editable: true,
				// width: 85,
				width: 120,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				// sorter: (a, b) => a.tax - b.tax,
				sorter: true,
				render: (tax, row) => {
					let data = null;
					if (row.type !== "transfer") {
						if (tax === null) {
							data = '0 %'
						} else {
							data = tax + ' %'
						}
					}
					return <span>{data}</span>
				}
			},
			{
				title: 'Тип налога',
				dataIndex: 'tax_type',
				key: 'tax_type',
				// width: 150,
				width: 5,
				shouldCellUpdate: (record, prevRecord) => !_.isEqual(record, prevRecord),
				sorter: true,
				// sorter: (record) => {
				// 	if (record.tax_type) {
				// 		return record.tax_type.localeCompare(["internal", "external"])
				// 	}
				// },
				render: (tax_type, row) => {
					if (row.type !== "transfer") {
						if (tax_type === 'internal') {
							return <font color="red">Внутренний</font>
						} else {
							return <font color="green">Внешний</font>
						}
					} else {
						return <font color="blue">Перевод</font>
					}
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
							<EditPayment
								payment={record}
								token={this.props.query.token}
								payboxes={this.props.PBData}
								payboxesList={this.props.PBData}

								// caData={this.props.caSel}
								caMeta={this.props.caData}

								name_meta={this.props.paymentsMeta.names}
								tags_meta={this.props.paymentsMeta.tags}
								art_meta={this.props.paymentsMeta.articles}
								projects_select={this.props.PRData}
							/>
							{(record.raspilen || record.parent_id) ?
								<RaspilView token={this.props.query.token} payment={record} projects_select={this.props.PRData}
									caData={this.props.caSel} /> :
								<Raspil token={this.props.query.token} payment={record} projects_select={this.props.PRData}
									caData={this.props.caSel} />}
						</>

					) : "Загрузка...")
				}
			},
		];

		this.state = {
			count: 0,
			dataSource: [],
			currentRel: "all",
			currentPage: 1,
			filtered: false,
			filters: {},
			loading: true,
			filterCollapsed: false,
			sorted: false,
			sort_param: null,
			tagsMeta: this.props.paymentsMeta.tags
		};
	}

	randomInteger = (min, max) => {
		return Math.floor(Math.random() * (max - min + 1)) + min;
	}

	fetchTags = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments_meta?token=${this.props.query.token}&tags=${name}`)
				.then((response) => response.json())
				.then((body) => {
					return body
				})
				.then((body) => {
					if (body) {
						let res = body.map((payment) => payment.tags.split(",")).flat(1)
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

	handleDelete = (id) => {
		const dataSource = [...this.state.dataSource];
		const row_index = dataSource.findIndex((item) => item.id === id)
		const row = dataSource[row_index]
		dataSource.splice(row_index, 1);

		this.setState({
			dataSource: dataSource,
		});

		axios.delete(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments/${row.id}`, { params: { token: this.props.query.token } })
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
		this.state.filtered ? this.fetch(1, this.state.filters) : this.fetch(1)
		const { websocket } = this.props;

		websocket.onmessage = message => {
			const data = JSON.parse(message.data)

			if (data.target === "payments") {
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

	fetchNames = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments?token=${this.props.query.token}&name=${name}`)
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
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments?token=${this.props.query.token}`)
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

	fetchProjects = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${this.props.query.token}&name=${name}`)
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
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects?token=${this.props.query.token}`)
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

	fetchPaybox = async (name) => {
		if (name) {
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${this.props.query.token}&name=${name}`)
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
			return fetch(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes?token=${this.props.query.token}`)
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

	handleChangeDate = (date, row) => {
		this.edit_request(row.id, row, { date: date })
	}

	handleChangeStatus = (status, row) => {
		this.edit_request(row.id, row, { status: status })
	}

	handleChangeDebCred = (status, row) => {
		this.edit_request(row.id, row, { deb_cred: status })
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
				amount_without_tax: finded_row.amount,
				amount: finded_row.amount,
				tax: finded_row.tax
			});
		} else {
			this.edit_request(finded_row.id, item, { amount: finded_row.amount, tax: finded_row.tax, name: finded_row.name });
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
			axios.put(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments/${id}?token=${this.props.query.token}`, edit_dict)
		} else {
			message.error(<>Вы не сделали никаких изменений!</>);
		}

	}

	filterHandler = async (values) => {
		// console.log(values)
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

		// console.log(values)

		if ("tags" in values) {
			values['tags'] = values.tags.map((item) => item.label).join(",")
		}

		if (values.payment_type === "all") {
			delete values['payment_type']
		}

		// console.log(values)

		this.setState({ filters: values })
		this.fetch(1, values, this.state.sorted ? this.state.sort_param : null)
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

		axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments`, params)
			.then(response => {

				this.setState({
					count: response.data.count,
					dataSource: response.data.result,
					loading: false
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

	render() {
		const { dataSource, tagsMeta } = this.state;
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
					<CreatePayment
						token={this.props.query.token}
						payboxes={this.props.PBData}

						caData={this.props.caSel}
						// caMeta={this.props.caData}

						name_meta={this.props.paymentsMeta.names}
						tags_meta={this.props.paymentsMeta.tags}
						art_meta={this.props.paymentsMeta.articles}
						// projects={this.props.projects}
						projects_select={this.props.PRData}
					/>
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
									name="relship"
									initialValue={this.props.query.relship ? this.props.query.relship : "all"}
								>
									<Select style={{ width: 150, marginTop: 10 }}>
										<Option value="all">Все платежи</Option>
										<Option value="parents">Родительские</Option>
										<Option value="childs">Дочерние</Option>
									</Select>
								</Form.Item>

								<Form.Item
									name="payment_type"
									initialValue={this.props.query.payment_type ? this.props.query.payment_type : "all"}
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
									initialValue={this.props.query.name}
								>
									{/* <AutoComplete
										style={{ width: 200, marginTop: 10 }}
										options={this.props.paymentsMeta.names}
										placeholder="Введите название платежа"
										allowClear={true}
										filterOption={(inputValue, option) =>
											option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
									/> */}

									<DebounceSelectFil
										// mode="multiple"
										placeholder="Введите название платежа"
										fetchOptions={this.fetchNames}
										removeIcon={null}
										// start_options={this.props.paymentsMeta.names}
										// onChange={(newValue) => {
										// 	if (newValue.length <= 1) {
										// 		this.formRef.current.setFieldsValue({
										// 			name: newValue,
										// 		})
										// 	}
										// 	else {
										// 		this.formRef.current.setFieldsValue({
										// 			name: newValue.slice(-1),
										// 		})
										// 	}
										// }}
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="tags"
									initialValue={this.props.query.tags ? this.props.query.tags.split(",") : []}
								>
									{/* <Select mode="tags"
										style={{ width: 250, marginTop: 10 }}
										placeholder="Введите тег"
										allowClear={true}
									>
										{this.props.paymentsMeta.tags}
									</Select> */}
									<Select
										style={{ width: 250, marginTop: 10 }}
										options={tagsMeta}
										placeholder="Введите теги"
										allowClear={true}
										showSearch
										labelInValue
										filterOption={false}
										mode={"tags"}
										onSearch={this.fetchTags}
									/>
									{/* <DebounceSelect
										// mode="multiple"
										placeholder="Введите тег"
										fetchOptions={this.fetchTags}
										removeIcon={null}
										style={{
											width: 250, marginTop: 10
										}}
									/> */}
								</Form.Item>

								<Form.Item
									name="dates"
								>
									<RangePicker style={{ width: 300, marginTop: 10 }}
										placeholder={['Начальная дата', 'Конечная дата']}
										allowEmpty={[false, false]}
										initialValue={[
											this.props.query.datefrom ? dayjs(this.props.query.datefrom) : false,
											this.props.query.dateto ? dayjs(this.props.query.dateto) : false
										]}
										format={"DD-MM-YYYY"}
									/>

								</Form.Item>

								<Form.Item
									name="project"
									initialValue={this.props.query.project}
								>
									{/* <AutoComplete
										style={{ width: 200, marginTop: 10 }}
										options={this.props.projMeta}
										placeholder="Введите название проекта"
										allowClear={true}
										filterOption={(inputValue, option) =>
											option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
									/> */}
									<DebounceSelectFil
										// mode="multiple"
										placeholder="Введите название проекта"
										fetchOptions={this.fetchProjects}
										removeIcon={null}
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="paybox"
									initialValue={this.props.query.paybox}
								>
									{/* <AutoComplete
										style={{ width: 200, marginTop: 10 }}
										options={this.props.payboxesMeta}
										placeholder="Название счета списания"
										allowClear={true}
										filterOption={(inputValue, option) =>
											option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
									/> */}
									<DebounceSelectFil
										// mode="multiple"
										placeholder="Название счета"
										fetchOptions={this.fetchPaybox}
										removeIcon={null}
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Form.Item
									name="contragent"
									initialValue={this.props.query.contragent}
								>
									{/* <AutoComplete
										style={{ width: 200, marginTop: 10 }}
										options={this.props.caMeta.names}
										placeholder="Введите контрагента"
										allowClear={true}
										filterOption={(inputValue, option) =>
											option.value.toUpperCase().indexOf(inputValue.toUpperCase()) !== -1}
									/> */}
									<DebounceSelectFil
										// mode="multiple"
										placeholder="Введите контрагента"
										fetchOptions={this.fetchContr}
										removeIcon={null}
										style={{
											width: 250, marginTop: 10
										}}
									/>
								</Form.Item>

								<Button style={{ marginTop: 10 }} htmlType="submit" icon={<SearchOutlined />}>Поиск</Button>

							</Form>
							{/* <div style={{ display: 'inline-block', marginLeft: 15 }}>
                        </div> */}
						</Panel>
					</Collapse>
				</Space>


				<Table
					components={components}
					loading={this.state.loading}
					rowClassName={record => record.is_deleted && "disabled-row"}
					rowKey={record => record.id}
					// bordered
					// scroll={{
					// 	y: this.state.filterCollapsed ? 450 : 580,
					// 	x: '85vw',
					// }}
					dataSource={dataSource}
					onChange={this.handleChange}
					columns={columns}
					pagination={
						{
							total: this.state.count,
							// onChange: page => {

							// 	this.setState({ currentPage: page, loading: true }, () => {
							// 		if (this.state.currentRel === "all") this.fetch(page, this.state.filters)
							// 		if (this.state.currentRel === "parents") this.fetch(page, this.state.filters)
							// 		if (this.state.currentRel === "childs") this.fetch(page, this.state.filters)
							// 	})

							// 	// window.scrollTo({ top: 0, behavior: 'smooth' });
							// },
							pageSize: 35,
							showSizeChanger: false
						}
					}

				/>
			</div>
		);
	}
}

export default EditableTable;