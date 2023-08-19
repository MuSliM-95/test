import React, { useEffect, useState } from 'react'
import "./style.css"
import { Calendar } from 'antd';

import { CalendarOutlined, CheckCircleOutlined, StopOutlined, InfoCircleOutlined } from '@ant-design/icons';

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc'
import axios from 'axios';

import locale from 'antd/es/date-picker/locale/ru_RU';
import { Menu, Modal} from 'antd';
import Filter from '../filter';

dayjs.extend(utc)

const nav_items = (token) => [
    { label: "Календарь платежей", key: '1', icon: <CalendarOutlined /> },
  ]

const Analytics = ({listPayment, query, filter, websocket}) => {

    const [arr, setArr] = useState([])
	const [isModalOpen, setIsModalOpen] = useState(false);
	const [item, setItem] = useState({})

	useEffect(() => {
		setArr(listPayment)
	},[listPayment])

	websocket.onmessage = message => {
		const data = JSON.parse(message.data)

		if (data.target === "payments") {
			if (data.action === "create") {
				setArr(arr.concat(data.result))
				}
			}
			if (data.action === "edit") {
				const newData = [...arr];
					const index = newData.findIndex((item) => data.result.id === item.id);

					if (index !== -1) {
						const item = newData[index];
						newData.splice(index, 1, { ...item, ...data.result });
						setArr(newData);
					}
			}

			if (data.action === "delete") {
				let arrDelete = arr.filter(item => item.id!==data.result)
				setArr(arrDelete)
			}
	}
	


	const showModal = (item) => {
		setIsModalOpen(true);
		setItem(item)
	};
	
	const handleOk = () => {
		setIsModalOpen(false);
		setItem({})
	};
	
	const handleCancel = () => {
		setIsModalOpen(false);
		setItem({})
	};

    const dateCellRender = (value) => {
            
        return (
            <div>
                {arr.map((item,idx) => (
					(dayjs.unix(item.date).date() === value.date() && dayjs.unix(item.date).month() === value.month() && dayjs.unix(item.date).year() === value.year() ? 
					<div className='title__payment' onClick={() => showModal(item)} key={idx}>
						{item.status ? <CheckCircleOutlined style={{color: item.type==='outgoing' ? 'red' : 'green', marginRight:"10px"}} /> : <StopOutlined style={{ color: item.type==='outgoing' ? 'red' : 'green', marginRight:"10px"}}/> }
						<span className={`${item.type}`}>{item.amount}</span>
					</div>
					: null)
				)

            )}
            </div>
        
        )
    };

   const handleFeatch = (page, pars, sort = null) => {
		const limit = 35
		const offset = (page * 35) - 35 || 35
		let filters = pars
		let params = {
			params: {
				token: query.token,
				limit: limit,
				offset: offset,
				...filters,
			}
		}
        
		axios.get(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments`, params)
		.then(response => {
			const newArr =  [...new Set(response.data.result.map(item => ({type: item.type, amount: item.amount, date: item.date, name:item.name})))]
			setArr(newArr)

		});
        
		const serialize = (obj) => {
			var str = [];
			for (var p in obj)
				if (obj.hasOwnProperty(p)) {
					str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
				}
			return str.join("&");
		}

		const newurl = window.location.protocol + "//" + window.location.host + window.location.pathname + "?" + serialize({ ...query, ...filters });
		window.history.pushState({ path: newurl }, "", newurl);
	}

    return (
        <div className='content'>
			{filter && <Filter query={query} onChange={handleFeatch}/>}
            <Menu
				theme="light"
				mode="horizontal"
				items={nav_items(query.token)}
				style={{ float: 'left', marginBottom:'20px'}}
			/>

			<Modal title="Информация о платеже" open={isModalOpen} onOk={handleOk} onCancel={handleCancel}>
					<div>
						<label>Название платежа:</label>
						<h3>{item.name}</h3>
					</div>
					<div>
						<label>Сумма платежа:</label>
						<h3>{item.amount}</h3>
					</div>
					<div>
						<label>Дата платежа:</label>
						<h3>{dayjs.unix(item.date).format('DD-MM-YYYY')}</h3>
					</div>
					<div>
						<label>Статус платежа:</label>
						<p>{item.type==='incoming' ? <CheckCircleOutlined style={{color:'green', fontSize:'60px'}}/> : item.type==='outgoing' ? <StopOutlined style={{color:'red', fontSize:'60px'}}/> : <InfoCircleOutlined style={{fontSize:'60px'}} />}</p>
					</div>
			</Modal>
			

            <Calendar  
				dateCellRender={dateCellRender} 
				locale={locale}
				mode={"month"}
			 />;
        </div>
)}

export default Analytics