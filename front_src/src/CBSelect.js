import { BankOutlined, WalletOutlined } from '@ant-design/icons';
import { Menu } from 'antd';

function CBSelect(props) {
    const index = props.CBData.findIndex((item) => props.token === item.token)
    const CurrentCB = props.CBData[index]

    const sub = [{ label: "Кассы", key: "cboxes", icon: <WalletOutlined />, children: props.CBOptions }]

    return (
        <>
            <BankOutlined /> Баланс аккаунта: <b>{CurrentCB.balance}</b> / <b>{CurrentCB.name}</b>
            <Menu items={sub} selectedKeys={CurrentCB.name} mode="horizontal" style={{ display: "inline-block" }} />
        </>
    );
}

export default CBSelect;