import { Spin, Result } from 'antd';
import logo from './cbox_logo.jpg'
import { SmileOutlined } from '@ant-design/icons';

const LoadingState = () => (
    <Result
        icon={<img style={{ width: '30vh' }} alt={"Логотип"} src={logo}></img>}
        title={<>Пожалуйста, подождите, идет загрузка <SmileOutlined/></>}
        extra={<Spin size="large"/>}
    />
);

export default LoadingState;