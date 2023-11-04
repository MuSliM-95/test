import './Menu.css';

import { Layout, Menu } from 'antd';
import { Link } from 'react-router-dom';
import { LineChartOutlined, TeamOutlined, HistoryOutlined, CreditCardOutlined, CodeOutlined, FundOutlined, SolutionOutlined, ApiOutlined } from '@ant-design/icons';
import React from 'react';

const { Header, Content, Footer } = Layout;

const nav_items = (props) => [
  { label: <Link to={`/?token=${props.query.token}`}>Платежи</Link>, key: 'payments', icon: <LineChartOutlined /> },
  { label: <Link to={`/loyality_cards?token=${props.query.token}`}>Карты лояльности</Link>, key: 'loyality_cards', icon: <LineChartOutlined /> },
  { label: <Link to={`/loyality_transactions?token=${props.query.token}`}>Транзакции по КЛ</Link>, key: 'loyality_transactions', icon: <LineChartOutlined /> },
  { label: <Link to={`/payboxes?token=${props.query.token}`}>Счета</Link>, key: 'payboxes', icon: <CreditCardOutlined /> },
  { label: <Link to={`/projects?token=${props.query.token}`}>Проекты</Link>, key: 'projects', icon: <FundOutlined /> },
  { label: <Link to={`/users?token=${props.query.token}`}>Пользователи</Link>, key: 'users', icon: <TeamOutlined /> },
  { label: <Link to={`/contragents?token=${props.query.token}`}>Контрагенты</Link>, key: 'contragents', icon: <SolutionOutlined /> },
  { label: <Link to={`/integrations?token=${props.query.token}`}>Интеграции</Link>, key: 'integrations', icon: <ApiOutlined /> },
  { label: <Link to={`/events?token=${props.query.token}`}>События</Link>, key: 'events', icon: <HistoryOutlined />},
  {
    label: (
      <a href={`https://${process.env.REACT_APP_APP_URL}/api/v1/docs`} target="_blank" rel="noopener noreferrer">
        Swagger API
      </a>
    ), key: 'apiswagger', icon: <CodeOutlined />
  },
  {
    label: (
      <a href={`https://${process.env.REACT_APP_APP_URL}/docasaurus/`} target="_blank" rel="noopener noreferrer">
        Документация
      </a>
    ), key: 'api', icon: <FileMarkdownOutlined />
  },
]

class MainMenu extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
    }
    
  }

  render() {
    let selected = []
    if (this.props.pathName === '/') {
      selected = ['payments']
    }
    else {
      selected = [this.props.pathName.split("/")[1]]
    }

    return (
      <Layout>
        <Header
          style={{
            position: 'fixed',
            zIndex: 2,
            width: '100%',
            background: '#fff'
          }}
        >

          <Menu
            theme="light"
            mode="horizontal"
            defaultSelectedKeys={selected}
            items={nav_items(this.props)}
            style={{ float: 'left' }}
          />

          <div style={{ float: 'right' }}>{this.props.CBInfoText}</div>

        </Header>

        <Content
          className="site-layout"
          style={{
            padding: '10px 20px',
            marginTop: 64,
          }}
        >

          <div
            className="site-layout-background"
            style={{
              padding: 15,
              minHeight: "83.5vh",
            }}
          >
            {this.props.content}
          </div>

        </Content>

        <Footer
          style={{
            textAlign: 'center',
          }}
        >
          TableCRM © 2022 Платформа для учета финансов
        </Footer>
      </Layout>
    )
  };

}

export default MainMenu;