import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route, Link } from "react-router-dom";
import axios from "axios";

import { QueryClient, QueryClientProvider } from "react-query";

import { Select, Result, Layout, Menu, Button } from "antd";
import {
  BankOutlined,
  AlertOutlined,
  LineChartOutlined,
  TeamOutlined,
  HistoryOutlined,
  CreditCardOutlined,
  CodeOutlined,
  FundOutlined,
  SolutionOutlined,
  ApiOutlined,
  StockOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  FileMarkdownOutlined,
  BarChartOutlined,
  SnippetsOutlined,
  ShoppingCartOutlined,
  TableOutlined,
} from "@ant-design/icons";

import PaymentsTable from "./Payments";
import CBSelect from "./CBSelect";
import AuthError from "./AuthError";
import LoadingState from "./Loading";
import PayboxTable from "./Payboxes";
import ProjectsTable from "./Projects";
import UsersTable from "./Users";
import CATable from "./Contragents";
import Integrations from "./Integrations";
import Events from "./Events";
import Analytics from "./components/analytics";
import LoyalityCards from "./LoyalityCards";
import LoyalityTransactions from "./LoyalityTransactions";
import DocsSales from "./DocsSales";
import {
  TableCategoriesPage,
  TableNomenclature,
  TableOrganizations,
  TableContracts,
  TableWarehouses,
  TableWarehousesDocs,
  TablePrices,
  TableDocsPurchases,
  TablePricesHandsontable,
  TableLoyalitySettings,
  TableLoyalityReport,
} from "./components/pages/";
import { currentMonthRange } from "./components/shared";

const { Option } = Select;
const { Header, Content, Footer, Sider } = Layout;
const root = ReactDOM.createRoot(document.getElementById("root"));

const urlSearchParams = new URLSearchParams(window.location.search);
const params = Object.fromEntries(urlSearchParams.entries());
const queryClient = new QueryClient();

const nav_items = (token) => [
  {
    label: <Link to={`/?token=${token}`}>Платежи</Link>,
    key: "payments",
    icon: <LineChartOutlined />,
  },
  {
    label: <Link to={`/loyality_cards?token=${token}`}>Карты лояльности</Link>,
    key: "loyality_cards",
    icon: <CreditCardOutlined />,
  },
  {
    label: (
      <Link to={`/loyality_transactions?token=${token}`}>Транзакции по КЛ</Link>
    ),
    key: "loyality_transactions",
    icon: <BarChartOutlined />,
  },
  {
    label: <Link to={`/payboxes?token=${token}`}>Счета</Link>,
    key: "payboxes",
    icon: <CreditCardOutlined />,
  },
  {
    label: <Link to={`/projects?token=${token}`}>Проекты</Link>,
    key: "projects",
    icon: <FundOutlined />,
  },
  {
    label: <Link to={`/users?token=${token}`}>Пользователи</Link>,
    key: "users",
    icon: <TeamOutlined />,
  },
  {
    label: <Link to={`/analytecs?token=${token}`}>Аналитика</Link>,
    key: "analytecs",
    icon: <StockOutlined />,
  },
  {
    label: <Link to={`/contragents?token=${token}`}>Контрагенты</Link>,
    key: "contragents",
    icon: <SolutionOutlined />,
  },
  {
    label: <Link to={`/integrations?token=${token}`}>Интеграции</Link>,
    key: "integrations",
    icon: <ApiOutlined />,
  },
  {
    label: <Link to={`/docs_sales?token=${token}`}>Документы продаж</Link>,
    key: "docs_sales",
    icon: <SnippetsOutlined />,
  },
  {
    label: <Link to={`/events?token=${token}`}>События</Link>,
    key: "events",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/categories?token=${token}`}>Категории</Link>,
    key: "categories",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/nomenclature?token=${token}`}>Номенклатура</Link>,
    key: "nomenclature",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/organizations?token=${token}`}>Организации</Link>,
    key: "organizations",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/contracts?token=${token}`}>Контракты</Link>,
    key: "contracts",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/warehouses?token=${token}`}>Складские помещения</Link>,
    key: "warehouses",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/docs_warehouse?token=${token}`}>Документы склада</Link>,
    key: "docs_warehouse",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/prices?token=${token}`}>Цены</Link>,
    key: "prices",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/loyality_settings?token=${token}`}>Настройки карты лояльности</Link>,
    key: "loyality_settings",
    icon: <HistoryOutlined />,
  },
  {
    label: (
      <Link to={`/analytics_cards?token=${token}`}>
        Отчеты по карте лояльности
      </Link>
    ),
    key: "loyality_reports",
    icon: <HistoryOutlined />,
  },
  {
    label: <Link to={`/docs_purchases?token=${token}`}>Закупки</Link>,
    key: "docs_purchases",
    icon: <ShoppingCartOutlined />,
  },
  {
    label: <Link to={`/nomenclature_handsontable?token=${token}`}>Цены</Link>,
    key: "nomenclature_handsontable",
    icon: <TableOutlined />,
  },
  {
    label: (
      <a
        href={`https://${process.env.REACT_APP_APP_URL}/api/v1/docs`}
        target="_blank"
        rel="noopener noreferrer"
      >
        Swagger API
      </a>
    ),
    key: "swaggerapi",
    icon: <CodeOutlined />,
  },
  {
    label: (
      <a
        href={`https://${process.env.REACT_APP_APP_URL}/docasaurus/`}
        target="_blank"
        rel="noopener noreferrer"
      >
        Документация
      </a>
    ),
    key: "api",
    icon: <FileMarkdownOutlined />,
  },
];

class LogIn extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      collapsed: true,
      loggedIn: false,
      manufacturersData: [],
      organizationsData: [],
      contractsData: [],
      conteragentsData: [],
      nomenclatureData: [],
      categoriesData: [],
      priceTypeData: [],
      pricesData: [],
      unitsData: [],
      loyalitySettingsData: [],
      loaded: [
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
      ],
      authError403: false,
      cboxData: {},
      CBInfoText: (
        <span style={{ marginRight: 10 }}>
          <BankOutlined /> <b>Загрузка...</b>{" "}
        </span>
      ),
    };
  }

  timeout = 250;

  componentDidMount() {
    let Loading = this.state.loaded.slice();
    const { date_from, date_to } = currentMonthRange();

    axios
      .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/cashboxes_meta`, {
        params: { token: params.token },
      })
      .then((response) => {
        var options_list = [];
        for (var i in response.data.cboxes) {
          options_list.push({
            label: (
              <a
                href={
                  `https://${process.env.REACT_APP_APP_URL}/?token=` +
                  response.data.cboxes[i].token
                }
                target="_self"
                rel="noopener noreferrer"
              >
                {response.data.cboxes[i].name}
              </a>
            ),
            key: response.data.cboxes[i].name,
          });
        }

        this.setState({
          loggedIn: true,
          CBInfoText: (
            <CBSelect
              token={params.token}
              CBOptions={options_list}
              CBData={response.data.cboxes}
            ></CBSelect>
          ),
          cboxData: response.data.cboxes,
          invite_token: response.data.invite_token,
        });

        axios
          .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/payboxes`, {
            params: { token: params.token, limit: 35, offset: 0 },
          })
          .then((response) => {
            var options_list = [];
            for (var i in response.data.result) {
              let color = "green";
              if (response.data.result[i].balance < 0) {
                color = "red";
              }
              options_list.push(
                <Option key={response.data.result[i].id}>
                  {response.data.result[i].name}:{" "}
                  <font color={color}>
                    {response.data.result[i].balance} руб.
                  </font>
                </Option>
              );
            }

            const uniquePayboxes = response.data.result.map((item) => {
              return { value: item.name };
            });

            this.setState({
              payboxesDS: response.data.result,
              payboxesCount: response.data.count,
              payboxesSelect: options_list,
              payboxesMeta: uniquePayboxes,
            });
          });

        axios
          .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/payments`, {
            params: { ...params, limit: 35, offset: 0 },
          })
          .then((response) => {
            const nameValues = [
              ...new Set(response.data.result.map((item) => item.name)),
            ].filter((x) => x !== null);
            const tagValues = [
              ...new Set(response.data.result.map((item) => item.tags)),
            ].filter((x) => x !== null);

            const mergedTags = [
              ...new Set(
                [].concat.apply(
                  [],
                  tagValues.map((item) => item.split(","))
                )
              ),
            ];

            const uniqueNameValues = nameValues.map((item) => {
              return { value: item, label: item };
            });
            const uniqueTagValues = mergedTags.map((value, i) => {
              return {
                label: `${value}`,
                value: i,
              };
            });
            // const uniqueArticleValues = articleValues.map(item => { return { value: item } });
            const paymentCalendar = [
              ...new Set(response.data.result.map((item) => item)),
            ];

            const { Option } = Select;
            const children = [];
            for (let i = 0; i < uniqueTagValues.length; i++) {
              children.push(
                <Option key={uniqueTagValues[i].value}>
                  {uniqueTagValues[i].value}
                </Option>
              );
            }

            const metaDict = { names: uniqueNameValues, tags: uniqueTagValues };

            Loading[0] = true;

            this.setState({
              PaymentsCount: response.data.count,
              PaymentsDS: response.data.result,
              loaded: Loading,
              paymentsMeta: metaDict,
              listPayment: paymentCalendar,
            });
          });

        axios
          .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/projects`, {
            params: { token: params.token, limit: 35, offset: 0 },
          })
          .then((response) => {
            var options_list = [];

            options_list.push(<Option key={0}>Не указывать</Option>);

            for (var i in response.data.result) {
              options_list.push(
                <Option key={response.data.result[i].id}>
                  {response.data.result[i].id}. {response.data.result[i].name}:{" "}
                  {response.data.result[i].proj_sum} руб.
                </Option>
              );
            }

            const uniqueProjects = response.data.result.map((item) => {
              return { value: item.name };
            });

            Loading[1] = true;

            this.setState({
              ProjectsCount: response.data.count,
              ProjectsDS: response.data.result,
              loaded: Loading,
              ProjectSelect: options_list,
              ProjMeta: uniqueProjects,
            });
          });

        axios
          .get(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/cashbox_users`,
            {
              params: { token: params.token, limit: 100, offset: 0 },
            }
          )
          .then((response) => {
            Loading[2] = true;

            this.setState({
              UsersCount: response.data.count,
              UsersDS: response.data.result,
              loaded: Loading,
            });
          });

        axios
          .get(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_cards/`,
            {
              params: { token: params.token, limit: 100, offset: 0 },
            }
          )
          .then((response) => {
            Loading[3] = true;

            this.setState({
              LoyalityCount: response.data.count,
              LoyalityDS: response.data.result,
              loaded: Loading,
            });
          });

        axios
          .get(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/organizations/`,
            {
              params: { token: params.token, limit: 100, offset: 0 },
            }
          )
          .then((response) => {
            Loading[4] = true;
            let options_list = [];

            for (var i in response.data) {
              options_list.push({
                value: `${response.data[i].id}`,
                label: response.data[i].short_name,
              });
            }

            this.setState({
              OrgsCount: response.data.length,
              OrgsDS: options_list,
              loaded: Loading,
            });
          });

        axios
          .get(
            `https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_transactions/`,
            {
              params: { token: params.token, limit: 100, offset: 0 },
            }
          )
          .then((response) => {
            Loading[5] = true;

            this.setState({
              LoyalityTransactionsCount: response.data.count,
              LoyalityTransactionsDS: response.data.result,
              loaded: Loading,
            });
          });

        axios
          .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/contragents`, {
            params: { token: params.token, limit: 35, offset: 0 },
          })
          .then((response) => {
            var options_list = [];

            options_list.push(<Option key={0}>Не указывать</Option>);

            for (var i in response.data.result) {
              options_list.push(
                <Option key={response.data.result[i].id}>
                  {response.data.result[i].name}
                </Option>
              );
            }

            Loading[6] = true;

            const nameValues = [
              ...new Set(response.data.result.map((item) => item.name)),
            ].filter((x) => x !== null);
            const PhoneValues = [
              ...new Set(response.data.result.map((item) => item.phone)),
            ].filter((x) => x !== null);
            const InnValues = [
              ...new Set(response.data.result.map((item) => item.inn)),
            ].filter((x) => x !== null);

            const uniqueNameValues = nameValues.map((item) => {
              return { value: item };
            });
            const uniquePhoneValues = PhoneValues.map((item) => {
              return { value: item };
            });
            const uniqueInnValues = InnValues.map((item) => {
              return { value: item };
            });

            this.setState({
              CACount: response.data.count,
              CADS: response.data.result,
              CASel: options_list,
              loaded: Loading,
              CAAC: {
                names: uniqueNameValues,
                phones: uniquePhoneValues,
                inns: uniqueInnValues,
              },
            });

            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/categories/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[7] = true;
                this.setState({
                  categoriesData: res.data,
                });
              });

            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[8] = true;
                this.setState({
                  nomenclatureData: res.data,
                });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/manufacturers/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[9] = true;
                this.setState({ manufacturersData: res.data });
              });
            axios
              .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/units/`, {
                params: { token: params.token, limit: 500 },
              })
              .then((res) => {
                Loading[10] = true;
                this.setState({ unitsData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/organizations/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[11] = true;
                this.setState({ organizationsData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/contracts/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[12] = true;
                this.setState({ contractsData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/warehouses/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[13] = true;
                this.setState({ warehousesData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_warehouse/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[14] = true;
                this.setState({ warehouseDocsData: res.data });
              });
            axios
              .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/prices/`, {
                params: { ...params },
              })
              .then((res) => {
                Loading[15] = true;
                this.setState({ pricesData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/price_types/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[16] = true;
                this.setState({ priceTypeData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/loyality_settings/`,
                {
                  params: { token: params.token },
                }
              )
              .then((res) => {
                Loading[17] = true;
                this.setState({ loyalitySettingsData: res.data });
              });
            axios
              .get(
                `https://${process.env.REACT_APP_APP_URL}/api/v1/analytics_cards/`,
                {
                  params: {
                    token: params.token,
                    date_from: params.date_from || date_from,
                    date_to: params.date_to || date_to,
                  },
                }
              )
              .then((res) => {
                Loading[18] = true;
                this.setState({
                  analyticsCardsData: res.data,
                });
              });

            this.ws_connect();
          });
      })
      .catch((error) => {
        if (error.response.status === 403) {
          this.setState({ authError403: true });
        }
      });
  }

  ws_connect = () => {
    var ws = new WebSocket(
      `wss://${process.env.REACT_APP_APP_URL}/ws/` + params.token
    );
    let that = this; // cache the this
    var connectInterval;

    ws.onopen = () => {
      console.log("connected websocket main component");

      this.setState({ ws: ws });

      that.timeout = 250; // reset timer to 250 on open of websocket connection
      clearTimeout(connectInterval); // clear Interval on on open of websocket connection
    };

    // websocket onclose event listener
    ws.onclose = (e) => {
      console.log(
        `Socket is closed. Reconnect will be attempted in ${Math.min(
          10000 / 1000,
          (that.timeout + that.timeout) / 1000
        )} second.`,
        e.reason
      );

      that.timeout = that.timeout + that.timeout; //increment retry interval
      connectInterval = setTimeout(this.check, Math.min(10000, that.timeout)); //call check function after timeout
    };

    // websocket onerror event listener
    ws.onerror = (err) => {
      console.error(
        "Socket encountered error: ",
        err.message,
        "Closing socket"
      );

      ws.close();
    };
  };

  check = () => {
    const { ws } = this.state;
    if (!ws || ws.readyState === WebSocket.CLOSED) this.connect();
  };

  render() {
    const pathName = window.location.pathname;
    if (!this.state.authError403) {
      const set = new Set(this.state.loaded);
      const arr = Array.from(set);
      const loaded =
        arr.length === 1 && JSON.stringify(arr) !== JSON.stringify([false]);
      if (!params.token) {
        return (
          <Result
            status="403"
            title={
              <>
                Вы не ввели токен <AlertOutlined />
                <br /> Пожалуйста, проверьте вашу ссылку.
              </>
            }
          />
        );
      }
      if (this.state.loggedIn && loaded && this.state.ws) {
        let selected = [];
        if (pathName === "/") {
          selected = ["payments"];
        } else {
          selected = [pathName.split("/")[1]];
        }
        return (
          <QueryClientProvider client={queryClient}>
            <BrowserRouter>
              <Layout>
                <Sider
                  trigger={null}
                  collapsible
                  collapsed={this.state.collapsed}
                  style={{ background: "white" }}
                >
                  <Menu
                    theme="light"
                    mode="inline"
                    defaultSelectedKeys={selected}
                    items={nav_items(params.token)}
                  />
                </Sider>
                <Layout>
                  <Header
                    style={{
                      padding: 0,
                      marginTop: -5,
                      background: "white",
                    }}
                  >
                    <Button
                      type="text"
                      icon={
                        this.state.collapsed ? (
                          <MenuUnfoldOutlined />
                        ) : (
                          <MenuFoldOutlined />
                        )
                      }
                      onClick={() =>
                        this.setState({ collapsed: !this.state.collapsed })
                      }
                      style={{
                        fontSize: "16px",
                        width: 64,
                        height: 64,
                      }}
                    />
                    <div style={{ float: "right" }}>
                      {this.state.CBInfoText}
                    </div>
                  </Header>
                  <Content className="site-layout">
                    <div
                      className="site-layout-background"
                      style={{
                        paddingLeft: 15,
                        paddingRight: 15,
                        paddingTop: 15,
                        minHeight: "83.5vh",
                      }}
                    >
                      {
                        <Routes>
                          <Route
                            path="/"
                            element={
                              <PaymentsTable
                                PBData={this.state.payboxesSelect}
                                PRData={this.state.ProjectSelect}
                                payboxesMeta={this.state.payboxesMeta}
                                paymentsMeta={this.state.paymentsMeta}
                                projMeta={this.state.ProjMeta}
                                query={params}
                                caData={this.state.CADS}
                                caSel={this.state.CASel}
                                caMeta={this.state.CAAC}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/payboxes"
                            element={
                              <PayboxTable
                                query={params}
                                payboxesData={{
                                  c: this.state.payboxesCount,
                                  ds: this.state.payboxesDS,
                                }}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/loyality_cards"
                            element={
                              <LoyalityCards
                                query={params}
                                orgsData={this.state.OrgsDS}
                                loyalityCardsData={{
                                  c: this.state.LoyalityCount,
                                  ds: this.state.LoyalityDS,
                                }}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/loyality_transactions"
                            element={
                              <LoyalityTransactions
                                query={params}
                                loyalityCardsData={this.state.LoyalityDS}
                                loyalityTransactionsData={{
                                  c: this.state.LoyalityTransactionsCount,
                                  ds: this.state.LoyalityTransactionsDS,
                                }}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/analytecs"
                            element={
                              <Analytics
                                listPayment={this.state.listPayment}
                                websocket={this.state.ws}
                                query={params}
                                filter={true}
                              />
                            }
                          />

                          <Route
                            path="/projects"
                            element={
                              <ProjectsTable
                                c={this.state.ProjectsCount}
                                ds={this.state.ProjectsDS}
                                query={params}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/integrations"
                            element={
                              <Integrations
                                query={params}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/events"
                            element={
                              <Events
                                token={params.token}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/users"
                            element={
                              <UsersTable
                                c={this.state.UsersCount}
                                ds={this.state.UsersDS}
                                query={params}
                                invite={this.state.invite_token}
                                websocket={this.state.ws}
                              />
                            }
                          />

                          <Route
                            path="/contragents"
                            element={
                              <CATable
                                query={params}
                                c={this.state.CACount}
                                ds={this.state.CADS}
                                websocket={this.state.ws}
                                // meta={this.state.CAAC}
                              />
                            }
                          />
                          <Route
                            path="/categories"
                            element={
                              <TableCategoriesPage
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.categoriesData}
                              />
                            }
                          />
                          <Route
                            path="/nomenclature"
                            element={
                              <TableNomenclature
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.nomenclatureData}
                                manufacturersData={this.state.manufacturersData}
                                categoriesData={this.state.categoriesData}
                                unitsData={this.state.unitsData}
                              />
                            }
                          />
                          <Route
                            path="/organizations"
                            element={
                              <TableOrganizations
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.organizationsData}
                              />
                            }
                          />
                          <Route
                            path="/contracts"
                            element={
                              <TableContracts
                                token={params.token}
                                organizationsData={this.state.organizationsData}
                                conteragentsData={this.state.CADS}
                                websocket={this.state.ws}
                                initialData={this.state.contractsData}
                              />
                            }
                          />
                          <Route
                            path="/warehouses"
                            element={
                              <TableWarehouses
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.warehousesData}
                              />
                            }
                          />
                          <Route
                            path="/docs_warehouse"
                            element={
                              <TableWarehousesDocs
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.warehouseDocsData}
                                warehousesData={this.state.warehousesData}
                                nomenclatureData={this.state.nomenclatureData}
                                organizationsData={this.state.organizationsData}
                                unitsData={this.state.unitsData}
                                priceTypesData={this.state.priceTypeData}
                              />
                            }
                          />
                          <Route
                            path="/prices"
                            element={
                              <TablePrices
                                params={params}
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.pricesData}
                                priceTypesData={this.state.priceTypeData}
                                manufacturersData={this.state.manufacturersData}
                                nomenclatureData={this.state.nomenclatureData}
                                categoriesData={this.state.categoriesData}
                                unitsData={this.state.unitsData}
                              />
                            }
                          />
                          <Route
                            path="/loyality_settings"
                            element={
                              <TableLoyalitySettings
                                params={params}
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.loyalitySettingsData}
                                organizationsData={this.state.organizationsData}
                              />
                            }
                          />
                          <Route
                            path="/analytics_cards"
                            element={
                              <TableLoyalityReport
                                params={params}
                                token={params.token}
                                websocket={this.state.ws}
                                initialData={this.state.analyticsCardsData}
                              />
                            }
                          />
                          <Route
                            path="/docs_sales"
                            element={
                              <DocsSales
                                query={params}
                                websocket={this.state.ws}
                              />
                            }
                          />
                          <Route
                            path="/docs_purchases"
                            element={
                              <TableDocsPurchases
                                token={params.token}
                                websocket={this.state.ws}
                              />
                            }
                          />
                          <Route
                            path="/nomenclature_handsontable"
                            element={
                              <TablePricesHandsontable
                                token={params.token}
                                websocket={this.state.ws}
                              />
                            }
                          />
                        </Routes>
                      }
                    </div>
                  </Content>

                  <Footer
                    style={{
                      textAlign: "center",
                    }}
                  >
                    TableCRM © 2022 Платформа для учета финансов
                  </Footer>
                </Layout>
              </Layout>
            </BrowserRouter>
          </QueryClientProvider>
        );
      } else {
        return <LoadingState />;
      }
    } else {
      return <AuthError />;
    }
  }
}

root.render(<LogIn></LogIn>);
