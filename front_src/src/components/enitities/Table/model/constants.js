export const COL_CATEGORIES = [
  {
    title: "ID",
    dataIndex: "id",
    align: "center",
    width: "100px",
  },
  {
    title: "Имя",
    dataIndex: "name",
    width: "250px",
  },
  {
    title: "Описание",
    dataIndex: "description",
    width: "350px",
  },
  {
    title: "Код",
    dataIndex: "code",
  },
  {
    title: "Родитель",
    dataIndex: "parent",
  },
  {
    title: "Статус",
    dataIndex: "status",
    align: "center",
    width: "150px",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "150px",
  },
];

export const COL_NOMENCLATURE = [
  {
    title: "Тип",
    dataIndex: "type",
  },
  {
    title: "Имя",
    dataIndex: "name",
    ellipsis: true,
  },
  {
    title: "Описание",
    dataIndex: "description_short",
    width: "550px",
    ellipsis: true,
  },
  {
    title: "Изображение",
    dataIndex: "pictures",
    align: "center",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "150px",
  },
];

export const COL_ORGANIZATIONS = [
  {
    title: "Тип",
    dataIndex: "type",
    align: "center",
    width: 100,
  },
  {
    title: "Тип организации",
    dataIndex: "org_type",
    width: 100,
    align: "center",
    ellipsis: true,
  },
  {
    title: "Короткое имя",
    dataIndex: "short_name",
    width: 150,
  },
  {
    title: "Длинное имя",
    dataIndex: "full_name",
    ellipsis: true,
    width: 150,
  },
  {
    title: "Рабочее имя",
    dataIndex: "work_name",
    ellipsis: true,
    width: 150,
  },
  {
    title: "Префикс",
    dataIndex: "prefix",
    align: "center",
    width: 100,
  },
  {
    title: "Дата регистрации",
    dataIndex: "registration_date",
    align: "center",
    width: "150px",
  },
  {
    title: "OKVED",
    dataIndex: "okved",
    align: "center",
    width: "100px",
  },
  {
    title: "OKVED2",
    dataIndex: "okved2",
    align: "center",
    width: "100px",
  },
  {
    title: "OKPO",
    dataIndex: "okpo",
    align: "center",
    width: "100px",
  },
  {
    title: "Тип таксы",
    dataIndex: "tax_type",
    align: "center",
    width: "100px",
  },
  {
    title: "Tакса",
    dataIndex: "tax_percent",
    align: "center",
    width: "100px",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "150px",
  },
];

export const COL_CONTRACTS = [
  {
    title: "Номер",
    dataIndex: "number",
    align: "center",
    width: "100px",
  },
  {
    title: "Документ продажи",
    dataIndex: "payment_type",
  },
  {
    title: "Название договора",
    dataIndex: "name",
  },
  {
    title: "Название для печати",
    dataIndex: "print_name",
  },
  {
    title: "Контрагент",
    dataIndex: "contragent",
  },
  {
    title: "Организация",
    dataIndex: "organization",
  },
  {
    title: "До отгрузки",
    dataIndex: "payment_time",
  },
  {
    title: "От даты",
    dataIndex: "dated",
    width: "180px",
    align: "center",
  },
  {
    title: "Действует от",
    dataIndex: "used_from",
    width: "150px",
    align: "center",
  },
  {
    title: "Действует до",
    dataIndex: "used_to",
    width: "150px",
    align: "center",
  },
  {
    title: "Статус",
    dataIndex: "status",
    align: "center",
    width: "150px",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "150px",
  },
];

export const COL_WAREHOUSES = [
  {
    title: "ID",
    dataIndex: "id",
    align: "center",
    width: "100px",
  },
  {
    title: "Название",
    dataIndex: "name",
    width: "200px",
  },
  {
    title: "Тип помещения",
    dataIndex: "type",
    width: "200px",
  },
  {
    title: "Описание",
    dataIndex: "description",
    ellipsis: true,
  },
  {
    title: "Адресс",
    dataIndex: "address",
    ellipsis: true,
  },
  {
    title: "Телефон",
    dataIndex: "phone",
    align: "center",
    width: "200px",
  },
  {
    title: "Родитель",
    dataIndex: "parent",
    align: "center",
    width: "100px",
  },
  {
    title: "Статус",
    dataIndex: "status",
    align: "center",
    width: "150px",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "150px",
  },
];

export const COL_WAREHOUSES_DOCS = [
  {
    title: "Номер",
    dataIndex: "number",
    width: "100px",
    align: "center",
  },
  {
    title: "От даты",
    dataIndex: "dated",
    width: "200px",
    align: "center",
  },
  {
    title: "Операция",
    dataIndex: "operation",
  },
  {
    title: "Комментарий",
    dataIndex: "comment",
    ellipsis: true,
  },
  {
    title: "Организация",
    dataIndex: "organization",
  },
  {
    title: "Склад",
    dataIndex: "warehouse",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "150px",
  },
];

export const COL_WAREHOUSES_DOCS_GOODS = [
  {
    title: "Номенклатура",
    dataIndex: "nomenclature",
    editable: true,
    width: "80px",
    ellipsis: true,
  },
  {
    title: "Количество",
    dataIndex: "quantity",
    editable: true,
    width: "50px",
  },
  {
    title: "Единица измерения",
    dataIndex: "unit",
    width: "80px",
    editable: true,
    ellipsis: true,
  },
  {
    title: "Цена",
    dataIndex: "price",
    editable: true,
    width: "50px",
  },
  {
    title: "Тип",
    dataIndex: "price_type",
    editable: true,
    width: "50px",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "45px",
  },
];

export const COL_PRICES = [
  {
    title: "Код",
    dataIndex: "code",
    width: "30px",
    align: "center",
  },
  {
    title: "Название",
    dataIndex: "nomenclature_name",
    width: "90px",
    ellipsis: true,
  },
  {
    title: "Категория",
    dataIndex: "category_name",
    width: "70px",
    ellipsis: true,
  },
  {
    title: "Тип",
    dataIndex: "price_type",
    width: "50px",
    align: "center",
  },
  {
    title: "Цена",
    dataIndex: "price",
    width: "40px",
    align: "center",
    editable: true,
  },
  {
    title: "Единица измерения",
    dataIndex: "unit_name",
    width: "50px",
    ellipsis: true,
    align: "center",
  },
  {
    title: "Действует до",
    dataIndex: "date_to",
    width: "50px",
    editable: true,
    align: "center",
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "45px",
  },
];

export const COL_LOYALITY_SETTING = [
  {
    title: "Организация",
    dataIndex: "organization",
    width: "100px",
    align: "center",
  },
  {
    title: "Процент кешбэка",
    dataIndex: "cashback_percent",
    width: "50px",
    align: "center",
    ellipsis: true,
    editable: true,
  },
  {
    title: "Минимальная сумма чека",
    dataIndex: "minimal_checque_amount",
    width: "70px",
    align: "center",
    ellipsis: true,
    editable: true,
  },
  {
    title: "Максимальный процент списания",
    dataIndex: "max_withdraw_percentage",
    width: "70px",
    align: "center",
    editable: true,
  },
  {
    title: "Максимальный процент начисления",
    dataIndex: "max_percentage",
    width: "70px",
    align: "center",
    editable: true,
  },
  {
    title: "Начало периода",
    dataIndex: "start_period",
    width: "50px",
    ellipsis: true,
    align: "center",
    editable: true,
  },
  {
    title: "Конец периода",
    dataIndex: "end_period",
    width: "50px",
    align: "center",
    editable: true,
  },
  {
    title: "Действие",
    dataIndex: "action",
    align: "center",
    width: "45px",
  },
];

export const COL_ANALYTICS_CARDS = [
  {
    title: "Имя пользователя",
    dataIndex: "first_name",
    ellipsis: true,
  },
  {
    title: "Количество",
    dataIndex: "all_count",
    ellipsis: true,
  },
];

export const COL_ANALYTICS_EXPANDED = [
  {
    title: "Дата",
    dataIndex: "date",
    align: "center",
  },
  {
    title: "Количество",
    dataIndex: "day_count",
    align: "center",
  },
];
