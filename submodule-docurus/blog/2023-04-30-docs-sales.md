---
slug: docs-sales
title: API-документация по документам продаж
authors:
  name: SFSky
  title: Команда Tablecrm.com
  image_url: https://app.tablecrm.com/photos/file_37.jpg
---

<font size={7}>Добавление документов продаж</font><br/><br/><br/>

<b>Метод</b><br/>
POST /api/v1/docs_sales<br/>
<b>Описание</b><br/>
Метод позволяет добавлять документы продаж в кассу пакетно.<br/>
<b>Ограничения</b><br/>
Метод доступен только авторизованным пользователям (token в строке запроса)<br/>
<b>Заголовок запроса</b><br/>
Content-Type: application/json<br/>
<b>Параметры запроса (тело запроса)</b><br/>
 <table>
  <tr>
    <th>Параметр</th>
    <th>Обязательность</th>
    <th>Тип данных</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td>number</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Номер документа продажи</td>
  </tr>
  <tr>
    <td>dated</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Дата проведения документа продажи</td>
  </tr>
  <tr>
    <td>operation</td>
    <td>Не обязателен</td>
    <td>str</td>
    <td>Тип операции, возможные варианты: «Заказ», «Реализация»</td>
  </tr>
  <tr>
    <td>docs_sales</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор продажи</td>
  </tr>
  <tr>
    <td>comment</td>
    <td>Не обязателен</td>
    <td>str</td>
    <td>Комментарий к документу продажи</td>
  </tr>
  <tr>
    <td>client</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор клиента в базе (contragents)</td>
  </tr>
  <tr>
    <td>contragent</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор контрагента в базе (contragents)</td>
  </tr>
  <tr>
    <td>contract</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор контракта в базе (contracts)</td>
  </tr>
  <tr>
    <td>organization</td>
    <td>Обязателен</td>
    <td>int</td>
    <td>Идентификатор организации в базе (organizations)</td>
  </tr>
  <tr>
    <td>warehouse</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор склада в базе (warehouses)</td>
  </tr>
  <tr>
    <td>tax_included</td>
    <td>Не обязателен</td>
    <td>bool</td>
    <td>Включен ли налог в продажу</td>
  </tr>
  <tr>
    <td>tax_active</td>
    <td>Не обязателен</td>
    <td>bool</td>
    <td>?</td>
  </tr>
  <tr>
    <td>sales_manager</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор менеджера по продаже (users)</td>
  </tr>
  <tr>
    <td>goods</td>
    <td>Не обязателен</td>
    <td>array</td>
    <td>Массив, включающий в себя информацию о проданных предметах или услуг</td>
  </tr>
</table>
<b>Параметры запроса параметра goods</b><br/>
<table>
  <tr>
    <th>Параметр</th>
    <th>Обязательность</th>
    <th>Тип данных</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td>price_type</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Тип цены</td>
  </tr>
  <tr>
    <td>price</td>
    <td>Обязателен</td>
    <td>float</td>
    <td>Цена</td>
  </tr>
  <tr>
    <td>quantity</td>
    <td>Обязателен</td>
    <td>int</td>
    <td>Количество</td>
  </tr>
  <tr>
    <td>unit</td>
    <td>Не обязателен</td>
    <td>int</td>
    <td>Идентификатор единицы измерения</td>
  </tr>
  <tr>
    <td>tax</td>
    <td>Не обязателен</td>
    <td>float</td>
    <td>Налог</td>
  </tr>
  <tr>
    <td>discount</td>
    <td>Не обязателен</td>
    <td>float</td>
    <td>Скидка (процент)</td>
  </tr>
  <tr>
    <td>sum_discounted</td>
    <td>Не обязателен</td>
    <td>float</td>
    <td>Сумма скидки</td>
  </tr>
  <tr>
    <td>status</td>
    <td>Не обязателен</td>
    <td>str</td>
    <td>Статус</td>
  </tr>
  <tr>
    <td>nomenclature</td>
    <td>Обязателен</td>
    <td>int</td>
    <td>Номенклатура</td>
  </tr>
</table>
<b>Пример запроса</b><br/>


<code>

&emsp;{
&emsp;&emsp;"number": "string",
&emsp;&emsp;"dated": 0,
&emsp;&emsp;"operation": "Заказ",
&emsp;&emsp;"docs_sales": 0,
&emsp;&emsp;"comment": "string",
&emsp;&emsp;"client": 0,
&emsp;&emsp;"contragent": 0,
&emsp;&emsp;"contract": 0,
&emsp;&emsp;"organization": 0,
&emsp;&emsp;"warehouse": 0,
&emsp;&emsp;"tax_included": true,
&emsp;&emsp;"tax_active": true,
&emsp;&emsp;"sales_manager": 0,
&emsp;&emsp;"goods": [
&emsp;&emsp;&emsp;{
&emsp;&emsp;&emsp;&emsp;"price_type": 0,
&emsp;&emsp;&emsp;&emsp;"price": 0,
&emsp;&emsp;&emsp;&emsp;"quantity": 0,
&emsp;&emsp;&emsp;&emsp;"unit": 0,
&emsp;&emsp;&emsp;&emsp;"tax": 0,
&emsp;&emsp;&emsp;&emsp;"discount": 0,
&emsp;&emsp;&emsp;&emsp;"sum_discounted": 0,
&emsp;&emsp;&emsp;&emsp;"status": "string",
&emsp;&emsp;&emsp;&emsp;"nomenclature": 0
&emsp;&emsp;&emsp;}
&emsp;&emsp;]
&emsp;}

</code>

<b>HTTP коды ответа</b><br/>
<table>
  <tr>
    <th>Код ответа</th>
    <th>Условие</th>
  </tr>
  <tr>
    <td>200</td>
    <td>Документы успешно созданы</td>
  </tr>
  <tr>
    <td>400</td>
    <td>Документы не созданы</td>
  </tr>
</table>


<b>Параметры ответа</b><br/>
<table>
  <tr>
    <th>Параметр</th>
    <th>Тип данных</th>
    <th>Описание</th>
  </tr>
  <tr>
    <td>id</td>
    <td>int</td>
    <td>Идентификатор созданного документа</td>
  </tr>
  <tr>
    <td>number</td>
    <td>str</td>
    <td>Номер созданного документа</td>
  </tr>
  <tr>
    <td>dated</td>
    <td>int</td>
    <td>Дата создания документа</td>
  </tr>
  <tr>
    <td>operation</td>
    <td>str</td>
    <td>Тип операции созданного документа</td>
  </tr>
  <tr>
    <td>docs_sales</td>
    <td>int</td>
    <td>Идентификатор продажи</td>
  </tr>
  <tr>
    <td>comment</td>
    <td>str</td>
    <td>Комментарий документа продажи</td>
  </tr>
  <tr>
    <td>client</td>
    <td>int</td>
    <td>Идентификатор клиента</td>
  </tr>
  <tr>
    <td>contragent</td>
    <td>int</td>
    <td>Идентификатор контрагента</td>
  </tr>
  <tr>
    <td>contract</td>
    <td>int</td>
    <td>Идентификатор контракта</td>
  </tr>
  <tr>
    <td>organization</td>
    <td>int</td>
    <td>Идентификатор организации</td>
  </tr>
  <tr>
    <td>warehouse</td>
    <td>int</td>
    <td>Идентификатор склада</td>
  </tr>
  <tr>
    <td>sum</td>
    <td>int</td>
    <td>Сумма продажи</td>
  </tr>
  <tr>
    <td>tax_included</td>
    <td>bool</td>
    <td>Включен ли налог</td>
  </tr>
  <tr>
    <td>tax_active</td>
    <td>bool</td>
    <td>?</td>
  </tr>
  <tr>
    <td>sales_manager</td>
    <td>int</td>
    <td>Идентификатор пользователя менеджера по продаже</td>
  </tr>
  <tr>
    <td>created_at</td>
    <td>int</td>
    <td>Время, когда был создан документ</td>
  </tr>
  <tr>
    <td>updated_at</td>
    <td>int</td>
    <td>Время, когда был обновлен документ</td>
  </tr>
</table>
<b>Пример ответа</b><br/>
<code>

&emsp;{
&emsp;&emsp;"id": 0,
&emsp;&emsp;"number": "string",
&emsp;&emsp;"dated": 0,
&emsp;&emsp;"operation": "string",
&emsp;&emsp;"docs_sales": 0,
&emsp;&emsp;"comment": "string",
&emsp;&emsp;"client": 0,
&emsp;&emsp;"contragent": 0,
&emsp;&emsp;"contract": 0,
&emsp;&emsp;"organization": 0,
&emsp;&emsp;"warehouse": 0,
&emsp;&emsp;"sum": 0,
&emsp;&emsp;"tax_included": true,
&emsp;&emsp;"tax_active": true,
&emsp;&emsp;"sales_manager": 0,
&emsp;&emsp;"updated_at": 0,
&emsp;&emsp;"created_at": 0
&emsp;}

</code>


