import { Form, Input, Select, DatePicker } from "antd";
import locale from 'antd/lib/date-picker/locale/ru_RU'

export default function FormItem({ data }) {
    const { name, label, isRequired, isSelector, options, isDate, isNumber, isDisabled } = data;
    return <Form.Item
        label={label}
        name={name}
        labelCol={{ span: 24 }}
        style={{ marginBottom: 0 }}

        rules={[
            {
                required: !!isRequired,
                message: "Обязательное поле!",

            },
        ]}
    >
        {
            isSelector ? <Select
                showArrow={false}
                optionFilterProp="label"
                allowClear
                showSearch
                style={{ width: 180, marginRight: 18, }}
                disabled={isDisabled}
                options={options}
            /> : isDate ?
                <DatePicker style={{ width: '180px', marginRight: 18, }} disabled={isDisabled} format="YYYY-MM-DD" placeholder={"Выберите дату"} locale={locale} />
                :
                <Input style={{ width: '180px', marginRight: 18, }} type={isDate ? "date" : isNumber ? "number" : undefined} disabled={isDisabled} />
        }
    </Form.Item>
}