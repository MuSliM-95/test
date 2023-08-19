import React, { useState, useRef, useEffect, useContext, useMemo } from "react";
import { TableContext } from "src/components/shared/lib/hooks/context/getTableContext";
import { DatePicker, Form, InputNumber } from "antd";
import { serializeTimestamp } from "src/components/enitities/Form";
import dayjs from "dayjs";

export const EditableCell = ({
  editable,
  children,
  dataIndex,
  index,
  record,
  handleEdit,
  ...restProps
}) => {
  const [editing, setEditing] = useState(false);
  const inputRef = useRef(null);
  const form = useContext(TableContext);
  const editableCell = useRef(null);
  let childNode = children;
  useEffect(() => {
    if (editing) inputRef.current.focus();
  }, [editing]);

  const toggleEdit = (keys, values) => {
    for (let key of keys) {
      form.setFieldsValue({ [key]: values[key] });
    }
    setEditing(false);
  };

  const compareValues = (values, record) => {
    const changedField = Object.keys(values).filter(
      (key) => values[key] !== record[key]
    );
    return changedField;
  };

  const checkDataField = (values) => {
    const dateField = Object.keys(values).filter((key) =>
      dayjs(values[key], "YYYY-MM-DD", true).isValid()
    );
    return dateField;
  };

  const save = async () => {
    try {
      let values = await form.validateFields();
      const changedField = compareValues(values, record);
      if ((changedField || []).length) {
        toggleEdit(changedField, values);
        const dateField = checkDataField({ ...record, ...values });
        if ((dateField || []).length) {
          const newValues = serializeTimestamp(values, [...dateField]);
          handleEdit({ ...record, ...newValues });
          return newValues;
        }
        handleEdit({ ...record, ...values });
      } else {
        setEditing(false);
      }
    } catch (errInfo) {
      console.log("Save failed:", errInfo);
    }
  };

  useMemo(() => {
    switch (dataIndex) {
      case "max_percentage":
      case "max_withdraw_percentage":
      case "minimal_checque_amount":
      case "cashback_percent":
        editableCell.current = (
          <Form.Item
            style={{ margin: 0 }}
            name={dataIndex}
            initialValue={record[dataIndex]}
          >
            <InputNumber
              ref={inputRef}
              onBlur={save}
              style={{ width: "100%" }}
            />
          </Form.Item>
        );
        break;
      case "start_period":
      case "end_period":
        editableCell.current = (
          <Form.Item
            style={{ margin: 0 }}
            name={dataIndex}
            initialValue={record[dataIndex]}
          >
            <DatePicker
              ref={inputRef}
              open={true}
              onBlur={save}
              style={{ width: "100%" }}
            />
          </Form.Item>
        );
        break;
      default:
        break;
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataIndex]);

  if (editable) {
    childNode = editing ? (
      editableCell.current
    ) : (
      <div
        className="editable-cell-value-wrap"
        onClick={() => setEditing(!editing)}
      >
        {children}
      </div>
    );
  }
  return <td {...restProps}>{childNode}</td>;
};
