import React, { useState, useRef, useEffect, useContext } from "react";
import { Form, Input, Select } from "antd";
import { TableContext } from "../../lib/hooks/context/getTableContext";

export const EditableCell = ({
  title,
  editable,
  formContext,
  children,
  dataIndex,
  index,
  options,
  record,
  handleEdit,
  ...restProps
}) => {
  const [editing, setEditing] = useState(false);
  const inputRef = useRef(null);
  const form = useContext(TableContext);
  let childNode = children;

  useEffect(() => {
    if (editing) inputRef.current.focus();
  }, [editing]);

  const toggleEdit = () => {
    setEditing(!editing);
    form.setFieldsValue({ [dataIndex]: record[dataIndex] });
  };

  const save = async () => {
    try {
      const values = await form.validateFields();
      toggleEdit();
      handleEdit({ id: record.id, ...values }, index);
    } catch (errInfo) {
      console.log("Save failed:", errInfo);
    }
  };

  if (editable) {
    childNode = editing ? (
      <Form.Item style={{ margin: 0 }} name={dataIndex}>
        {(options || []).length !== 0 ? (
          <Select
            ref={inputRef}
            options={options}
            onPressEnter={save}
            onBlur={save}
          />
        ) : (
          <Input ref={inputRef} onBlur={save} />
        )}
      </Form.Item>
    ) : (
      <div className="editable-cell-value-wrap" onClick={toggleEdit}>
        {children}
      </div>
    );
  }
  return <td {...restProps}>{childNode}</td>;
};
