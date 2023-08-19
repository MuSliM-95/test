import React from "react";
import { Form } from "antd";
import { TableContext } from "../../lib/hooks/context/getTableContext";

export const EditableRow = ({ index, ...props }) => {
  const [form] = Form.useForm();
  return (
    <Form form={form} component={false}>
      <TableContext.Provider value={form}>
        <tr {...props} />
      </TableContext.Provider>
    </Form>
  );
};
