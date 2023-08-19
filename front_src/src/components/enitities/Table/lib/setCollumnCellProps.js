export const setColumnCellProps = (columns, props) => {
  const newColumns = [...columns];
  for (let [key, value] of Object.entries(props)) {
    for (let item of value) {
      const findCell = newColumns.findIndex((item) => item.dataIndex === key);
      if (newColumns[findCell]?.editable && item.key === "onCell") {
        newColumns[findCell] = {
          ...newColumns[findCell],
          [item.key]: item.action(newColumns[findCell]),
        };
      } else {
        newColumns[findCell] = {
          ...newColumns[findCell],
          [item.key]: item.action,
        };
      }
    }
  }
  return newColumns;
};
