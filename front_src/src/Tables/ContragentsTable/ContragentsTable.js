import React from 'react';
// import moment from 'moment';
import { HotTable, HotColumn } from '@handsontable/react';
import { registerAllModules } from 'handsontable/registry';
import { Button, Popconfirm } from 'antd';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';

import { TABLE_COLUMNS } from '../../constants/contragents';

import 'handsontable/dist/handsontable.full.min.css';
import './ContragentsTable.css';

registerAllModules();

const StatusRenderer = ({ value, TD }) => {
  const color = value === '200' ? '#FF4136' : '#2ECC40';
  TD.className = 'contragents-table__td htCenter htMiddle';

  return (
    <span style={{ color }}>{value}</span>
  );
};


const ActionRenderer = (props) => {
  props.TD.className = 'contragents-table__td htCenter htMiddle';

  return (
    <>
      <Popconfirm title="Подтвердите удаление"
          cancelText="Отмена"
          okText="OK"
      >
          <Button icon={<DeleteOutlined />} style={{ marginRight: 10 }} />
      </Popconfirm>
      <Button style={{ marginRight: 10 }} icon={<EditOutlined />} />
    </>
  );
};


const ContragentsTable = ({ dataSource }) => {
  return (
    <HotTable
      className={'contragents-table__td htCenter htMiddle'}
      height={'auto'}
      width={'100%'}
      rowHeights={'65px'}
      columnHeaderHeight={'65px'}
      data={dataSource}
      licenseKey="non-commercial-and-evaluation"
      autoRowSize={true}
      autoColumnSize={true}
      stretchH="all"
      selectionMode="single"
      readOnly={true} 
    >
        {TABLE_COLUMNS.map((col, i) => {
          if (col.data === 'response_code') {
            return (
              <HotColumn settings={col} key={`${col.title}-${i}`}>
                <StatusRenderer hot-renderer/>
              </HotColumn>
            );
          } else if (col.data === 'action') {
            return (
              <HotColumn settings={col} key={`${col.title}-${i}`}>
                <ActionRenderer hot-renderer/>
              </HotColumn>
            );
          } else {
            return <HotColumn settings={col} key={`${col.title}-${i}`}/>;
          }
        })}
    </HotTable>
  );
};

export default ContragentsTable;
