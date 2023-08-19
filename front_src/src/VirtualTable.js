import { Table, theme } from 'antd';
import classNames from 'classnames';
import ResizeObserver from 'rc-resize-observer';
import { useEffect, useRef, useState } from 'react';
import { VariableSizeGrid as Grid } from 'react-window';

import './Payments.css';

const VirtualTable = (props) => {
    const { columns, scroll } = props;
    const [tableWidth, setTableWidth] = useState(0);
    const { token } = theme.useToken();
    const widthColumnCount = columns.filter(({ width }) => !width).length;
    // console.log(Math.floor(tableWidth / widthColumnCount))
    const mergedColumns = columns.map((column) => {
        if (column.width) {
            return column;
        }
        return {
            ...column,
            width: Math.floor(tableWidth / widthColumnCount),
        };
    });
    const gridRef = useRef();
    const [connectObject] = useState(() => {
        const obj = {};
        Object.defineProperty(obj, 'scrollLeft', {
            get: () => {
                if (gridRef.current) {
                    return gridRef.current?.state?.scrollLeft;
                }
                return null;
            },
            set: (scrollLeft) => {
                if (gridRef.current) {
                    gridRef.current.scrollTo({
                        scrollLeft,
                    });
                }
            },
        });
        return obj;
    });
    const resetVirtualGrid = () => {
        gridRef.current?.resetAfterIndices({
            columnIndex: 0,
            shouldForceUpdate: true,
        });
    };
    useEffect(() => resetVirtualGrid, [tableWidth]);
    const renderVirtualList = (rawData, { scrollbarSize, ref, onScroll }) => {
        ref.current = connectObject;
        const totalHeight = rawData.length * 54;
        // console.log(totalHeight)
        return (
            <Grid
                ref={gridRef}
                className="virtual-grid"
                columnCount={mergedColumns.length}
                columnWidth={(index) => {
                    const { width } = mergedColumns[index];
                    return totalHeight > scroll.y && index === mergedColumns.length - 1
                        ? width - scrollbarSize - 1
                        : width;
                }}
                height={scroll.y}
                rowCount={rawData.length}
                rowHeight={() => 54}
                width={tableWidth}
                onScroll={({ scrollLeft }) => {
                    onScroll({
                        scrollLeft,
                    });
                }}
            >
                {({ columnIndex, rowIndex, style }) => (
                    <div
                        className={classNames('virtual-table-cell', {
                            'virtual-table-cell-last': columnIndex === mergedColumns.length - 1,
                        })}
                        style={{
                            ...style,
                            boxSizing: 'border-box',
                            padding: token.padding,
                            borderBottom: `${token.lineWidth}px ${token.lineType} ${token.colorSplit}`,
                            background: token.colorBgContainer,
                        }}
                    >
                        {rawData[rowIndex][mergedColumns[columnIndex].dataIndex]}
                    </div>
                )}
            </Grid>
        );
    };
    return (
        <ResizeObserver
            onResize={({ width }) => {
                setTableWidth(width);
            }}
        >
            <Table
                {...props}
                className="virtual-table"
                columns={mergedColumns}
                pagination={props.pagination}
                components={{
                    body: { ...renderVirtualList, ...props.components.body },
                }}
            />
        </ResizeObserver>
    );
};

export default VirtualTable;