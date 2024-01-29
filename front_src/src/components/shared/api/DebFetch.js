import { Select, Spin } from 'antd';
import debounce from 'lodash/debounce';
import React, { useMemo, useRef, useState } from 'react';

function DebounceSelect({ fetchOptions, debounceTimeout = 800, ...props }) {
    const [options, setOptions] = useState([]);
    const [fetching, setFetching] = useState(false);
    const fetchRef = useRef(0);

    // setOptions(props.start_options)
    // console.log(1)

    const debounceFetcher = useMemo(() => {

        fetchOptions().then((res) => { setOptions(res) })


        const loadOptions = (value) => {
            fetchRef.current += 1;
            const fetchId = fetchRef.current;
            setOptions([]);
            setFetching(true);
            fetchOptions(value).then((newOptions) => {
                if (fetchId !== fetchRef.current) {
                    // for fetch callback order
                    return;
                }

                setOptions(newOptions);
                setFetching(false);
            });

        };

        return debounce(loadOptions, debounceTimeout);
    }, [fetchOptions, debounceTimeout]);
    return (
        <Select
            // labelInValue
            showSearch
            allowClear
            filterOption={false}
            onSearch={debounceFetcher}
            notFoundContent={fetching ? <Spin size="small" /> : null}
            {...props}
            options={options}
        />
    );
} // Usage of DebounceSelect


export default DebounceSelect;