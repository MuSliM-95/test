import React, {
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useState,
  useMemo
} from "react";
import axios from "axios";
import { NomenclatureContext } from "src/shared/lib/hooks/context/getNomenclatureContext";
import { Nomenclature } from "src/enitities/Table/";
import { saveRow, removeRow, addRow } from "src/shared";
import { API } from "src/shared/api/api";
import { useLocation } from "react-router-dom";
import useDebounce from "src/shared/lib/hooks/context/useDebounce";
import { Space, Input } from 'antd'
import { AddNomenclatureButton } from "src/widgets/Button";

export default function TableNomenclature() {
  const { token, websocket, initialData, nomenclatureDataCount } = useContext(NomenclatureContext);


  const { pathname } = useLocation();
  const [dataSource, setDataSource] = useState(initialData);
  const [total, setTotal] = useState(nomenclatureDataCount)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)

  const [search, setSearch] = useState('')

  const debauncedSearch = useDebounce(search, 400)

  const queryOffsetData = (page, pageSize, name) => {
    axios
      .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/`, {
        params: {
          token: token,
          offset: page * pageSize - pageSize,
          limit: pageSize,
          ...(name ? { name, } : {})
        },
      })
      .then((res) => {
        setDataSource(res.data.result);
        setTotal(res.data.count)
        setPage(page)
        setPageSize(pageSize)
        return res.data;
      });
  };

  useEffect(() => {
    const response = async () => {
      await queryOffsetData(1, 10, debauncedSearch)
    }

    response()

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debauncedSearch])

  const handleSaveImage = (picture) => {
    const newData = JSON.parse(JSON.stringify(dataSource));
    const index = dataSource.findIndex((item) => item.id === picture.entity_id);
    if ((newData[index].pictures || []).length !== 0) {
      const dubData = newData[index].pictures.filter((item) => item.id === picture.id);
      if (dubData.length === 0) newData[index].pictures.push(picture);
    } else {
      newData[index].pictures = [];
      newData[index].pictures.push(picture);
    }
    setDataSource(newData);
  };

  const handleDeleteImage = async (id) => {
    const newData = dataSource.map((item) => {
      const newItem = JSON.parse(JSON.stringify(item));
      const index = newItem.pictures.findIndex((item) => item.id === id);
      if (index !== -1) {
        newItem.pictures.splice(index, 1);
      }
      return newItem;
    });
    setDataSource(newData);
  };

  // TODO: GO TO FOLDER OF MODEL;
  const queryPictures = useCallback(async () => {
    const url = [];
    const request = [];
    for (let item of dataSource) {
      url.push(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/pictures/?token=${token}&entity=nomenclature&entity_id=${item.id}`
      );
    }
    request.push(...url.map((url) => axios.get(url)));
    const newData = await axios.all(request).then((response) => {
      const bubble = dataSource.map((data) => {
        const newData = JSON.parse(JSON.stringify(data));
        const item = response.filter(
          (item) => item?.data[0]?.entity_id === data.id
        );
        newData.pictures = item[0]?.data || [];
        return newData;
      });
      return bubble;
    });
    return newData;
  }, [dataSource, token]);

  const picturesData = async () => {
    const newData = await queryPictures();
    setDataSource(newData);
  };

  useLayoutEffect(() => {
    picturesData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token]);

  useEffect(() => {
    websocket.onmessage = (message) => {
      const data = JSON.parse(message.data);
      if (data.target === "nomenclature") {
        if (data.action === "create") {
          addRow(dataSource, data.result, setDataSource);
        }
        if (data.action === "edit") {
          saveRow(dataSource, data.result, setDataSource);
        }
        if (data.action === "delete") {
          removeRow(dataSource, data.result.id, setDataSource);
        }
      }
      if (data.target === "pictures") {
        if (data.action === "create") {
          handleSaveImage(data.result);
        }
        if (data.action === "delete") {
          handleDeleteImage(data.result.id);
        }
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, dataSource]);

  return (
    <>
      <Space style={{ marginBottom: 15 }}>
        <AddNomenclatureButton />
        <Input placeholder="Поиск" value={search} onChange={e => setSearch(e.target.value)} />
      </Space>
      {useMemo(() => (
        <Nomenclature
          page={page}
          dataSource={dataSource}
          handleSave={API.crud.edit(token, pathname)}
          handleRemove={API.crud.remove(token, pathname)}
          handleSaveImage={handleSaveImage}
          handleDeleteImage={API.pictures.removeImage(token)}
          queryOffsetData={queryOffsetData}
          total={total}
          search={search}
          pageSize={pageSize}
        />
        // eslint-disable-next-line react-hooks/exhaustive-deps
      ), [dataSource, page, API, total, pageSize])}
    </>
  );
}
