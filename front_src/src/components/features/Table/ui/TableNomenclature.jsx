import React, {
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useState,
} from "react";
import axios from "axios";
import { NomenclatureContext } from "../../../shared/lib/hooks/context/getNomenclatureContext";
import { Nomenclature } from "../../../enitities/Table/";
import { saveRow, removeRow, addRow } from "../../../shared";
import { API } from "../../../shared/api/api";
import { useLocation } from "react-router-dom";

export default function TableNomenclature() {
  const { token, websocket, initialData } = useContext(NomenclatureContext);
  const { pathname } = useLocation();
  const [dataSource, setDataSource] = useState(initialData);
  // const queryOffsetData = (page, pageSize) => {
  //   axios
  //     .get(`https://${process.env.REACT_APP_APP_URL}/api/v1/manufacturers/`, {
  //       params: {
  //         token: token,
  //         offset: page * pageSize - pageSize,
  //         limit: pageSize,
  //       },
  //     })
  //     .then((res) => {
  //       setDataSource(res.data);
  //       return res.data;
  //     });
  // };
  
  const handleSaveImage = (picture) => {
    const newData = JSON.parse(JSON.stringify(dataSource));
    const index = dataSource.findIndex((item) => item.id === picture.entity_id);
    const dubData = newData[index].pictures.filter(
      (item) => item.id === picture.id
    );
    if (dubData.length === 0) {
      newData[index].pictures.push(picture);
      setDataSource(newData);
    }
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
        if (data.action === 'create') {
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
      <Nomenclature
        dataSource={dataSource}
        handleSave={API.crud.edit(token, pathname)}
        handleRemove={API.crud.remove(token, pathname)}
        handleSaveImage={handleSaveImage}
        handleDeleteImage={API.pictures.removeImage(token)}
        // queryOffsetData={queryOffsetData}
      />
    </>
  );
}
