import { message } from "antd";

import { API } from "src/components/shared";
import { paramsToString } from "src/components/shared";
import dayjs from "dayjs";

export const getFilterData = async (filters, pathname, params) => {
  const { token, ...restParams } = params;
  const queryParams = {};
  const getQueryParams = API.crud.get(token, pathname);
  if (filters.first_name) {
    const user_id = filters.first_name.join();
    Object.assign(queryParams, { user_id });
  }
  if (filters.date) {
    const date = [...filters.date[0]];
    const date_from = dayjs(date?.[0]).unix() || undefined;
    const date_to = dayjs(date?.[1]).unix() || undefined;
    Object.assign(queryParams, { date_from, date_to });
  }
  if (filters.tags) {
    const tags = filters.tags.join();
    Object.assign(queryParams, { tags });
  }
  const filterParams = { ...restParams, ...queryParams };
  const paramsString = paramsToString(filterParams);
  window.history.pushState({}, "", `${pathname}?token=${token}${paramsString}`);

  try {
    const newData = await getQueryParams(undefined, filterParams);
    return newData;
  } catch (e) {
    if (e.status === 404) {
      switch (e.message.detail) {
        case "Not Found": {
          message.info("По заданным параметрам ничего не найдено");
          break;
        }
        default:
          break;
      }
    } else
      message.info("Для отображения данных укажите дату в периоде от и до");

    return [];
  }
};
