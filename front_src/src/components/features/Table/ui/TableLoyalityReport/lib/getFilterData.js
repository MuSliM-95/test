import { message } from "antd";
import { currentMonthRange } from "src/components/shared";

import { API } from "src/components/shared";
import { paramsToString } from "src/components/shared";
import dayjs from "dayjs";

export const getFilterData = async (filters, pathname) => {
  const queryParams = {};
  const urlSearchParams = new URLSearchParams(window.location.search);
  const params = Object.fromEntries(urlSearchParams.entries());
  const { token, ...restParams } = params;
  const getQueryParams = API.crud.get(token, pathname);

  if (filters.user_id) {
    Object.assign(queryParams, { user_id: filters.user_id });
  }

  if (filters.date) {
    const date = [...filters.date];
    const date_from = dayjs(date?.[0]).unix() || undefined;
    const date_to = dayjs(date?.[1]).unix() || undefined;
    Object.assign(queryParams, { date_from, date_to });
  } else {
    const { date_from, date_to } = currentMonthRange();
    Object.assign(queryParams, { date_from, date_to });
  }

  if (filters.tags) {
    const tags = filters.tags.join();
    Object.assign(queryParams, { tags });
  }

  const filterParams = { ...restParams, ...queryParams };
  const paramsString = paramsToString(filterParams);
  window.history.pushState(
    {path: `${window.location.protocol}//${window.location.host}${pathname}?token=${token}${paramsString}`},
    "",
    `${pathname}?token=${token}${paramsString}`
  );

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
