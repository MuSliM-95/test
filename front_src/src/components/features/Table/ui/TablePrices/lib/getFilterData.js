import dayjs from 'dayjs';
import { API, paramsToString } from 'src/components/shared';

export const getFilterData = async (filters, pathname, params) => {
  const { token, ...restParams } = params;
  const queryPrices = API.crud.get(token, pathname);
  const filterParams = {
    ...restParams,
    unit: filters.unit_name?.join(),
    category: filters.category_name?.join(),
    price_type_id: filters.price_type?.join(),
    date_to: dayjs(filters.date_to).unix() || undefined,
  };
  const paramsString = paramsToString(filterParams);
  window.history.pushState({}, "", `${pathname}?token=${token}${paramsString}`);
  const newData = await queryPrices(undefined, filterParams);
  return newData;
};