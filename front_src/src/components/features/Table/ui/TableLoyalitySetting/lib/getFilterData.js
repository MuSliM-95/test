// import dayjs from 'dayjs';
import { API, paramsToString } from 'src/components/shared';

export const getFilterData = async (filters, pathname, params) => {
  const { token, ...restParams } = params;
  const queryPrices = API.crud.get(token, pathname);
  const filterParams = {
    ...restParams,
    organization_id: filters.organization?.join(),
    // end_period: dayjs(filters.end_period).unix() || undefined,
    // start_period: dayjs(filters.start_period).unix() || undefined,
  };
  const paramsString = paramsToString(filterParams);
  window.history.pushState({}, "", `${pathname}?token=${token}${paramsString}`);
  const newData = await queryPrices(undefined, filterParams);
  return newData;
};