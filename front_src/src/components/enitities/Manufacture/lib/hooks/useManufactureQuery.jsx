import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "../../../../../hooks";

export const useFetchGetManufactures = (options) => {
  const { token, name } = options;
  const query = useQuery(
    ["manufacturers", token, name],
    async () => {
      const params = { token, name };
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/manufacturers/`,
        { params }
      );
      return response.data.result;
    },
    {
      refetchOnWindowFocus: false,
    }
  );
  return query;
};

export const useFetchAllManufacturers = ({ token }) =>
  useFetchAllData({ token, key: "manufacturers", path: "manufacturers/" });
