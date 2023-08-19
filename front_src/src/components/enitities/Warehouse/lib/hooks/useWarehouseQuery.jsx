import { useQuery } from "react-query";
import axios from "axios";

export const useFetchGetWarehouses = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["warehouses", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/warehouses/`,
        { params }
      );
      return response.data;
    }
  );
  return query;
};