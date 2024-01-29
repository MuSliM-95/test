import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "src/shared/lib/hooks/context";

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
      return response.data.result;
    }
  );
  return query;
};


export const useFetchAllWarehouses = ({ token }) =>
  useFetchAllData({ token, key: "warehouses", path: "warehouses/" });
