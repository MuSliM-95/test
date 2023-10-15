import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "../../../../../hooks";

export const useFetchGetCategories = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["categories", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/categories/`,
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

export const useFetchAllCategories = ({ token }) =>
  useFetchAllData({ token, key: "categories", path: "categories/" });
