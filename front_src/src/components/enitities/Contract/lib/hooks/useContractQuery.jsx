import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "../../../../../hooks";

export const useFetchGetContracts = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["contracts", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/contracts/`,
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

export const useFetchAllContracts = ({ token }) =>
  useFetchAllData({ token, key: "contracts", path: "contracts/" });

