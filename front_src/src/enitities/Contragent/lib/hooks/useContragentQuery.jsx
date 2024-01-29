import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "src/shared/lib/hooks/context";

export const useFetchGetContragents = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["contragents", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/contragents/`,
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

export const useFetchAllContragents = ({ token }) =>
  useFetchAllData({ token, key: "contragents", path: "contragents/" });
