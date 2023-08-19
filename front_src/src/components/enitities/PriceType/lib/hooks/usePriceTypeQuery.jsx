import { useQuery } from "react-query";
import axios from "axios";

export const useFetchGetPriceTypes = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["priceTypes", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/price_types/`,
        { params }
      );
      return response.data;
    },
    {
      refetchOnWindowFocus: false,
    }
  );
  return query;
};