import { useQuery } from "react-query";
import axios from "axios";

export const useFetchGetUnits = () => {
  const query = useQuery(
    ["units",],
    async () => {
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/units/`,
      );
      return response.data;
    },
    {
      refetchOnWindowFocus: false,
    }
  );
  return query;
};

