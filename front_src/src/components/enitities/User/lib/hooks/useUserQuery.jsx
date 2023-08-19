import { useQuery } from "react-query";
import axios from "axios";

export const useFetchGetUsers = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["users", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/cashbox_users`,
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
