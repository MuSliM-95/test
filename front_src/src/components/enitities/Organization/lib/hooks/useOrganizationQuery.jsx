import { useQuery } from "react-query";
import axios from "axios";

export const useFetchGetOrganization = (options) => {
  const { token, name, } = options;
  const query = useQuery(
    ["organization", token, name],
    async () => {
      const params = { token, name }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/organizations/`,
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
