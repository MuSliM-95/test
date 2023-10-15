import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "../../../../../hooks";

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
      return response.data.result;
    },
    {
      refetchOnWindowFocus: false,
    }
  );
  return query;
};


export const useFetchAllOrganization = ({ token }) =>
  useFetchAllData({ token, key: "organization", path: "organizations/" });
