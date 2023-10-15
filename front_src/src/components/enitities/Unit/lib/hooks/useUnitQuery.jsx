import { useQuery } from "react-query";
import axios from "axios";
import { useFetchAllData } from "../../../../../hooks";

export const useFetchGetUnits = ({ limit, offset }) => {
  const query = useQuery(
    ["units"],
    async () => {
      const params = { offset, limit };
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/units/`,
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

export const useFetchAllUnits = () =>
  useFetchAllData({ key: "units", path: "units/" });
