import { useQuery } from "react-query";
import axios from "axios";

export const useFetchAllData = ({ token, key, path }) => {
  const query = useQuery({
    queryKey: [key],
    queryFn: async () => {
      const limit = 500;
      let offset = 0;
      const url = `https://${process.env.REACT_APP_APP_URL}/api/v1/${path}/`;
      const response = await axios.get(url, {
        params: { token, limit, offset },
      });
      const count = response?.data?.count || 0;
      let result = response?.data?.result || [];
      let pageCount = count < limit ? 0 : Math.floor(count / limit);
      if (key === "nomenclature") console.log("pageCount", pageCount);
      while (pageCount) {
        offset = offset + limit;
        const response = await axios.get(url, {
          params: { token, limit, offset },
        });
        result = result.concat(response?.data?.result || []);
        if (key === "nomenclature") console.log("pageCount", pageCount);
        --pageCount;
      }
      return result;
    },
  });
  return query;
};
