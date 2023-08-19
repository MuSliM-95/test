import { useQuery, useMutation } from "react-query";
import axios from "axios";

export const useFetchGetPrices = (options) => {
  const { token, name, limit = 100000 } = options;
  const query = useQuery(
    ["prices", token, name],
    async () => {
      const params = { token, name, limit };
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/prices/`,
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

export const useFetchCreatePrice = (token, onSuccess) => {
  const { mutate, isLoading, isError, error } = useMutation(
    async (options) =>
      await axios.post(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/prices/?token=${token}`,
        options
      )
  );
  return [mutate, { isLoading, isError, error }];
};

export const useFetchEditPrice = (token) => {
  const { mutate, isLoading, isError } = useMutation(
    async (options) =>
      await axios.patch(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/prices/?token=${token}`,
        options
      )
  );
  return [mutate, { isLoading, isError }];
};

export const useFetchDeletePrices = (token) => {
  const { mutate, isLoading, isError, isSuccess } = useMutation(
    async (ids) =>
      await axios.delete(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/prices/`,
        {
          params: { token, ids: ids.join(",") },
        }
      )
  );
  return [mutate, { isLoading, isError, isSuccess }];
};
