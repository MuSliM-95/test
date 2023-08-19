import { useQuery, useMutation } from "react-query";
import axios from "axios";

export const useFetchGetNomenclature = (options) => {
  const { token, name, limit = 100000 } = options;
  const query = useQuery(
    ["nomenclature", token, name],
    async () => {
      const params = { token, name, limit };
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/`,
        { params }
      );
      return response.data?.sort((a, b) => a.id - b.id);
    },
    {
      refetchOnWindowFocus: false,
    }
  );
  return query;
};

export const useFetchCreateNomenclature = (token, onSuccess) => {
  const { mutate, isLoading, isError, error, isSuccess, data, mutateAsync } =
    useMutation({
      mutationFn: async (options) => {
        return await axios.post(
          `https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/?token=${token}`,
          options
        );
      },
    });
  return [mutate, { isLoading, isError, error, isSuccess, data, mutateAsync }];
};

export const useFetchEditNomenclature = (token) => {
  const { mutate, isLoading, isError } = useMutation(
    async (options) =>
      await axios.patch(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/?token=${token}`,
        options
      )
  );
  return [mutate, { isLoading, isError }];
};

export const useFetchDeleteNomenclature = (token) => {
  const { mutate, isLoading, isError, isSuccess } = useMutation(
    async (ids) =>
      await axios.delete(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/nomenclature/?token=${token}`,
        { data: ids }
      )
  );
  return [mutate, { isLoading, isError, isSuccess }];
};
