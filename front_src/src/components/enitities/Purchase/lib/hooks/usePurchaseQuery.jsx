import { useQuery, useMutation, useQueryClient } from "react-query";
import axios from "axios";

export const useFetchGetPurchases = (options) => {
  const { token, pageSize, current } = options;
  const query = useQuery(
    ["purchases", current, pageSize, token],
    async () => {
      const params= { token, limit: pageSize, offset: (current-1) * +pageSize || undefined }
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/`,
        { params }
      );
      return response.data;
    }
  );
  return query;
};

export const useFetchGetPurchaseById = (options) => {
  const { token, id } = options;
  const query = useQuery(
    ["purchase", id, token],
    async () => {
      // const params= { token, limit: pageSize, offset: (current-1) * +pageSize || undefined }
      const params = { token };
      const response = await axios.get(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/${id}`,
        { params }
      );
      return response.data;
    },
    {
      enabled: id !== null&&id !== undefined,
    }
  );
  return query;
};

export const useFetchCreatePurchase = (token, onSuccess) => {
  const queryClient = useQueryClient();
  const { mutate, isLoading, isError,error } = useMutation(
    async (options) =>
      await axios.post(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/?token=${token}`,
        [options]
      ),
    {
      onSuccess: () => {
        if (typeof onSuccess === "function") onSuccess();
        queryClient.invalidateQueries("purchases");
      },
    }
  );
  return [mutate, { isLoading, isError, error }];
};

export const useFetchEditPurchase = (token, onSuccess) => {
  const queryClient = useQueryClient();
  const { mutate, isLoading, isError } = useMutation(
    async (options) =>
      await axios.patch(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/${options?.id}?token=${token}`,
        [options]
      ),
    {
      onSuccess: () => {
        if (typeof onSuccess === "function") onSuccess();
        queryClient.invalidateQueries("purchases");
        queryClient.invalidateQueries("purchase");
      },
    }
  );
  return [mutate, { isLoading, isError }];
};

export const useFetchDeletePurchase = (token) => {
  const queryClient = useQueryClient();
  const { mutate, isLoading, isError } = useMutation(
    async (id) =>
      await axios.delete(
        `https://${process.env.REACT_APP_APP_URL}/api/v1/docs_purchases/${id}?token=${token}`,
        {
          data: [id],
        }
      ),
    {
      onSuccess: () => {
        queryClient.invalidateQueries("purchases");
        queryClient.invalidateQueries("purchase");
      },
    }
  );
  return [mutate, { isLoading, isError }];
};