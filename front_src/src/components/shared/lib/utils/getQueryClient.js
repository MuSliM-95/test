import { cache } from "react";
import { QueryClient } from "@tanstack/react-query";

export const getQueryClient = cache(
  () =>
    new QueryClient({
      defaultOptions: {
        queries: {
          cacheTime: 1000 * 60 * 60 * 24,
          staleTime: Infinity,
        },
      },
    })
);
