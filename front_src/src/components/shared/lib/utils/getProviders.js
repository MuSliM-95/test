"use client";
import React from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import { getQueryClient } from "./getQueryClient";

export const Providers = ({ children }) => {
  const [client] = React.useState(getQueryClient());
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
};
