import axios from "axios";
import { getStrapiError } from "../lib/getStrapiError";

export const axiosInstance = axios.create({
  baseURL: `https://${process.env.REACT_APP_APP_URL}/api/v1`,
  headers: { "Content-Type": "application/json" },
});

axiosInstance.interceptors.response.use(
  null,
  (err) => Promise.reject(getStrapiError(err) || err)
);
