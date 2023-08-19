export const getStrapiError = (err) => {
  if (!err.response) return;
  const { data, status, statusText } = err.response;

  return {
    status,
    statusText,
    message: data,
  };
};
