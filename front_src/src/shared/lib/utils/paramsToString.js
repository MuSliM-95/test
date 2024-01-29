  export const paramsToString = (params) => {
    let string = "";
    for (let [key, value] of Object.entries(params)) {
      if (value) string += `&${key}=${value}`;
    }
    return string;
  };
