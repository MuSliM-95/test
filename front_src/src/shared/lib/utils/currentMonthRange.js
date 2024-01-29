const firstDayOfMonth = (currentMonth) => {
  const date = new Date();
  date.setMonth(currentMonth, 1);
  return Math.floor(date.getTime() / 1000);
};

const lastDayOfMonth = (currentMonth) => {
  const date = new Date();
  date.setMonth(currentMonth + 1, 0);
  return Math.floor(date.getTime() / 1000);
};

export const currentMonthRange = () => {
  const currentMonth = new Date().getMonth();
  const date_from = firstDayOfMonth(currentMonth);
  const date_to = lastDayOfMonth(currentMonth);
  return { date_from, date_to };
};
