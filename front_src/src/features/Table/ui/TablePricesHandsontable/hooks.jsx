import { useState } from "react";

const useVirtualization = () => {
  const [startIndex, setStartIndex] = useState(0); // начальный индекс для отображения
  const [endIndex, setEndIndex] = useState(19); // конечный индекс для отображения

  function rowRenderer(index, element) {
    if (index >= startIndex && index <= endIndex) {
      // Отображаем строку:
      element.style.display = "";
    } else {
      // Скрываем строку:
      element.style.display = "none";
    }
  }

  function handleScroll(e) {
    // Извлекаем значение scrollTop и устанавливаем соответствующие значения startIndex и endIndex:
    const scrollTop = e.target.scrollTop;
    const rowHeight = 23; // высота строки по умолчанию
    const startRowIndex = Math.floor(scrollTop / rowHeight);
    const endRowIndex = startRowIndex + 19;
    setStartIndex(startRowIndex);
    setEndIndex(endRowIndex);
  }
  return { rowRenderer, handleScroll };
};

export { useVirtualization };
