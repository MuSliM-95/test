// страница "Категории"
import React from "react";
import AddRowButton from "src/widgets/Button/ui/AddRowButton";
import TableCategories from "src/features/Table/ui/TableCategories";
import { CategoriesContext } from "src/shared/lib/hooks/context/getCategoriesContext";
import { useLocation } from "react-router-dom";

export default function TableCategoriesPage({ token, websocket, initialData }) {
  const { pathname } = useLocation();
  return (
    <>
      <CategoriesContext.Provider value={{ token, initialData, pathname, websocket }} >
        <AddRowButton />
        <TableCategories />
      </CategoriesContext.Provider>
    </>
  );
}
