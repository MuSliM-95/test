import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddCategoryModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddCategoryModal,
  }))
);

export default function AddRowButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить категорию
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddCategoryModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
