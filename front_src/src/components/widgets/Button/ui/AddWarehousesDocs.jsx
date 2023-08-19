import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddWarehousesDocsModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddWarehousesDocs,
  }))
);

export default function AddWarehousesDocsButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить документ
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddWarehousesDocsModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
