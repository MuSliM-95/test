import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddWarehousesModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddWarehouses,
  }))
);

export default function AddWarehousesButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить магазин/склад/участок
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddWarehousesModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
