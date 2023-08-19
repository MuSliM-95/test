import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddPriceModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddPrice,
  }))
);

export default function AddPriceButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить цену на товар
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddPriceModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
