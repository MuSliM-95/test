import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddContractsModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddContracts,
  }))
);

export default function AddContracts() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить контракт
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddContractsModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
