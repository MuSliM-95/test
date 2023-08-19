import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddDocsPurchasesModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddDocsPutchases,
  }))
);

export default function AddDocsPurchases() {
 const [isOpen, setOpen] = useState(false);
  return (
    <>
     <Button onClick={() => setOpen(true)}>Добавить</Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddDocsPurchasesModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}