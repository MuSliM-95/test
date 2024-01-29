import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddNomenclatureModal = lazy(() =>
  import("src/features/Modal").then((modal) => ({
    default: modal.AddNomenclatureModal,
  }))
);

export default function AddNomenclatureButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)}>
        Добавить номенклатуру
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddNomenclatureModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
