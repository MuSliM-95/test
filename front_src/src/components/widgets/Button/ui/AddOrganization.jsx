import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddOrganizations = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddOrganizations,
  }))
);

export default function AddNomenclatureButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить организацию
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddOrganizations isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
