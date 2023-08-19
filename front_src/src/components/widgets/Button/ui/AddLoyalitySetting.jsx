import React, { lazy, Suspense, useState } from "react";
import { Button } from "antd";
const AddLoyalitySettingModal = lazy(() =>
  import("../../../features/Modal").then((modal) => ({
    default: modal.AddLoyalitySetting,
  }))
);

export default function AddPriceButton() {
  const [isOpen, setOpen] = useState(false);
  return (
    <>
      <Button onClick={() => setOpen(true)} style={{ marginBottom: 15 }}>
        Добавить настройки к карте
      </Button>
      {isOpen ? (
        <Suspense fallback={<div></div>}>
          <AddLoyalitySettingModal isOpen={isOpen} setOpen={setOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
