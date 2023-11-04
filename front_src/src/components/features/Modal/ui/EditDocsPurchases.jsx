import { useState, useContext } from "react";
import { Form, Modal } from "antd";
import { useFetchEditPurchase } from "../../../enitities/Purchase/lib/hooks/usePurchaseQuery";

import { DocsPurchasesContext } from "../../../shared/lib/hooks/context/getDocsPurchasesContext";
import { EditDocsPurchasesForm } from "../../../enitities/Form";
import { prepareDataForCreate } from "../../../features/Table/ui/TableDocsPurchases/utils/modal"

export default function EditModal({ editModalId, setEditModalId }) {
  const { token } = useContext(DocsPurchasesContext);
  const [goods, setGoods] = useState([]);


  const [
    mutateEditPurchase,
    { isLoading: isLoadingEdit, isError: isErrorEdit, error: errorEdit },
  ] = useFetchEditPurchase(token, () => {
    resetFields();
    setEditModalId(null);
    setGoods([])
  });

  const [form] = Form.useForm();
  const { resetFields, getFieldsError, getFieldsValue } = form;





  const handleCancel = () => {
    resetFields();
    setEditModalId(null);
    setGoods([])
  };

  const handleOk = async () => {
    await form.submit();
    const errors = getFieldsError();
    if (errors.filter((item) => item?.errors.length)?.length) {
      form.validateFields();
      return;
    }
    const data = getFieldsValue();
    const prepareData = prepareDataForCreate(data, goods);
    await mutateEditPurchase({ ...prepareData, id: editModalId });
    setGoods([])
  };

  return (
    <>
      <Modal
        title={`Редактирование закупки`}
        open={editModalId !== null}
        onOk={handleOk}
        onCancel={handleCancel}
        width={"max-content"}
        okText="Сохранить"
        cancelText="Отмена"
      >
        <EditDocsPurchasesForm
          isLoadingEdit={isLoadingEdit}
          isErrorEdit={isErrorEdit}
          errorEdit={errorEdit}
          editModalId={editModalId}
          form={form}
          token={token}
          goods={goods}
          setGoods={setGoods}
        />
      </Modal>
    </>
  );
};