import { useState, useContext } from "react";
import { Form, Modal } from "antd";
import { useFetchCreatePurchase } from "../../../enitities/Purchase/lib/hooks/usePurchaseQuery";

import { DocsPurchasesContext } from "../../../shared/lib/hooks/context/getDocsPurchasesContext";
import { DocsPurchasesModal } from "../../../enitities/Form";
import {prepareDataForCreate} from "../../../features/Table/ui/TableDocsPurchases/utils/modal"

export default function CreateModal({ isOpen, setOpen }){
    const [goods, setGoods] = useState([]);
    const { token } = useContext(DocsPurchasesContext);

    const [mutateCreatePurchase, { isLoading: isLoadingCreate, isError, error }] =
        useFetchCreatePurchase(token, () => {
            resetFields();
            setOpen(false);
            setGoods([])
        });



    const [form] = Form.useForm();
    const { resetFields, getFieldsError, getFieldsValue } = form;

    const handleCancel = () => {
        resetFields();
        setOpen(false);
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
        mutateCreatePurchase(prepareData);
        setGoods([])
    };



    return (
        <>
            <Modal
                title="Cоздание закупки"
                open={isOpen}
                onOk={handleOk}
                onCancel={handleCancel}
                width={"max-content"}
                okText="Сохранить"
                cancelText="Отмена"
            >
                <DocsPurchasesModal
                    isLoadingCreate={isLoadingCreate}
                    isError={isError} error={error}
                    form={form}
                    token={token}
                    goods={goods}
                    setGoods={setGoods}
                    />
            </Modal>
        </>
    );
};