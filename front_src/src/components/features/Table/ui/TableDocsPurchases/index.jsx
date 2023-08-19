import { useContext, useMemo, useEffect, useState } from "react";
import { Table, Typography, notification } from "antd";
import { useFetchDeletePurchase } from "../../../../enitities/Purchase/lib/hooks/usePurchaseQuery";
import { EditDocsPurchasesModal } from "../../../../features/Modal";
import { getColumnsTable, useGetDataTable } from "./utils/table";
import { DocsPurchasesContext } from "../../../../shared/lib/hooks/context/getDocsPurchasesContext";

const pageSizeOptions = [10, 50, 100];

const { Title } = Typography;

export default function TableDocsPurchases() {
  const [editModalId, setEditModalId] = useState(null);
  const [api, contextHolder] = notification.useNotification();
  const { token } = useContext(DocsPurchasesContext);

  const [current, setCurrent] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const [
    mutateDeletePurchase,
    { isLoading: isLoadingDelete, isError: isErrorDelete, error: errorDelete },
  ] = useFetchDeletePurchase(token);

  const handlePagination = (page, pageSize) => {
    setCurrent(page);
    setPageSize(pageSize);
  };

  const columns = useMemo(
    () => getColumnsTable({ setEditModalId, mutateDeletePurchase }),
    [mutateDeletePurchase, setEditModalId]
  );

  const { data, total, isLoading, isErrorPurchases, isErrorDirectory, errors } =
    useGetDataTable({ token, current, pageSize });

  useEffect(() => {
    if (isErrorPurchases || isErrorDirectory)
      errors?.forEach((error) => {
        const errorId = Date.now() + Math.floor(Math.random() * 1000);
        api.open({
          key: errorId,
          message: "Ошибка загрузки",
          description: error?.message,
        });
      });
  }, [isErrorPurchases, errors, isErrorDirectory, api]);

  useEffect(() => {
    if (isErrorDelete)
      api.open({
        key: "delete",
        message: "Ошибка удаления",
        description: errorDelete?.message,
      });
  }, [api, errorDelete?.message, isErrorDelete]);

  if (isErrorPurchases) return <div> Ошибка загрузки закупок </div>;
  if (isErrorDelete) return <div>Ошибка удаления</div>;

  return (
    <>
      <Title level={3}> Закупки </Title>
      <Table
        columns={columns}
        dataSource={data}
        pagination={{
          showSizeChanger: true,
          current,
          pageSize: Number(pageSize),
          total,
          pageSizeOptions,
          onChange: handlePagination,
        }}
        loading={isLoading || isLoadingDelete}
      />
      <EditDocsPurchasesModal
        editModalId={editModalId}
        setEditModalId={setEditModalId}
      />
      {contextHolder}
    </>
  );
}
