import { useMemo } from "react";
import { Button, } from "antd";
import { EditOutlined } from "@ant-design/icons";

import { useFetchGetPurchases } from "../../../../../enitities/Purchase/lib/hooks/usePurchaseQuery";
import { useFetchGetContracts } from "../../../../../enitities/Contract/lib/hooks/useContractQuery";
import { useFetchGetContragents } from "../../../../../enitities/Contragent/lib/hooks/useContragentQuery";
import { useFetchGetOrganization } from "../../../../../enitities/Organization/lib/hooks/useOrganizationQuery";
import { useFetchGetUsers } from "../../../../../enitities/User/lib/hooks/useUserQuery";
import { useFetchGetWarehouses } from "../../../../../enitities/Warehouse/lib/hooks/useWarehouseQuery";

import { RemoveButton } from "../../../../../widgets/Button";

const getNewGoodsData = ({ data, currentNomenclatureId, currentQuantity }) => {
    const currentNomenclature = data?.nomenclatures?.find((item) => item.id === currentNomenclatureId)

    const currentPrice = data?.prices?.find((price) => price.name === currentNomenclature?.name)
    const cuarrentPriceTypes = data?.priceTypes?.find((item) => item?.name === currentPrice?.price_type)

        return {
            nomenclature: { label: currentNomenclature?.name, value: currentNomenclature?.id },
            price_type: { label: cuarrentPriceTypes?.name, value: cuarrentPriceTypes?.id },
            price: currentPrice?.price,
            quantity: +currentQuantity,
            unit: { value: currentNomenclature?.unit, label: currentNomenclature?.unit_name }
        }
}

const getPrepareTableData = ({ data, contragents, contracts, organizations, warehouses, users }) => {
    return data?.result?.map((item) => ({
        ...item,
        client: contragents[item.client] || item.client,
        contragent: contragents[item.contragent] || item.contragent,
        contract: contracts[item.contract] || item.contract,
        organization: organizations[item.organization] || item.organization,
        warehouse: warehouses[item.warehouse] || item.warehouse,
        purchased_by: users[item.purchased_by] || item.purchased_by,
    })) || [];
}

const convertArrayToObject = (arr) => {
    const obj = {};
    arr?.forEach(({ id, name, short_name, username }) => {
        obj[id] = name || short_name || username;
    });
    return obj;
}

const convertUsersArrayToObject = (arr) => {
    const obj = {};
    arr?.forEach(({ id, username, last_name, first_name, }) => {
        obj[id] = (last_name || first_name) ? `${first_name || ''} ${last_name || ""}`:username;
    });
    return obj;
}

const useGetDataTable = ({ token, current, pageSize }) => {
    const { isLoading, isError, isSuccess, data, error } = useFetchGetPurchases({ token,current, pageSize });
    const { isLoading: isLoadingContragents, isError: isErrorContragents, isSuccess: isSuccessConragents, data: contragents, error: errorContragets } = useFetchGetContragents({ token })
    const { isLoading: isLoadingContracts, isError: isErrorContracts, isSuccess: isSuccessContracts, data: contracts, error: errorContracts } = useFetchGetContracts({ token })
    const { isLoading: isLoadingOrganizations, isError: isErrorOrganizations, isSuccess: isSuccessOrganizations, data: organizations, error: errorOrganizations } = useFetchGetOrganization({ token })
    const { isLoading: isLoadingWarehouses, isError: isErrorWarehouses, isSuccess: isSuccessWarehouses, data: warehouses, error: errorWarehouses } = useFetchGetWarehouses({ token })
    const { isLoading: isLoadingUsers, isError: isErrorUsers, isSuccess: isSuccessUsers, data: users, error: errorUsers } = useFetchGetUsers({ token })

    const prepareData = useMemo(() => {
        if (isSuccess && (isErrorContragents || isSuccessConragents) && (isErrorContracts || isSuccessContracts) && (isErrorOrganizations || isSuccessOrganizations) && (isErrorWarehouses || isSuccessWarehouses) && (isErrorUsers || isSuccessUsers)) {
            const prepareContragents = convertArrayToObject(contragents?.result);
            const prepareContracts = convertArrayToObject(contracts);
            const prepareOrganizations = convertArrayToObject(organizations);
            const prepareWarehouses = convertArrayToObject(warehouses);
            const prepareUsers = convertUsersArrayToObject(users?.result);
            return getPrepareTableData({ data, contragents: prepareContragents, contracts: prepareContracts, organizations: prepareOrganizations, warehouses: prepareWarehouses, users: prepareUsers })
        }
    }, [isSuccess, isErrorContragents, isSuccessConragents, isErrorContracts, isSuccessContracts, isErrorOrganizations, isSuccessOrganizations, isErrorWarehouses, isSuccessWarehouses, contragents, contracts, organizations, warehouses, isErrorUsers, isSuccessUsers, users, data])
    return {
        data: prepareData,
        total:data?.count,
        isLoading: isLoading || isLoadingContragents || isLoadingContracts || isLoadingOrganizations || isLoadingWarehouses || isLoadingUsers,
        isErrorPurchases: isError,
        isErrorDirectory: isErrorContragents || isErrorContracts || isErrorOrganizations || isErrorWarehouses || isErrorUsers,
        errors: [errorContragets, errorContracts, errorOrganizations, errorWarehouses, errorUsers, error].filter(item => item)
    }
}

const getColumnsTable = ({ setEditModalId, mutateDeletePurchase }) => {
    return [
        {
            title: "id",
            dataIndex: "id",
            key: "id",
        },
        {
            title: "Номер",
            dataIndex: "number",
            key: "number",
        },
        {
            title: "От",
            dataIndex: "dated",
            key: "dated",
            render: (date) => {
                return typeof date === "number" ? <span>{new Date(date * 1000).toLocaleDateString("ru-RU")}</span> : null
            },
        },

        {
            title: "Операция",
            dataIndex: "operation",
            key: "operation",
        },
        {
            title: "Комментарий",
            dataIndex: "comment",
            key: "comment",
        },
        {
            title: "Клиент",
            dataIndex: "client",
            key: "client",
        },
        {
            title: "Контрагент",
            dataIndex: "contragent",
            key: "contragent",
        },
        {
            title: "Договор",
            dataIndex: "contract",
            key: "contract",
        },
        {
            title: "Организация",
            dataIndex: "organization",
            key: "organization",
        },
        {
            title: "Склад",
            dataIndex: "warehouse",
            key: "warehouse",
        },
        {
            title: "Сумма",
            dataIndex: "sum",
            key: "sum",
        },
        {
            title: "Кто закупает",
            dataIndex: "purchased_by",
            key: "purchased_by",
        },
        {
            title: "Действия",
            key: "actions",
            render: (_, { id }) => {
                return (
                    <div
                        style={{
                            display: "flex",
                        }}
                    >
                        <Button
                            icon={<EditOutlined />}
                            onClick={() => setEditModalId(id)}
                        />
                        <RemoveButton callback={() => mutateDeletePurchase(id)} />
                    </div>
                );
            },
        },
    ]
}

export { getNewGoodsData, useGetDataTable, getColumnsTable }