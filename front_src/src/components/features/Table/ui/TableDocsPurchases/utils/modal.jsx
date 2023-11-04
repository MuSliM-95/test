import { useMemo } from "react";

import { useFetchGetContracts } from "../../../../../enitities/Contract/lib/hooks/useContractQuery";
import { useFetchGetContragents } from "../../../../../enitities/Contragent/lib/hooks/useContragentQuery";
import { useFetchGetOrganization } from "../../../../../enitities/Organization/lib/hooks/useOrganizationQuery";
import { useFetchGetUsers } from "../../../../../enitities/User/lib/hooks/useUserQuery";
import { useFetchGetWarehouses } from "../../../../../enitities/Warehouse/lib/hooks/useWarehouseQuery";
import {useFetchGetNomenclature} from "../../../../../enitities/Nomenclature/lib/hooks/useNomenclatureQuery";
import {useFetchGetPrices} from "../../../../../enitities/Price/lib/hooks/usePriceQuery";
import {useFetchGetPriceTypes} from "../../../../../enitities/PriceType/lib/hooks/usePriceTypeQuery"

import {prepareForSelector, prepareUsersForSelector} from "./index"

import { useFetchGetPurchaseById } from "../../../../../enitities/Purchase/lib/hooks/usePurchaseQuery";

const prepareDataForCreate = (data,goods) => {
    const newGoods=goods?.map((item)=>({...item,
      nomenclature:item?.nomenclature?.value,
      price_type:item?.price_type?.value,
      unit:item?.unit?.value,
    }))
    const newData = { goods:newGoods  };
    for (let key in data) {
  if (key === "number" || key === "operation" || key === "comment") {
        newData[key] = data[key];
      }else if(key === "dated"){
        const newDated=+new Date(data[key])/1000
        newData[key]=newDated||undefined
      } else newData[key] = Number(data[key]) || undefined;
    }
    return newData;
  };

  const useGetData = ({ token, editModalId }) => {
    const { isLoading: isLoadingContragents, isError: isErrorContragents, isSuccess: isSuccessConragents, data: contragents, error: errorContragets } = useFetchGetContragents({ token })
    const { isLoading: isLoadingContracts, isError: isErrorContracts, isSuccess: isSuccessContracts, data: contracts, error: errorContracts } = useFetchGetContracts({ token })
    const { isLoading: isLoadingOrganizations, isError: isErrorOrganizations, isSuccess: isSuccessOrganizations, data: organizations, error: errorOrganizations } = useFetchGetOrganization({ token })
    const { isLoading: isLoadingWarehouses, isError: isErrorWarehouses, isSuccess: isSuccessWarehouses, data: warehouses, error: errorWarehouses } = useFetchGetWarehouses({ token })
    const { isLoading: isLoadingUsers, isError: isErrorUsers, isSuccess: isSuccessUsers, data: users, error: errorUsers } = useFetchGetUsers({ token })
    const { isLoading: isLoadingNomenclatures, isError: isErrorNomenclatures, isSuccess: isSuccessNomenclatures, data: nomenclatures, error: errorNomenclatures } = useFetchGetNomenclature({ token })
    const { isLoading: isLoadingPrices, isError: isErrorPrices, data: prices, error: errorPrices } = useFetchGetPrices({ token })
    const { isLoading: isLoadingPriceTypes, isError: isErrorPriceTypes, data: priceTypes, error: errorPriceTypes } = useFetchGetPriceTypes({ token })
    const {
        data: purchaseData,
        isLoading: isLoadingPurchase,
        isError: isErrorPurchase,
        error: errorPurchase,
      } = useFetchGetPurchaseById({ token, id: editModalId });


    const prepareData = useMemo(() => {
        if ((isErrorContragents || isSuccessConragents) && (isErrorContracts || isSuccessContracts) && (isErrorOrganizations || isSuccessOrganizations) && (isErrorWarehouses || isSuccessWarehouses) && (isErrorUsers || isSuccessUsers) && (isSuccessNomenclatures || isErrorNomenclatures)) {
            const prepareContragents = prepareForSelector(contragents?.result);
            const prepareContracts = prepareForSelector(contracts);
            const prepareOrganizations = prepareForSelector(organizations);
            const prepareWarehouses = prepareForSelector(warehouses);
            const prepareUsers = prepareUsersForSelector(users?.result);
            const prepareNomenclatures = prepareForSelector(nomenclatures);
            return ({
                client: prepareContragents,
                contragent: prepareContragents,
                contract: prepareContracts,
                organization: prepareOrganizations,
                warehouse: prepareWarehouses,
                purchased_by: prepareUsers,
                nomenclature: prepareNomenclatures,
            })
        }
    }, [isErrorContragents, isSuccessConragents, isErrorContracts, isSuccessContracts, isErrorOrganizations, isSuccessOrganizations, isErrorWarehouses, isSuccessWarehouses, contragents, contracts, organizations, warehouses, isErrorUsers, isSuccessUsers, users, isSuccessNomenclatures, isErrorNomenclatures, nomenclatures])

    return {
        dataForSelector: prepareData,
        data: { nomenclatures, prices, priceTypes, purchase:purchaseData },
        isLoading: isLoadingContragents || isLoadingContracts || isLoadingOrganizations || isLoadingWarehouses || isLoadingUsers || isLoadingNomenclatures || isLoadingPrices || isLoadingPriceTypes||isLoadingPurchase,
        isErrorDirectory: isErrorContragents || isErrorContracts || isErrorOrganizations || isErrorWarehouses || isErrorUsers || isErrorNomenclatures || isErrorPrices || isErrorPriceTypes||isErrorPurchase,
        errors: [errorContragets, errorContracts, errorOrganizations, errorWarehouses, errorUsers, errorNomenclatures, errorPrices, errorPriceTypes, errorPurchase].filter(item => item)
    }
}


export {prepareDataForCreate, useGetData}