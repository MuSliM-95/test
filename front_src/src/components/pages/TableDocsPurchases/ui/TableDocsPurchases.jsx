import React from "react";
import { TableDocsPurchases } from "../../../features/Table";
import { AddDocsPurchases } from "../../../widgets/Button";
import { DocsPurchasesContext } from "../../../shared/lib/hooks/context/getDocsPurchasesContext";
import { useLocation } from "react-router-dom";

export default function TableDocsPurchasesPage({
    token,
    websocket,
}) {
    const { pathname } = useLocation();
    console.log("websocket",websocket)
    return (
        <DocsPurchasesContext.Provider
            value={{
                token,
                websocket,
                pathname,
            }}
        >
            <AddDocsPurchases />
            <TableDocsPurchases />
        </DocsPurchasesContext.Provider>
    );
}
