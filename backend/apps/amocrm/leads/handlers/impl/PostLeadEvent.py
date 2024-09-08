from sqlalchemy import select, insert, update

from apps.amocrm.leads.handlers.core.IPostLeadEvent import IPostLeadEvent
from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.amocrm.leads.repositories.core.ILeadsRepository import ILeadsRepository
from apps.amocrm.leads.repositories.models.CreateLeadModel import CreateLeadModel, CustomFieldValue, \
    CustomFieldValueElement, EmveddedModel, EmveddedContactModel
from apps.amocrm.tools.get_install import get_install_by_cashbox
from database.db import amo_leads, database, amo_leads_docs_sales_mapping, docs_sales_tags, docs_sales


class PostLeadEvent(IPostLeadEvent):

    def __init__(
        self,
        post_amo_lead_message: NewLeadBaseModelMessage,
        leads_repository: ILeadsRepository
    ):
        self.__post_amo_lead_message = post_amo_lead_message
        self.__leads_repository = leads_repository

    async def __call__(self):
        install_info = await get_install_by_cashbox(
            cashbox_id=self.__post_amo_lead_message.cashbox_id,
            type_install="leads"
        )

        custom_fields = [
            CustomFieldValue(
                field_code="ACC_LINK",
                values=[
                    CustomFieldValueElement(
                        value=self.__post_amo_lead_message.account_link
                    )
                ]
            ),
            CustomFieldValue(
                field_code="ACT_LINK",
                values=[
                    CustomFieldValueElement(
                        value=self.__post_amo_lead_message.act_link
                    )
                ]
            ),
            CustomFieldValue(
                field_code="NOMEN_INFO",
                values=[
                    CustomFieldValueElement(
                        value=self.__post_amo_lead_message.nomenclature
                    )
                ]
            ),
            CustomFieldValue(
                field_code="AREND_START",
                values=[
                    CustomFieldValueElement(
                        value=self.__post_amo_lead_message.start_period
                    )
                ]
            ),
            CustomFieldValue(
                field_code="AREND_END",
                values=[
                    CustomFieldValueElement(
                        value=self.__post_amo_lead_message.end_period
                    )
                ]
            ),
        ]

        if self.__post_amo_lead_message.contact_ext_id:
            create_lead_model = CreateLeadModel(
                name=self.__post_amo_lead_message.lead_name,
                price=0 if not self.__post_amo_lead_message.price else self.__post_amo_lead_message.price,
                status_id=self.__post_amo_lead_message.status_id,
                custom_fields_values=custom_fields,
                _embedded=EmveddedModel(
                    contacts=[
                        EmveddedContactModel(
                            id=self.__post_amo_lead_message.contact_ext_id
                        )
                    ]
                )
            )
        else:
            create_lead_model = CreateLeadModel(
                name=self.__post_amo_lead_message.lead_name,
                price=0 if not self.__post_amo_lead_message.price else self.__post_amo_lead_message.price,
                status_id=self.__post_amo_lead_message.status_id,
                custom_fields_values=custom_fields,
            )


        created_leads = await self.__leads_repository.create_lead(
            access_token=install_info.access_token,
            amo_lead_model=create_lead_model,
            referrer=install_info.referrer
        )
        for index, lead_info in enumerate(created_leads):
            query = (
                insert(amo_leads)
                .values(
                    amo_install_group_id=install_info.group_id,
                    name=self.__post_amo_lead_message.lead_name,
                    is_deleted=False,
                    amo_id=lead_info["id"],
                    contact_id=self.__post_amo_lead_message.contact_id,
                )
                .returning(amo_leads.c.id)
            )
            created_lead = await database.fetch_one(query)

            query = (
                insert(amo_leads_docs_sales_mapping)
                .values(
                    docs_sales_id=self.__post_amo_lead_message.docs_sales_id,
                    lead_id=created_lead.id,
                    table_status=1,
                    is_sync=True,
                    amo_install_group_id=install_info.group_id,
                    cashbox_id=self.__post_amo_lead_message.cashbox_id,
                )
            )
            await database.execute(query)

            query = (
                insert(docs_sales_tags)
                .values(
                    docs_sales_id=self.__post_amo_lead_message.docs_sales_id,
                    name=f"ID_{lead_info['id']}"
                )
            )
            await database.execute(query)

            query = (
                select(docs_sales.tags)
                .where(docs_sales.c.id == self.__post_amo_lead_message.docs_sales_id)
            )
            doc_sale_tags_info = await database.fetch_one(query)
            if doc_sale_tags_info:
                if doc_sale_tags_info.tags:
                    tags = doc_sale_tags_info.tags + f",ID_{lead_info['id']}"
                else:
                    tags = f"ID_{lead_info['id']}"
            else:
                tags = f"ID_{lead_info['id']}"

            query = (
                update(docs_sales)
                .where(docs_sales.c.id == self.__post_amo_lead_message.docs_sales_id)
                .values(
                    tags=tags
                )
            )
            await database.execute(query)