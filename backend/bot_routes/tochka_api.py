import aiohttp

class TochkaBankError(Exception):
    def __init__(self, code: str, message: str, error_id: str, errors: list):
        self.code = code
        self.message = message
        self.error_id = error_id
        self.errors = errors
        super().__init__(self.message)


async def send_payment_to_tochka(
    account_code: str,
    bank_code: str,
    counterparty_bank_bic: str,
    counterparty_account_number: str,
    paymentAmount: float,
    paymentDate: str,
    counterparty_name: str,
    payment_purpose: str,
) -> dict:
    url = "https://enter.tochka.com/sandbox/v2/payment/v1.0/for-sign"
    
    headers = {
        "Authorization": f"Bearer working_token",
        "Content-Type": "application/json"
    }
    
    payload = { 
        "Data": {
            "accountCode": account_code,
            "bankCode": bank_code,
            "paymentAmount": paymentAmount,
            "paymentDate": paymentDate,
            "counterpartyBankBic": counterparty_bank_bic,
            "counterpartyAccountNumber": counterparty_account_number,
            "counterpartyName": counterparty_name,
            "paymentPurpose": payment_purpose
        }
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            response_data = await response.json()
            
            if response.status == 200:
                return {
                    "success": True,
                    "request_id": response_data["Data"]["requestId"],
                    "status_code": 200
                }
            
            error_code = response_data.get("code")
            error_message = response_data.get("message")
            error_id = response_data.get("id")
            
            error_mapping = {
                "400": "Bad Request - Invalid input parameters",
                "401": "Unauthorized - Invalid or expired token",
                "403": "Forbidden - Insufficient permissions",
                "404": "Not Found - Resource not found",
                "500": "Internal Server Error"
            }
            
            raise TochkaBankError(
                code=error_code,
                message=error_mapping.get(error_code, error_message),
                error_id=error_id
            )
