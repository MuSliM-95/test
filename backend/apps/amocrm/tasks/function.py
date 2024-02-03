import re


async def phone_normalizer(phone):
    normalized_phone = ""
    if phone:
        reg_phone = re.findall(r"^(\+7|7|8).*?(\d{3}).*?(\d{3}).*?(\d{2}).*?(\d{2,})$", phone)
        if len(reg_phone) == 1:
            phone_list = list(reg_phone[0])
            phone_sum = sum(len(item) for item in phone_list[1:])
            if phone_list[0] == "+7":
                normalized_phone = "".join(phone_list)
            if phone_list[0] != "+7" and phone_sum == 10:
                phone_list[0] = "+7"
                normalized_phone = "".join(phone_list)
    return normalized_phone
