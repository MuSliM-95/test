class LoyalityEnricher:
    @staticmethod
    def enrich_item(item: dict, loyality_map: dict[tuple[int, int], dict]) -> dict:
        key = (item.get("id"), item.get("cashbox"))
        values = loyality_map.get(key, {})

        item["paid_loyality"] = round(values.get("total_amount", 0), 2)
        item["has_loyality_card"] = bool(values.get("has_link"))

        if item["has_loyality_card"]:
            item["color_status"] = "green"
        elif item.get("has_contragent"):
            item["color_status"] = "blue"
        else:
            item["color_status"] = "default"

        return item

    def enrich_single_item(self, item: dict, loyality_map: dict[tuple[int, int], dict]) -> dict:
        return self.enrich_item(item, loyality_map)