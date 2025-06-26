from typing import Union

import pandas as pd
import pickle


def process_price(price: Union[str, float, None]) -> float:
    if isinstance(price, str):
        try:
            return float(price.strip("$"))
        except:
            return None
    return price


class CustomerDataExtractor(object):
    def __init__(self, customer_file_path: str, vip_customer_file_path: str):
        with open(customer_file_path, 'rb') as f:
            self.customer_data = pickle.load(f)

        with open(vip_customer_file_path) as f:
            self.vip_customer_data = [int(line.strip()) for line in f.readlines()]

        self.category_mapping = {
            1: "Electronics",
            2: "Apparel",
            3: "Books",
            4: "Home Goods",
        }

    def construct_customer_data(self):
        customer_data = []
        for customer_entry in self.customer_data:
            for order in customer_entry.get("orders", []):
                order_prices = []
                for o in order.get("items", []):
                    if process_price(o["price"]) is None:
                        continue
                    if o["quantity"] == "FREE":
                        order_prices.append(0)
                    else:
                        order_prices.append(process_price(o["price"]) * int(o["quantity"]))
                order_value = sum(order_prices)
                for order_item in order.get("items", []):
                    try:
                        price = float(process_price(order_item["price"]))
                        quantity = int(order_item["quantity"])
                        total_item_price = price * quantity
                        customer_data_row = {
                            "customer_id": int(customer_entry["id"]),
                            "customer_name": str(customer_entry["name"]),
                            "registration_date": pd.to_datetime(customer_entry["registration_date"]),
                            "is_vip": int(customer_entry["id"]) in self.vip_customer_data,
                            "order_id": int(order["order_id"]),
                            "order_date": pd.to_datetime(order["order_date"]),
                            "product_id": int(order_item["item_id"]),
                            "product_name": str(order_item["product_name"]),
                            "category": self.category_mapping.get(int(order_item["category"]), "Misc"),
                            "unit_price": price,
                            "item_quantity": quantity,
                            "total_item_price": total_item_price,
                            "total_order_value_percentage": total_item_price / order_value,
                        }
                    except:
                        continue
                    else:
                        customer_data.append(customer_data_row)

        df = pd.DataFrame(customer_data)
        df.sort_values(by=["customer_id", "order_id", "product_id"], ascending=True, inplace=True)
        return df


extractor = CustomerDataExtractor("customer_orders.pkl", "vip_customers.txt")
extractor.construct_customer_data().to_csv("customer_data.csv", index=False)