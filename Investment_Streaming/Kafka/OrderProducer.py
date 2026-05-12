from confluent_kafka import Producer
import random
from datetime import date 
import json
import time 
import logging

class OrderProducer:
    def __init__(self):
        
        logging.basicConfig(
            filename = 'OrderProducer.log',
            levelname = logging.INFO,
            format = "OrderProducer - | %(asctime)s | %(levelname)s %(message)s"
            )
            
        self.cong = {
                "bootstrap.servers" : "",
                "security.protocol" : "SASL_SSL",
                "sasl.mechanism" : "PLAIN",
                "sasl.username" : "",
                "sasl.password" : "",
                "client.id" : 
        }
        
        self.topic = ""
        
    def account(self):
        account_list = (
            "12059-49504",
            "18594-07904",
            "06932-99420",
            "07942-19393",
            "97940-69402"
            )
        account_number = random.choice(account_list)
        return account_number
        
    def asset(self):
        asset_list = 
                (
                    "RELIANCE", 
                    "TCS", 
                    "HDFCBANK", 
                    "BHARTIARTL", 
                    "ICICIBANK" , 
                    "NVDA", 
                    "GOOGL", 
                    "AAPL", 
                    "MSFT", 
                    "AMZN"
                    )
        asset_id = random.choice(asset_list)
        return asset_id
        
    def payment_method(self):
        payment_list = ("UPI",
                        "CASH")
        payment_id = random.choice(payment_list)
        return payment_id
        
    def order_type(self):
        order_type_list = ("BUY" , "SELL")
        order_type_id = random.choice(order_type_list)
        return order_type_id
    
    def quantity(self):
        qty = round(random.uniform(0,100000),2)
        return qty 
        
    def order_data(self):
        
        asset = self.asset()
        
        if asset in ("ICICIBANK" , "NVDA", "GOOGL",  "AAPL", "MSFT", "AMZN"):
            currency = "INR"
        else :
            currently  = "USD"
            
        input = {
            "account_number" : self.account(),
            "asset_external_id" : asset,
            "order_type": self.order_type(),
            "payment_method" : self.payment_method(),
            "currency" : currency,
            "quantity" : self.quantity(),
            "created_dt" : str(datetime.now())
            
        }
        
        data  = json.dumps(input)
        return data 
        
    def delivery_msg(err , msg):
        if err is not None:
            logging.error(f"Error occured {err}")
        else:
            logging.info(f"Successfull")
            
    
    
        
        
        
        
        
        
        
        
        
        
        
        
        
       
 
