from confluent_kafka import Producer
import random
from datetime import datetime 
import json
import time 
import logging
import uuid

class OrderProducer:
    def __init__(self):
        
        logging.basicConfig(
            filename = '/Workspace/Users/hrithikpoojary29@gmail.com/Investment_Banking/Logs/OrderProducer.log',
            level = logging.INFO,
            format = "OrderProducer - | %(asctime)s | %(levelname)s %(message)s"
            )
            
        self.cong = {
                "bootstrap.servers" : "**",
                "security.protocol" : "SASL_SSL",
                "sasl.mechanism" : "PLAIN",
                "sasl.username" : "**",
                "sasl.password" : "**",
                "client.id" : "132"
        }
        
        self.topic = "DEV_ORDERS"
        
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
        asset_list = (
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
            currency  = "USD"
            
        input = {
            "unique_no" : str(uuid.uuid4()),
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
        
    def delivery_msg(self,err , msg):
        if err is not None:
            logging.error(f"Error occured {err}")
        else:
            account_number = msg.key().decode('utf-8')
            asset_number = json.loads(msg.value())['asset_external_id']
            logging.info(f"Account {account_number} Asset {asset_number} is pushed successfully")

    def order_producer(self):

        try:
            producer_data =self.order_data()
            key = json.loads(producer_data)['account_number']
            value = producer_data

            producer = Producer(self.cong)

            producer.produce(
                            self.topic,
                            key = key,
                            value = value,
                            callback = self.delivery_msg
                                )
            producer.poll(0)
            time.sleep(0.5)
        except Exception as e :
            logging.info(f"Failed due to {e}")

        finally:
            producer.flush(1)

if __name__ == '__main__':
    op = OrderProducer()
    op.order_producer()
    
