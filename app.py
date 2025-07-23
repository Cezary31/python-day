from fastapi import FastAPI,HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pandas as pd
import psycopg2

from pydantic import BaseModel


class CustomerCreate(BaseModel):
    customer_name: str
    email: str
    phone_number: str
    address_line_1: str
    city: str

app = FastAPI()

connection = psycopg2.connect(
    database="postgres",
    user="postgres.cocnknwvownydcmnrntu",
    password="Niewiem311299.",
    host="aws-0-eu-west-2.pooler.supabase.com",
    port=5432,
)
def execute_query(sql: str, values=None):
    try:
        cursor = connection.cursor()
        print(sql)
        if values:
            cursor.execute(sql,values)
        else:
            cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]

        # Fetch all rows from database
        record = list(cursor.fetchall()[0])
        return [dict(zip(columns, record))]
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()


def execute_insert_update(sql,data):

    try:
        cursor = connection.cursor()

        cursor.execute(sql,data)
        connection.commit()
    except Exception as e:
        print(e)
        return False
    finally:
        if cursor:
            cursor.close()

    return True



"""Handlers"""

def handle_get_customer(name: str): 
    db_query = f"SELECT * FROM dbt.customer WHERE customer_name = '{name}'"
    data = execute_query(db_query)
    return data


def handle_get_all_customers():
    db_query = "SELECT customer_id, customer_name, email, city FROM shop.customer"
    data = execute_query(db_query)

    return data

def handle_create_customer(customer:CustomerCreate):
    sql = "INSERT INTO customer (customer_name, email, phone_number, address_line_1, city) VALUES (%s, %s, %s, %s, %s)"
    customer_data = (customer.customer_name,customer.email,customer.phone_number,customer.address_line_1,customer.city)

    return execute_insert_update(sql,customer_data)

def handle_update_customer(customer_id:int ,customer:CustomerCreate):
    sql = "UPDATE customer SET customer_name=%s, email=%s, phone_number=%s, address_line_1=%s, city=%s WHERE customer_id=%s"
    data = (customer.customer_name,customer.email,customer.phone_number,customer.address_line_1,customer.city,customer_id)

    return execute_insert_update(sql,data)


def handle_get_customer_orders(customer_id:int):
    sql = "SELECT o.order_id, o.order_date, o.total_amount, s.status_name " \
    "FROM orders o " \
    "JOIN order_status s ON o.order_status_id = s.order_status_id " \
    "WHERE o.customer_id = %s " \
    "ORDER BY o.order_date DESC"

    values = (customer_id,)
    
    data = execute_query(sql,values)

    return data


def handle_get_product(product_id:int):

    db_query = f"SELECT * FROM shop.product WHERE product_id = '{product_id}'"
    data = execute_query(db_query)

    print(data)

    if len(data)==0:
        return HTTPException(404)
    
    return data


def handle_get_all_products():
    db_query = "SELECT * FROM shop.product"
    data = execute_query(db_query)

    return data


def handle_get_order_details(order_id:int):
    sql = "SELECT o.order_id, o.order_date, o.total_amount,c.customer_name," \
    " c.email, s.status_name " \
    "FROM shop.orders o " \
    "JOIN shop.customer c ON o.customer_id = c.customer_id" \
    " JOIN shop.order_status s ON o.order_status_id = s.order_status_id WHERE o.order_id = %s"
    values = (order_id,)

    data = execute_query(sql,values)

    if data is None:
        return HTTPException(404)

    return data

def handle_get_order_items(order_id:int):
    sql =  "SELECT ol.quantity, p.product_name, p.selling_price, (ol.quantity * p.selling_price) as line_total " \
    "FROM shop.order_line ol " \
    "JOIN shop.product p ON ol.product_id = p.product_id " \
    "WHERE ol.order_id = %s"

    values = (order_id,)

    data =execute_query(sql,values)

    if data is None:
        return HTTPException(404)

    return data


def handle_update_order_status(order_id:int,new_status_id:int):

    sql = "UPDATE shop.orders SET order_status_id = %s WHERE order_id = %s"
    values = (order_id,new_status_id)

    return execute_insert_update(sql,values)



def handle_delete_order(order_id:int):

    sql = "DELETE FROM shop.order_line WHERE order_id = %s"
    values = (order_id,)

    if execute_insert_update(sql,values):
    
        sql="DELETE FROM shop.orders WHERE order_id = %s"

        if execute_insert_update(sql,values):
            return True
    
    return False



"""Endpoints"""


@app.get("/",tags=["Home"])
async def root():
    return {"message": "Hello World"}


@app.get("/products",tags=["Product"])
async def get_all_products():
    return JSONResponse(content=jsonable_encoder(handle_get_all_products()))

    
@app.get("/products/{product_id}",tags=["Product"])
async def get_product(product_id: int):
    return JSONResponse(content=jsonable_encoder(handle_get_product(product_id)))

@app.get("/customers",tags=["Customer"])
async def get_all_customers():
    return JSONResponse(content=jsonable_encoder(handle_get_all_customers()))


@app.get("/customers/{customer_name}",tags=["Customer"])
async def get_customer(customer_name: int):
    return JSONResponse(content=jsonable_encoder(handle_get_customer(customer_name)))


@app.get("/customers/{customer_id}/orders", tags=["Customer"])
def get_customer_orders(customer_id: int):
    
    return JSONResponse(content=jsonable_encoder(handle_get_customer_orders(customer_id)))


@app.post("/customers",tags=["Customer"])
def create_customer(customer: CustomerCreate):

    if handle_create_customer(customer):
        return handle_get_customer(customer.customer_name)
    
    
    return HTTPException(400,"User not created")

@app.put("/customers",tags=["Customer"])
def update_customer(customer: CustomerCreate):

    if handle_update_customer(customer):
        return handle_get_customer(customer.customer_name)
    
    return HTTPException(400,"User not updated")


@app.get("/orders/{order_id}", tags=["Order"])
def get_order_details(order_id: int):

    return JSONResponse(content=jsonable_encoder(handle_get_order_details(order_id)))



@app.get("/orders/{order_id}/items", tags=["Order"])
def get_order_items(order_id: int):
    
    return JSONResponse(content=jsonable_encoder(handle_get_order_items(order_id)))


@app.put("/orders/{order_id}/status",tags=["Order"])
def update_order_status(order_id: int, new_status_id: int):
    
    if handle_update_order_status(order_id,new_status_id):
        return JSONResponse(content=jsonable_encoder(handle_get_order_details(order_id)))
    
    return HTTPException(400,"Error while updating order")



@app.delete("/orders/{order_id}", tags= ["Order"])
def delete_order(order_id: int):
    
    if handle_delete_order(order_id):
        return {"message": f"Deleted order with id:{order_id}"}


