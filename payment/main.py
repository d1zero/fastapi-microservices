from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.background import BackgroundTasks
from starlette.requests import Request
from redis_om import get_redis_connection, HashModel, NotFoundError
import requests
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*'],
)

redis = get_redis_connection(
    host="127.0.0.1",
    port=6379,
    decode_responses=True
)


class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str

    class Meta:
        database = redis


def change_order_status(order: Order, status: str):
    time.sleep(5)
    order.status = status
    order.save()
    redis.xadd('order_completed', order.dict(), '*')


@app.get('/orders/{pk}')
def get(pk: str):
    try:
        return Order.get(pk)
    except NotFoundError:
        return JSONResponse(status_code=404, content={'message':
                                                      'Order was not found'})


@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()

    if 'id' not in body:
        return JSONResponse(status_code=400, content={'message':
                                                      'id must be provided'})

    if 'quantity' not in body:
        return JSONResponse(status_code=400,
                            content={'message': 'quantity must be provided'})

    req = requests.get('http://127.0.0.1:8000/products/%s' % body['id'])

    product = req.json()

    order = Order(
        product_id=body['id'],
        price=product['price'],
        fee=product['price']*0.2,
        total=product['price'] * 1.2 * body['quantity'],
        quantity=body['quantity'],
        status='pending'
    )
    order.save()

    background_tasks.add_task(change_order_status, order, 'completed')

    return order
