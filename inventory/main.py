from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel, NotFoundError

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


class Product(HashModel):
    name: str
    price: float
    quantity: int

    class Meta:
        database = redis


def format(pk: str):
    product = Product.get(pk)
    return {
        'id': product.pk,
        'name': product.name,
        'price': product.price,
        'quantity': product.quantity,
    }


@app.get('/products')
def all():
    return [format(pk) for pk in Product.all_pks()]


@app.post('/products')
def create(product: Product):
    return product.save()


@app.get('/products/{pk}')
def get(pk: str):
    try:
        return Product.get(pk)
    except NotFoundError:
        return JSONResponse(status_code=404, content={'message':
                                                      'Product was not found'})


@app.delete('/products/{pk}')
def delete(pk: str):
    Product.delete(pk)
    return {'message': 'success'}
