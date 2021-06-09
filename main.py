# import from packages
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

#import from aap
from routers import inflation
from routers import econseries
from routers import tables


app = FastAPI()

origins = ["http://localhost:5000"]
# origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_headers = ['*'],
    allow_methods = ["GET"])


app.include_router(inflation.router)
app.include_router(econseries.router)
app.include_router(tables.router)

@app.get("/")
async def home():
    return {"message": "Hello Database User"}


