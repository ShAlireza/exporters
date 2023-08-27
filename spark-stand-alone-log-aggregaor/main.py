import requests
from fastapi import FastAPI, APIRouter
from fastapi.responses import PlainTextResponse

from spark_exporter import clusters

app = FastAPI()
router = APIRouter()

for cluster in clusters:
    @router.get(f"/{cluster.name}/metrics", response_class=PlainTextResponse)
    def get_metrics():
        response_text = cluster.export_metrics()

        return response_text

app.include_router(
    router,
    tags=['Metrics']
)
