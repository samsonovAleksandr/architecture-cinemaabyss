import logging
import os
import random

import httpx
from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("proxy-service")

PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081").rstrip("/")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082").rstrip("/")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = max(0, min(100, int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))))

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "content-encoding",
}

app = FastAPI(title="CinemaAbyss Proxy Service")
client = httpx.AsyncClient(timeout=30.0)


def choose_movies_upstream() -> str:
    if GRADUAL_MIGRATION and random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
        return MOVIES_SERVICE_URL
    return MONOLITH_URL


def resolve_upstream(path: str) -> str:
    if path == "/api/movies" or path.startswith("/api/movies/"):
        return choose_movies_upstream()
    if path == "/api/events" or path.startswith("/api/events/"):
        return EVENTS_SERVICE_URL
    return MONOLITH_URL


def filter_headers(headers) -> dict:
    return {k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}


@app.on_event("startup")
async def on_startup():
    logger.info(
        "Proxy started: monolith=%s movies=%s events=%s gradual_migration=%s movies_percent=%d",
        MONOLITH_URL,
        MOVIES_SERVICE_URL,
        EVENTS_SERVICE_URL,
        GRADUAL_MIGRATION,
        MOVIES_MIGRATION_PERCENT,
    )


@app.on_event("shutdown")
async def on_shutdown():
    await client.aclose()


@app.get("/health")
async def health():
    return {"status": True}


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy(full_path: str, request: Request):
    path = "/" + full_path
    upstream = resolve_upstream(path)
    url = upstream + path
    logger.info("%s %s -> %s", request.method, path, upstream)
    try:
        upstream_response = await client.request(
            request.method,
            url,
            params=request.query_params,
            headers=filter_headers(request.headers),
            content=await request.body(),
        )
    except httpx.RequestError as exc:
        logger.error("Upstream %s unavailable: %s", upstream, exc)
        return Response(
            content='{"error":"Upstream service unavailable"}',
            status_code=502,
            media_type="application/json",
        )
    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=filter_headers(upstream_response.headers),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
