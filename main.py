import os, uuid, json, base64
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis
from celery import Celery

REDIS_URL = os.environ["REDIS_URL"]

app = FastAPI(title="Clipr API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

celery_app = Celery("clipr", broker=REDIS_URL, backend=REDIS_URL)

@app.get("/")
def root():
    return {"status": "Clipr API running"}

@app.post("/api/jobs/upload")
async def create_upload_job(file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    file_bytes = await file.read()
    file_b64 = base64.b64encode(file_bytes).decode()
    r = aioredis.from_url(REDIS_URL)
    await r.set(f"job:{job_id}", json.dumps({"status": "queued", "progress": 0}), ex=3600)
    await r.aclose()
    celery_app.send_task("tasks.process_upload", args=[job_id, file_b64])
    return {"job_id": job_id, "status": "queued"}
