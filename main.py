import os, uuid, json, base64
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import redis.asyncio as aioredis
from celery import Celery

REDIS_URL = os.environ["REDIS_URL"]
os.makedirs("/data/clips", exist_ok=True)

app = FastAPI(title="Clipr API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

celery_app = Celery("clipr", broker=REDIS_URL, backend=REDIS_URL)

@app.get("/")
def root():
    return {"status": "Clipr API running"}

@app.post("/api/jobs/upload")
async def create_upload_job(file: UploadFile = File(...), clip_start: float = 0, clip_end: float = 0):
    job_id = str(uuid.uuid4())
    file_bytes = await file.read()
    file_b64 = base64.b64encode(file_bytes).decode()
    r = aioredis.from_url(REDIS_URL)
    await r.set(f"job:{job_id}", json.dumps({"status": "queued", "progress": 0}), ex=3600)
    await r.aclose()
    celery_app.send_task("tasks.process_upload", args=[job_id, file_b64, clip_start, clip_end])
    return {"job_id": job_id, "status": "queued"}

@app.post("/api/internal/store/{job_id}")
async def store_clip(job_id: str, file: UploadFile = File(...)):
    file_bytes = await file.read()
    with open(f"/data/clips/{job_id}.mp4", "wb") as f:
        f.write(file_bytes)
    return {"stored": True}

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    r = aioredis.from_url(REDIS_URL)
    data = await r.get(f"job:{job_id}")
    await r.aclose()
    if not data:
        return {"job_id": job_id, "status": "queued", "progress": 0}
    return json.loads(data)

@app.get("/api/clips/{job_id}/download")
async def download_clip(job_id: str):
    path = f"/data/clips/{job_id}.mp4"
    if not os.path.exists(path):
        return {"error": "File not found"}
    return FileResponse(path, media_type="video/mp4", filename="clipr-export.mp4")
