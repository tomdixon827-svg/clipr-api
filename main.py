import os, uuid, json
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

@app.post("/api/jobs/youtube")
async def create_youtube_job(payload: dict):
    job_id = str(uuid.uuid4())
    r = aioredis.from_url(REDIS_URL)
    await r.set(f"job:{job_id}", json.dumps({"status": "queued", "progress": 0}), ex=3600)
    await r.aclose()
    celery_app.send_task("tasks.process_youtube", args=[job_id, payload])
    return {"job_id": job_id, "status": "queued"}

@app.post("/api/jobs/upload")
async def create_upload_job(file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    os.makedirs(f"/tmp/{job_id}", exist_ok=True)
    file_path = f"/tmp/{job_id}/source.mp4"
    with open(file_path, "wb") as f:
        f.write(await file.read())
    r = aioredis.from_url(REDIS_URL)
    await r.set(f"job:{job_id}", json.dumps({"status": "queued", "progress": 0}), ex=3600)
    await r.aclose()
    celery_app.send_task("tasks.process_upload", args=[job_id, file_path])
    return {"job_id": job_id, "status": "queued"}

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    r = aioredis.from_url(REDIS_URL)
    data = await r.get(f"job:{job_id}")
    await r.aclose()
    if not data:
        return {"job_id": job_id, "status": "queued", "progress": 0}
       @app.get("/api/clips/{job_id}/download")
async def download_clip(job_id: str):
    r = aioredis.from_url(REDIS_URL)
    data = await r.get(f"job:{job_id}")
    await r.aclose()
    if not data:
        return {"error": "Job not found"}
    job = json.loads(data)
    if "download_url" in job:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(job["download_url"])
    return {"error": "File not ready yet"}
    return json.loads(data)
