import io
import os
import re
import json
import time
import uuid
import zipfile
import asyncio
import threading
import subprocess
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, UploadFile, Form, WebSocket, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR = os.path.expanduser("~/whisper-data")
UPLOADS_DIR = f"{DATA_DIR}/uploads"
OUTPUTS_DIR = f"{DATA_DIR}/outputs"
JOBS_DB = f"{DATA_DIR}/jobs.json"

os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)

app = FastAPI()

# ---------- persistence ----------

def load_jobs() -> List[Dict[str, Any]]:
    if not os.path.exists(JOBS_DB):
        return []
    with open(JOBS_DB, "r", encoding="utf-8") as f:
        return json.load(f)

def save_jobs(jobs: List[Dict[str, Any]]) -> None:
    tmp = JOBS_DB + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    os.replace(tmp, JOBS_DB)

def upsert_job(rec: Dict[str, Any]) -> None:
    jobs = load_jobs()
    for i, j in enumerate(jobs):
        if j["job_id"] == rec["job_id"]:
            jobs[i] = rec
            save_jobs(jobs)
            return
    jobs.insert(0, rec)
    save_jobs(jobs)

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    for j in load_jobs():
        if j["job_id"] == job_id:
            return j
    return None

# ---------- GPU detect / names ----------

def detect_gpus() -> List[int]:
    try:
        out = subprocess.check_output(["nvidia-smi", "-L"]).decode()
        ids = re.findall(r"GPU (\d+):", out)
        return [int(x) for x in ids] if ids else [0]
    except Exception:
        return [0]

def gpu_names() -> Dict[int, str]:
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"]
        ).decode().strip().splitlines()
        return {i: name.strip() for i, name in enumerate(out)}
    except Exception:
        return {0: "GPU 0"}

GPU_POOL = detect_gpus()
GPU_NAMES = gpu_names()

GPU_BUSY: Dict[int, bool] = {g: False for g in GPU_POOL}
GPU_LOCK = threading.Lock()

def acquire_gpu() -> Optional[int]:
    with GPU_LOCK:
        for g in GPU_POOL:
            if not GPU_BUSY[g]:
                GPU_BUSY[g] = True
                return g
    return None

def release_gpu(gpu_id: int) -> None:
    with GPU_LOCK:
        if gpu_id in GPU_BUSY:
            GPU_BUSY[gpu_id] = False

# ---------- runtime state ----------

class RuntimeJob:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self.ws: Optional[WebSocket] = None

        self.gpu_id: Optional[int] = None
        self.proc: Optional[subprocess.Popen] = None

        self.pause_requested = False
        self.pause_lock = threading.Lock()

    def request_pause(self):
        with self.pause_lock:
            self.pause_requested = True

    def is_pause_requested(self) -> bool:
        with self.pause_lock:
            return self.pause_requested

RUNTIME: Dict[str, RuntimeJob] = {}
RUNTIME_LOCK = threading.Lock()

# ---------- frontend ----------

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

# ---------- APIs ----------

@app.get("/jobs")
def list_jobs():
    return load_jobs()

@app.get("/gpus")
def gpus():
    return {
        "gpus": [{"id": g, "name": GPU_NAMES.get(g, f"GPU {g}")} for g in GPU_POOL],
        "busy": GPU_BUSY
    }

@app.get("/download/{job_id}.{ext}")
def download(job_id: str, ext: str):
    if ext not in ("txt", "srt"):
        raise HTTPException(400, "ext must be txt or srt")
    path = f"{OUTPUTS_DIR}/{job_id}.{ext}"
    if not os.path.exists(path):
        raise HTTPException(404, "file not found")
    # Use original filename (strip original extension, append target ext)
    job = get_job(job_id)
    orig = job.get("filename", job_id) if job else job_id
    base = os.path.splitext(orig)[0]  # strip original ext (e.g. .mp4)
    dl_name = f"{base}.{ext}"
    return FileResponse(path, filename=dl_name)

@app.get("/download-batch")
def download_batch(job_ids: str):
    """
    Download multiple completed jobs as a ZIP file.
    job_ids: comma-separated list of job IDs.
    """
    ids = [j.strip() for j in job_ids.split(",") if j.strip()]
    if not ids:
        raise HTTPException(400, "no job_ids provided")

    buf = io.BytesIO()
    seen: Dict[str, int] = {}
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for job_id in ids:
            job = get_job(job_id)
            if not job or job.get("status") != "done":
                continue
            orig = job.get("filename", job_id)
            base = os.path.splitext(orig)[0]
            for ext in ("srt", "txt"):
                path = f"{OUTPUTS_DIR}/{job_id}.{ext}"
                if os.path.exists(path):
                    arc = f"{base}.{ext}"
                    # Deduplicate: add suffix if name already used
                    if arc in seen:
                        seen[arc] += 1
                        name_part, ext_part = os.path.splitext(arc)
                        arc = f"{name_part}_{seen[arc]}{ext_part}"
                    else:
                        seen[arc] = 0
                    zf.write(path, arcname=arc)

    if buf.tell() == 0:
        raise HTTPException(404, "no completed files found")

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=transcripts.zip"},
    )

@app.delete("/job/{job_id}")
def delete_job(job_id: str):
    for ext in ("txt", "srt"):
        p = f"{OUTPUTS_DIR}/{job_id}.{ext}"
        if os.path.exists(p):
            os.remove(p)

    for fn in os.listdir(UPLOADS_DIR):
        if fn.startswith(job_id + "_") or fn == f"{job_id}.wav":
            try:
                os.remove(f"{UPLOADS_DIR}/{fn}")
            except:
                pass

    jobs = [j for j in load_jobs() if j["job_id"] != job_id]
    save_jobs(jobs)

    with RUNTIME_LOCK:
        rj = RUNTIME.get(job_id)
        if rj:
            rj.request_pause()
            if rj.proc and rj.proc.poll() is None:
                try:
                    rj.proc.terminate()
                except:
                    pass
            del RUNTIME[job_id]

    return {"ok": True}

# ---- Solution 2: Pause = terminate subprocess (release GPU after exit), Resume = rerun (new job_id)

@app.post("/job/{job_id}/pause")
def pause(job_id: str):
    rj = RUNTIME.get(job_id)
    if not rj:
        raise HTTPException(404, "job not running")
    rj.request_pause()
    if rj.proc and rj.proc.poll() is None:
        try:
            rj.proc.terminate()
        except Exception:
            pass
    return {"ok": True}

@app.post("/job/{job_id}/resume")
async def resume(job_id: str):
    old = get_job(job_id)
    if not old:
        raise HTTPException(404, "job not found")

    src = None
    src_name = None
    for fn in os.listdir(UPLOADS_DIR):
        if fn.startswith(job_id + "_"):
            src = f"{UPLOADS_DIR}/{fn}"
            src_name = fn.split("_", 1)[1] if "_" in fn else old.get("filename", "input")
            break

    if not src or not os.path.exists(src):
        raise HTTPException(400, "source file missing")

    new_job_id = str(uuid.uuid4())
    new_src = f"{UPLOADS_DIR}/{new_job_id}_{src_name}"

    try:
        os.link(src, new_src)
    except Exception:
        import shutil
        shutil.copy2(src, new_src)

    rec = {
        "job_id": new_job_id,
        "filename": old.get("filename", src_name),
        "traditional": bool(old.get("traditional", False)),
        "status": "queued",
        "created_at": int(time.time()),
        "gpu_id": None,
        "gpu_name": None,
        "started_at": None,
        "ended_at": None,
        "wall_total": None,
        "avg_speed": None,
        "audio_total": None,
        "resumed_from": job_id,
    }
    upsert_job(rec)

    rj = RuntimeJob(new_job_id)
    with RUNTIME_LOCK:
        RUNTIME[new_job_id] = rj

    asyncio.create_task(_run_job(new_job_id, new_src, rec["filename"], rec["traditional"]))

    return {"job_id": new_job_id}

@app.post("/upload")
async def upload(
    file: UploadFile,
    traditional: bool = Form(False),
):
    job_id = str(uuid.uuid4())
    src_path = f"{UPLOADS_DIR}/{job_id}_{file.filename}"

    with open(src_path, "wb") as f:
        f.write(await file.read())

    rec = {
        "job_id": job_id,
        "filename": file.filename,
        "traditional": bool(traditional),
        "status": "queued",
        "created_at": int(time.time()),
        "gpu_id": None,
        "gpu_name": None,
        "started_at": None,
        "ended_at": None,
        "wall_total": None,
        "avg_speed": None,
        "audio_total": None,
    }
    upsert_job(rec)

    rj = RuntimeJob(job_id)
    with RUNTIME_LOCK:
        RUNTIME[job_id] = rj

    asyncio.create_task(_run_job(job_id, src_path, file.filename, bool(traditional)))
    return {"job_id": job_id}

@app.websocket("/ws/{job_id}")
async def ws(job_id: str, ws: WebSocket):
    await ws.accept()

    rj = RUNTIME.get(job_id)
    if not rj:
        await ws.send_json({"type": "done"})
        await ws.close()
        return

    rj.ws = ws
    try:
        while True:
            msg = await rj.queue.get()
            await ws.send_json(msg)
            if msg.get("type") in ("done", "error", "paused"):
                break
    finally:
        try:
            await ws.close()
        except:
            pass
        rj.ws = None

# ---------- runner ----------

async def _run_job(job_id: str, src_path: str, filename: str, traditional: bool):
    rj = RUNTIME[job_id]

    gpu_id = None
    while gpu_id is None:
        gpu_id = acquire_gpu()
        if gpu_id is None:
            await asyncio.sleep(0.5)

    rj.gpu_id = gpu_id
    gpu_name = GPU_NAMES.get(gpu_id, f"GPU {gpu_id}")

    started_at = int(time.time())
    rec = get_job(job_id) or {}
    rec.update({
        "job_id": job_id,
        "filename": filename,
        "traditional": traditional,
        "status": "processing",
        "created_at": rec.get("created_at") or int(time.time()),
        "started_at": started_at,
        "gpu_id": gpu_id,
        "gpu_name": gpu_name,
    })
    upsert_job(rec)

    loop = asyncio.get_running_loop()

    def push(msg: Dict[str, Any]):
        def _put():
            try:
                rj.queue.put_nowait(msg)
            except Exception:
                pass
        loop.call_soon_threadsafe(_put)

    status = "error"
    err_msg: Optional[str] = None
    stats: Optional[Dict[str, Any]] = None

    def run_subprocess():
        nonlocal status, err_msg, stats

        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
        env["PYTHONUNBUFFERED"] = "1"

        # Ensure CUDA libraries are discoverable (needed on non-Docker setups)
        cuda_lib_paths = [
            "/home/yorukot/.local/lib/python3.12/site-packages/nvidia/cublas/lib",
            "/usr/local/lib/ollama/cuda_v12",
        ]
        existing = env.get("LD_LIBRARY_PATH", "")
        extra = ":".join(p for p in cuda_lib_paths if os.path.isdir(p))
        env["LD_LIBRARY_PATH"] = f"{extra}:{existing}" if existing else extra

        cmd = [
            "python3", "whisper_worker_entry.py",
            src_path,
            job_id,
            UPLOADS_DIR,
            OUTPUTS_DIR,
            "1" if traditional else "0",
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        rj.proc = proc

        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    continue

                # segment 直接轉送
                if msg.get("type") == "segment":
                    push(msg)
                elif msg.get("type") == "done":
                    stats = msg.get("stats") or {}
                    status = "done"
                elif msg.get("type") == "error":
                    status = "error"
                    err_msg = msg.get("message") or "unknown"
                elif msg.get("type") == "paused":
                    status = "paused"

            rc = proc.wait()

            # 如果是 pause 觸發 terminate，通常會走到這裡 rc != 0
            if rj.is_pause_requested():
                status = "paused"

            # 若什麼都沒收到，依 rc 判斷
            if status not in ("done", "paused", "error"):
                status = "error" if rc != 0 else "done"

            if status == "error" and not err_msg:
                # 把 stderr 摘一點出來
                try:
                    assert proc.stderr is not None
                    err = proc.stderr.read()
                    err_msg = err[-2000:] if err else "unknown"
                except Exception:
                    err_msg = "unknown"

            import logging
            logging.warning("[job %s] status=%s rc=%s err=%s", job_id, status, rc, err_msg)

        finally:
            rj.proc = None

    await asyncio.to_thread(run_subprocess)

    ended_at = int(time.time())

    # 釋放 GPU（最關鍵）
    release_gpu(gpu_id)

    # paused：清掉可能的輸出，避免誤判為 done
    if status == "paused":
        for ext in ("txt", "srt"):
            p = f"{OUTPUTS_DIR}/{job_id}.{ext}"
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass
        push({"type": "paused"})
    elif status == "error":
        push({"type": "error", "message": err_msg or "unknown"})
    else:
        push({"type": "done"})

    # 寫歷史：耗時 + 平均倍速 + audio_total + gpu_name
    wall_total = float(ended_at - started_at) if started_at else None

    rec = get_job(job_id) or {}
    rec.update({
        "status": status,
        "ended_at": ended_at,
        "wall_total": wall_total,
        "gpu_id": gpu_id,
        "gpu_name": gpu_name,
    })
    if stats:
        rec.update({
            "audio_total": stats.get("audio_total"),
            "avg_speed": stats.get("avg_speed"),
            "wall_total": stats.get("wall_total") or wall_total,
        })

    upsert_job(rec)

    with RUNTIME_LOCK:
        if job_id in RUNTIME:
            del RUNTIME[job_id]

