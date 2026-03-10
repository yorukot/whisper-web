// ─── helpers ────────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);

function fmtTime(ts){
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString();
}
function fmtSec(s){
  if (s === null || s === undefined || !isFinite(s)) return "—";
  return `${Number(s).toFixed(1)}s`;
}

// ─── DOM refs ────────────────────────────────────────────────────────────────
const fileInput       = $("file");
const fileListEl      = $("fileList");
const traditionalChk  = $("traditional");
const startBtn        = $("start");
const activeSection   = $("activeSection");
const activeJobsEl    = $("activeJobs");
const historyEl       = $("history");
const refreshBtn      = $("refresh");
const batchDownloadBtn= $("batchDownloadBtn");

// ─── state ───────────────────────────────────────────────────────────────────
// Map<job_id, { ws, card elements }>
const activeJobs = new Map();

// ─── file picker display ─────────────────────────────────────────────────────
fileInput.addEventListener("change", () => {
  fileListEl.innerHTML = "";
  for (const f of fileInput.files) {
    const chip = document.createElement("div");
    chip.className = "file-chip";
    chip.innerHTML = `<span class="fname">${f.name}</span><span>${(f.size/1024/1024).toFixed(1)} MB</span>`;
    fileListEl.appendChild(chip);
  }
});

// ─── start (batch upload) ────────────────────────────────────────────────────
startBtn.addEventListener("click", async () => {
  const files = Array.from(fileInput.files);
  if (!files.length) return alert("請選擇至少一個檔案");

  startBtn.disabled = true;

  // Upload all files concurrently; each gets its own job card immediately
  await Promise.all(files.map(f => uploadFile(f, traditionalChk.checked)));

  // Clear picker
  fileInput.value = "";
  fileListEl.innerHTML = "";
  startBtn.disabled = false;
});

async function uploadFile(file, traditional) {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("traditional", traditional);

  // Optimistically create a card in "uploading…" state
  const tempId = "tmp-" + Math.random().toString(36).slice(2);
  const card = createActiveCard(tempId, file.name, "上傳中…");

  let job_id;
  try {
    const res = await fetch("/upload", { method: "POST", body: fd });
    const data = await res.json();
    job_id = data.job_id;
  } catch (e) {
    card.setStatus("上傳失敗: " + e.message, true);
    return;
  }

  // Replace tempId with real job_id
  card.setJobId(job_id);
  const state = activeJobs.get(tempId);
  activeJobs.delete(tempId);
  activeJobs.set(job_id, state);

  connectWS(job_id, card);
}

// ─── active job card factory ─────────────────────────────────────────────────
function createActiveCard(jobId, filename, initialStatus) {
  activeSection.hidden = false;

  const wrap = document.createElement("div");
  wrap.className = "job-card";
  wrap.dataset.jobId = jobId;

  wrap.innerHTML = `
    <div class="jc-top">
      <div class="jc-name" title="${filename}">${filename}</div>
      <div class="jc-actions">
        <button class="pause-btn ghost">暫停</button>
      </div>
    </div>
    <div class="jc-stats">${initialStatus}</div>
    <div class="progress"><div class="bar"></div></div>
    <div class="jc-output"></div>
  `;

  const bar     = wrap.querySelector(".bar");
  const statsEl = wrap.querySelector(".jc-stats");
  const outputEl= wrap.querySelector(".jc-output");
  const pauseBtn= wrap.querySelector(".pause-btn");

  pauseBtn.addEventListener("click", async () => {
    pauseBtn.disabled = true;
    statsEl.textContent = "取消中…";
    const id = wrap.dataset.jobId;
    await fetch(`/job/${id}/pause`, { method: "POST" });
  });

  activeJobsEl.prepend(wrap);
  activeJobs.set(jobId, { wrap, bar, statsEl, outputEl, pauseBtn });

  return {
    setJobId(newId) {
      wrap.dataset.jobId = newId;
    },
    setStatus(msg, isError = false) {
      statsEl.textContent = msg;
      if (isError) statsEl.style.color = "var(--red)";
    },
  };
}

// ─── WebSocket per job ────────────────────────────────────────────────────────
function connectWS(jobId, _card) {
  const state = activeJobs.get(jobId);
  if (!state) return;

  const { wrap, bar, statsEl, outputEl, pauseBtn } = state;

  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/${jobId}`);
  state.ws = ws;

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);

    if (msg.type === "segment") {
      outputEl.textContent += msg.text + "\n";
      outputEl.scrollTop = outputEl.scrollHeight;

      const percent = msg.percent ?? (msg.audio_total ? (msg.processed_audio / msg.audio_total * 100) : 0);
      const speed   = msg.speed   ?? (msg.wall_elapsed ? (msg.processed_audio / msg.wall_elapsed) : 0);

      bar.style.width = Math.min(percent, 100).toFixed(1) + "%";
      statsEl.textContent =
        `已跑 ${fmtSec(msg.processed_audio)} / ${fmtSec(msg.audio_total)} `
        + `• ${Number(percent).toFixed(1)}% `
        + `• ${fmtSec(msg.wall_elapsed)} `
        + `• x${Number(speed).toFixed(2)}`;
    }

    if (msg.type === "paused") {
      statsEl.textContent = "已暫停";
      finishCard(jobId, false);
    }

    if (msg.type === "done") {
      bar.style.width = "100%";
      statsEl.textContent = statsEl.textContent + " • 完成 ✅";
      finishCard(jobId, true);
    }

    if (msg.type === "error") {
      statsEl.textContent = "錯誤：" + (msg.message || "unknown");
      statsEl.style.color = "var(--red)";
      finishCard(jobId, false);
    }
  };

  ws.onclose = () => {
    state.ws = null;
  };
}

function finishCard(jobId, success) {
  const state = activeJobs.get(jobId);
  if (!state) return;

  const { wrap, pauseBtn } = state;
  pauseBtn.disabled = true;
  pauseBtn.hidden = true;

  // Move to history after a short delay
  setTimeout(() => {
    wrap.remove();
    activeJobs.delete(jobId);
    if (activeJobsEl.children.length === 0) activeSection.hidden = true;
    loadHistory();
  }, 2000);
}

// ─── history ──────────────────────────────────────────────────────────────────
async function loadHistory() {
  const jobs = await fetch("/jobs").then(r => r.json());
  if (!jobs.length) {
    historyEl.textContent = "沒有紀錄";
    batchDownloadBtn.disabled = true;
    return;
  }

  historyEl.innerHTML = "";
  const doneJobs = jobs.filter(j => j.status === "done");
  batchDownloadBtn.disabled = doneJobs.length === 0;

  for (const j of jobs) {
    const card = document.createElement("div");
    card.className = "job-card";

    const isDone = j.status === "done";
    const wall   = j.wall_total ?? null;
    const avg    = j.avg_speed  ?? null;
    const gpu    = j.gpu_name ?? (j.gpu_id != null ? `GPU ${j.gpu_id}` : "—");

    const checkboxHtml = isDone
      ? `<input type="checkbox" class="jc-check batch-chk" data-job-id="${j.job_id}" />`
      : `<span style="width:16px;display:inline-block"></span>`;

    const txtClass = isDone ? "" : "disabled";
    const srtClass = isDone ? "" : "disabled";

    card.innerHTML = `
      <div class="jc-top">
        <div style="display:flex;align-items:center;gap:8px;min-width:0;flex:1">
          ${checkboxHtml}
          <div class="jc-name" title="${j.filename || ''}">${j.filename || '(unknown)'}</div>
        </div>
        <div class="jc-actions">
          <a href="/download/${j.job_id}.txt" class="${txtClass}" target="_blank">TXT</a>
          <a href="/download/${j.job_id}.srt" class="${srtClass}" target="_blank">SRT</a>
          <button class="rerun">重跑</button>
          <button class="del">刪除</button>
        </div>
      </div>
      <div class="jc-meta">
        狀態: ${j.status}
        • 上傳: ${fmtTime(j.created_at)}
        • 耗時: ${fmtSec(wall)}
        • 平均: x${avg === null ? "—" : Number(avg).toFixed(2)}
        • GPU: ${gpu}
      </div>
    `;

    card.querySelector(".rerun").addEventListener("click", () => resumeJob(j.job_id));
    card.querySelector(".del").addEventListener("click", async () => {
      if (!confirm("確定刪除？")) return;
      await fetch(`/job/${j.job_id}`, { method: "DELETE" });
      loadHistory();
    });

    historyEl.appendChild(card);
  }

  // Update batch download button based on checked items (or all done if none checked)
  updateBatchBtn();
}

function updateBatchBtn() {
  const checked = getCheckedIds();
  const doneCount = historyEl.querySelectorAll(".batch-chk").length;
  batchDownloadBtn.disabled = doneCount === 0;
  batchDownloadBtn.textContent = checked.length > 0
    ? `批次下載 ZIP (${checked.length})`
    : `批次下載 ZIP`;
}

function getCheckedIds() {
  return Array.from(historyEl.querySelectorAll(".batch-chk:checked"))
    .map(el => el.dataset.jobId);
}

historyEl.addEventListener("change", (e) => {
  if (e.target.classList.contains("batch-chk")) updateBatchBtn();
});

batchDownloadBtn.addEventListener("click", () => {
  let ids = getCheckedIds();
  // If nothing checked, download all done jobs
  if (!ids.length) {
    ids = Array.from(historyEl.querySelectorAll(".batch-chk"))
      .map(el => el.dataset.jobId);
  }
  if (!ids.length) return;
  window.location.href = `/download-batch?job_ids=${ids.join(",")}`;
});

// ─── resume ───────────────────────────────────────────────────────────────────
async function resumeJob(oldJobId) {
  const res = await fetch(`/job/${oldJobId}/resume`, { method: "POST" });
  const { job_id } = await res.json();

  // Find original filename from history card
  const oldCard = Array.from(historyEl.querySelectorAll(".job-card"))
    .find(c => c.querySelector(`[data-job-id="${oldJobId}"]`));
  const name = oldCard?.querySelector(".jc-name")?.textContent || "重跑";

  createActiveCard(job_id, name, "重跑開始…");
  connectWS(job_id, null);
}

// ─── init ─────────────────────────────────────────────────────────────────────
refreshBtn.addEventListener("click", loadHistory);
loadHistory();
