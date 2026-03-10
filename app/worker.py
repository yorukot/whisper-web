import os
import time
import subprocess
from typing import Callable, Dict, Any, List

from faster_whisper import WhisperModel
from opencc import OpenCC


class JobCancelled(Exception):
    pass


def ffprobe_duration(path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path
    ]
    out = subprocess.check_output(cmd).decode().strip()
    return float(out)


def ensure_wav_16k_mono(src_path: str, dst_wav_path: str) -> str:
    """
    任何輸入（影片/音訊）都轉成 16k mono wav，最穩。
    """
    cmd = [
        "ffmpeg", "-y", "-i", src_path,
        "-ac", "1", "-ar", "16000",
        "-vn",
        dst_wav_path
    ]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return dst_wav_path


def srt_time(sec: float) -> str:
    ms = int((sec % 1) * 1000)
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = int(sec % 60)
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


class WhisperRunner:
    """
    ⚠️ 這個 runner 不再碰 CUDA_VISIBLE_DEVICES！
    GPU 的選擇由「啟動 subprocess 的那一層」用環境變數控制。

    在 subprocess 裡，CUDA_VISIBLE_DEVICES 通常會只暴露 1 張卡，
    所以這裡 device_index 用 0 就好。
    """
    def __init__(self, model_name: str = "large-v3-turbo", compute_type: str = "float16"):
        self.model = WhisperModel(
            model_name,
            device="cuda",
            device_index=0,
            compute_type=compute_type,
        )
        self.cc = OpenCC("s2t")

    def run(
        self,
        src_path: str,
        job_id: str,
        to_traditional: bool,
        uploads_dir: str,
        outputs_dir: str,
        on_event: Callable[[Dict[str, Any]], None],
        is_cancelled: Callable[[], bool],
    ) -> Dict[str, Any]:
        """
        寫出：
          outputs/{job_id}.txt
          outputs/{job_id}.srt

        回傳統計：
          audio_total, wall_total, avg_speed
        """
        wav_path = os.path.join(uploads_dir, f"{job_id}.wav")
        audio_path = ensure_wav_16k_mono(src_path, wav_path)

        audio_total = ffprobe_duration(audio_path)

        start_wall = time.time()

        segments, _info = self.model.transcribe(
            audio_path,
            language="zh",
            beam_size=5,
            vad_filter=True,
        )

        txt_lines: List[str] = []
        srt_blocks: List[str] = []
        idx = 1
        processed_audio = 0.0

        for seg in segments:
            if is_cancelled():
                raise JobCancelled("cancelled by user")

            text = (seg.text or "").strip()
            if to_traditional:
                text = self.cc.convert(text)

            # TXT：每段一行
            txt_lines.append(text)

            # SRT
            srt_blocks.append(
                f"{idx}\n"
                f"{srt_time(float(seg.start))} --> {srt_time(float(seg.end))}\n"
                f"{text}\n"
            )
            idx += 1

            processed_audio = max(processed_audio, float(seg.end))
            wall_elapsed = time.time() - start_wall
            speed = processed_audio / wall_elapsed if wall_elapsed > 0 else 0.0
            percent = (processed_audio / audio_total * 100.0) if audio_total > 0 else 0.0

            on_event({
                "type": "segment",
                "start": float(seg.start),
                "end": float(seg.end),
                "text": text,
                "audio_total": float(audio_total),
                "processed_audio": float(processed_audio),
                "wall_elapsed": float(wall_elapsed),
                "percent": float(percent),
                "speed": float(speed),
            })

        wall_total = time.time() - start_wall
        avg_speed = (audio_total / wall_total) if wall_total > 0 else 0.0

        txt_out = os.path.join(outputs_dir, f"{job_id}.txt")
        srt_out = os.path.join(outputs_dir, f"{job_id}.srt")

        with open(txt_out, "w", encoding="utf-8") as f:
            f.write("\n".join(txt_lines).strip() + "\n")

        with open(srt_out, "w", encoding="utf-8") as f:
            f.write("\n".join(srt_blocks).strip() + "\n")

        return {
            "audio_total": float(audio_total),
            "wall_total": float(wall_total),
            "avg_speed": float(avg_speed),
        }

