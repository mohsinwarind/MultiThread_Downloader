import os
import math
import time
import json
import threading
import requests
import tkinter as tk
from tkinter import ttk, messagebox, filedialog


CHUNK_SIZE = 64 * 1024  
META_EXT = ".meta.json"
PART_EXT = ".part"      

def format_bytes(n):
    """Human friendly byte count."""
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024.0
        if n < 1024.0:
            return f"{n:.2f} {unit}"
    return f"{n:.2f} PB"

class DownloadTask:
    def __init__(self, url, threads, download_folder):
        self.url = url.strip()
        self.threads = max(1, int(threads))
        self.folder = download_folder or "."
        self.file_name = os.path.basename(self.url) or "downloaded.file"
        self.file_path = os.path.join(self.folder, self.file_name)
        self.meta_path = self.file_path + META_EXT

        # state
        self.file_size = None
        self.ranges = []            
        self.part_files = []        
        self.thread_progress = []   
        self.lock = threading.Lock()
        self.total_downloaded = 0
        self.start_time = None
        self.last_update_time = None
        self.last_update_bytes = 0

        # control events
        self.pause_event = threading.Event()   
        self.pause_event.set()
        self.stop_event = threading.Event()    

        self.workers = []  # threads

        # UI-connected (set by GUI)
        self.ui_frame = None
        self.ui_overall_pb = None
        self.ui_speed_lbl = None
        self.ui_thread_pbs = []

        # status
        self.status = "Queued"  # Queued / Running / Paused / Completed / Error / Cancelled

    
    def probe(self):
        """Probe server for content-length and range support."""
        try:
            head = requests.head(self.url, allow_redirects=True, timeout=10)
            cl = head.headers.get("content-length")
            accept_ranges = head.headers.get("accept-ranges", "")
            if cl is None:
                
                r = requests.get(self.url, stream=True, timeout=10)
                cl = r.headers.get("content-length")
            if cl is None:
                return False, "No content-length"
            self.file_size = int(cl)
            supports_range = ("bytes" in accept_ranges.lower()) or head.status_code == 206 or True  # many servers allow range even if header missing; we'll test with an actual ranged GET
            
            return True, None
        except Exception as e:
            return False, str(e)

    def prepare_ranges(self):
        """Generate byte ranges for parts. Handles last part remainder."""
        if not self.file_size:
            return
        part_size = self.file_size // self.threads
        self.ranges = []
        for i in range(self.threads):
            start = i * part_size
            end = self.file_size - 1 if i == self.threads - 1 else (start + part_size - 1)
            self.ranges.append((start, end))

        # Create part file names here so that we can merge it later easily
        self.part_files = [f"{self.file_path}{PART_EXT}{i}" for i in range(len(self.ranges))]
        self.thread_progress = [0] * len(self.ranges)

  
    def load_meta(self):
        """Load metadata if exists (resume support)."""
        if os.path.exists(self.meta_path):
            try:
                with open(self.meta_path, "r") as f:
                    meta = json.load(f)
                # basic validation to check for any exception
                if meta.get("url") == self.url and meta.get("file_size") == self.file_size:
                    # reconstruct part progress
                    saved = meta.get("parts", {})
                    for i, part_path in enumerate(self.part_files):
                        if os.path.exists(part_path):
                            size = os.path.getsize(part_path)
                            self.thread_progress[i] = size
                            self.total_downloaded += size
                        else:
                            self.thread_progress[i] = 0
                    return True
            except Exception:
                pass
        return False

    def save_meta(self):
        """Persist simple meta for resume"""
        try:
            meta = {
                "url": self.url,
                "file_name": self.file_name,
                "file_size": self.file_size,
                "threads": self.threads,
                "parts": [os.path.basename(p) for p in self.part_files],
            }
            with open(self.meta_path, "w") as f:
                json.dump(meta, f)
        except Exception as e:
            print("Failed to save meta:", e)

    def worker(self, idx):
        """Download assigned byte range into part file idx."""
        start, end = self.ranges[idx]
        part_file = self.part_files[idx]

        # If part file exists and is partially downloaded, resume from its size
        existing = 0
        if os.path.exists(part_file):
            existing = os.path.getsize(part_file)
        current_start = start + existing
        if current_start > end:
            # if its already done
            return

        headers = {"Range": f"bytes={current_start}-{end}"}
        try:
            with requests.get(self.url, headers=headers, stream=True, timeout=15) as resp:
                # If server doesn't support range, it may send 200 and full body.Thus, we must handle fallback.
                if resp.status_code not in (200, 206):
                    # Something wrong: mark error
                    self.status = "Error"
                    return

                mode = "ab" if existing else "wb"
                with open(part_file, mode) as pf:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if self.stop_event.is_set():
                            return
                        # Pause handling: block here while paused
                        while not self.pause_event.is_set():
                            time.sleep(0.2)
                            if self.stop_event.is_set():
                                return

                        if not chunk:
                            continue
                        pf.write(chunk)
                        written = len(chunk)
                        with self.lock:
                            self.thread_progress[idx] += written
                            self.total_downloaded += written
                return
        except Exception as e:
            # on network error, allow retry by exiting worker (task-level can restart)
            print(f"Worker {idx} error:", e)
            return

 
    def start(self, on_update_callback=None, on_complete_callback=None):
        """Start/resume the download in background threads.
           on_update_callback will be called periodically by GUI to refresh UI.
        """
        # Basic probe
        ok, err = self.probe()
        if not ok:
            self.status = f"Error: {err}"
            if on_update_callback:
                on_update_callback()
            return

        # prepare ranges and part files
        self.prepare_ranges()
        # create folder if missing
        os.makedirs(self.folder, exist_ok=True)
        # load existing progress if any
        self.load_meta()
        self.save_meta()

        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_update_bytes = self.total_downloaded
        self.status = "Running"

        # spawn workers
        self.stop_event.clear()
        self.pause_event.set()
        self.workers = []
        for i in range(len(self.ranges)):
            t = threading.Thread(target=self.worker, args=(i,), daemon=True)
            t.start()
            self.workers.append(t)

        # spawn monitor thread to call UI updates and detect completion
        monitor = threading.Thread(target=self._monitor_loop, args=(on_update_callback, on_complete_callback), daemon=True)
        monitor.start()

    def _monitor_loop(self, on_update_callback, on_complete_callback):
        """Monitor progress, update UI, and merge parts on completion."""
        while not self.stop_event.is_set():
            # update UI callback
            if on_update_callback:
                on_update_callback()
            # check if all workers finished
            all_dead = all((not t.is_alive()) for t in self.workers)
            if all_dead:
                # verify all parts complete
                finished = True
                for i, (s, e) in enumerate(self.ranges):
                    expected = e - s + 1
                    actual = os.path.getsize(self.part_files[i]) if os.path.exists(self.part_files[i]) else 0
                    if actual < expected:
                        finished = False
                        break
                if finished:
                    # merge
                    try:
                        self._merge_parts()
                        self.status = "Completed"
                        # remove meta + parts
                        try:
                            os.remove(self.meta_path)
                            for p in self.part_files:
                                os.remove(p)
                        except Exception:
                            pass
                        if on_update_callback:
                            on_update_callback()
                        if on_complete_callback:
                            on_complete_callback()
                        return
                    except Exception as e:
                        self.status = f"Error: merge failed ({e})"
                        if on_update_callback:
                            on_update_callback()
                        return
                else:
                    # Some worker ended because of error; try to re-spawn remaining parts if not stopped/paused
                    if not self.stop_event.is_set() and self.pause_event.is_set():
                        # find parts with incomplete sizes and spawn new workers for those (simple retry)
                        new_workers = []
                        for i, (s, e) in enumerate(self.ranges):
                            expected = e - s + 1
                            actual = os.path.getsize(self.part_files[i]) if os.path.exists(self.part_files[i]) else 0
                            if actual < expected:
                                if not any(t.is_alive() for t in self.workers):
                                    # spawn a new worker to finish this part
                                    t = threading.Thread(target=self.worker, args=(i,), daemon=True)
                                    t.start()
                                    new_workers.append(t)
                        if new_workers:
                            self.workers = new_workers
                            continue
                        else:
                            # nothing to respawn -> error out
                            self.status = "Error"
                            if on_update_callback:
                                on_update_callback()
                            return
            time.sleep(0.3)

    def pause(self):
        if self.status != "Running":
            return
        self.pause_event.clear()
        self.status = "Paused"

    def resume(self, on_update_callback=None, on_complete_callback=None):
        if self.status != "Paused":
            return
        self.pause_event.set()
        self.status = "Running"
        # restart any dead workers for unfinished parts
        self.workers = []
        for i, (s, e) in enumerate(self.ranges):
            expected = e - s + 1
            actual = os.path.getsize(self.part_files[i]) if os.path.exists(self.part_files[i]) else 0
            if actual < expected:
                t = threading.Thread(target=self.worker, args=(i,), daemon=True)
                t.start()
                self.workers.append(t)
        monitor = threading.Thread(target=self._monitor_loop, args=(on_update_callback, on_complete_callback), daemon=True)
        monitor.start()

    def cancel(self):
        self.stop_event.set()
        self.pause_event.set()
        self.status = "Cancelled"

    # -----------------------------
    # Merge helper
    # -----------------------------
    def _merge_parts(self):
        tmp_out = self.file_path + ".tmpmerge"
        with open(tmp_out, "wb") as out:
            for p in self.part_files:
                with open(p, "rb") as pf:
                    while True:
                        chunk = pf.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        out.write(chunk)
        # rename
        os.replace(tmp_out, self.file_path)

    # -----------------------------
    # Speed calculation
    # -----------------------------
    def get_speed(self):
        """Return bytes/sec instantaneous moving average over last interval."""
        now = time.time()
        with self.lock:
            cur = self.total_downloaded
        dt = now - self.last_update_time if self.last_update_time else 0.1
        if dt <= 0:
            return 0.0
        speed = (cur - self.last_update_bytes) / dt
        # update last snapshot
        self.last_update_time = now
        self.last_update_bytes = cur
        return speed

class DownloaderAppGUI:
    def __init__(self, root):
        self.root = root
        root.title("Multi-Threading IDM")
        root.geometry("1200x800")
        root.minsize(800, 520)

        # ttk Style - modern-ish
        style = ttk.Style(root)
        style.theme_use("classic")
        style.configure("TButton", padding=6)
        style.configure("Header.TLabel", font=("Segoe UI", 14, "bold"))
        style.configure("Small.TLabel", font=("Segoe UI", 10))
        style.configure("TProgressbar", thickness=16)

        # top frame: add download
        top = ttk.Frame(root, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="URL:", style="Small.TLabel").grid(row=0, column=0, sticky="w")
        self.url_var = tk.StringVar()
        self.url_entry = ttk.Entry(top, textvariable=self.url_var, width=80)
        self.url_entry.grid(row=0, column=1, padx=8, sticky="w")

        ttk.Label(top, text="Threads:", style="Small.TLabel").grid(row=1, column=0, sticky="w", pady=6)
        self.threads_var = tk.IntVar(value=4)
        ttk.Spinbox(top, from_=1, to=16, textvariable=self.threads_var, width=6).grid(row=1, column=1, sticky="w")

        ttk.Label(top, text="Save to:", style="Small.TLabel").grid(row=2, column=0, sticky="w")
        self.save_folder = tk.StringVar(value=os.getcwd())
        ttk.Entry(top, textvariable=self.save_folder, width=60).grid(row=2, column=1, sticky="w", padx=8)
        ttk.Button(top, text="Browse", command=self.browse_folder).grid(row=2, column=2, padx=6)

        ttk.Button(top, text="Add Download", command=self.add_download).grid(row=0, column=2, padx=6)

        # middle frame: downloads list
        middle = ttk.Frame(root, padding=10)
        middle.pack(fill="both", expand=True)

        header = ttk.Label(middle, text="Downloads", style="Header.TLabel")
        header.pack(anchor="w")

        # canvas + scrollbar so we can have dynamic number of downloads
        self.canvas = tk.Canvas(middle)
        self.scrollbar = ttk.Scrollbar(middle, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        # bottom frame: global actions
        bottom = ttk.Frame(root, padding=10)
        bottom.pack(fill="x")
        ttk.Button(bottom, text="Pause All", command=self.pause_all).pack(side="left", padx=6)
        ttk.Button(bottom, text="Resume All", command=self.resume_all).pack(side="left", padx=6)
        ttk.Button(bottom, text="Cancel All", command=self.cancel_all).pack(side="left", padx=6)

        # state
        self.tasks = []  # list of DownloadTask
        self.task_frames = {}  # map task -> frame

    def browse_folder(self):
        folder = filedialog.askdirectory(initialdir=self.save_folder.get())
        if folder:
            self.save_folder.set(folder)

    def add_download(self):
        url = self.url_var.get().strip()
        if not url:
            messagebox.showerror("Error", "Please enter a valid URL")
            return
        threads = self.threads_var.get()
        folder = self.save_folder.get()
        task = DownloadTask(url, threads, folder)
        self.tasks.append(task)
        self._create_task_ui(task)
        # start automatically
        t = threading.Thread(target=task.start, args=(lambda: self.update_task_ui(task), lambda: self.on_task_complete(task)), daemon=True)
        t.start()

    def _create_task_ui(self, task: DownloadTask):
        frame = ttk.Frame(self.scrollable_frame, relief="ridge", padding=8)
        frame.pack(fill="x", pady=6, padx=4)

        toprow = ttk.Frame(frame)
        toprow.pack(fill="x")
        ttk.Label(toprow, text=task.file_name, style="Small.TLabel").pack(side="left")
        status_lbl = ttk.Label(toprow, text=task.status, style="Small.TLabel")
        status_lbl.pack(side="right")
        task.status_label = status_lbl

        # overall progress
        overall_pb = ttk.Progressbar(frame, length=650, mode="determinate")
        overall_pb.pack(pady=6)
        task.ui_overall_pb = overall_pb

        info_row = ttk.Frame(frame)
        info_row.pack(fill="x")
        speed_lbl = ttk.Label(info_row, text="Speed: 0 B/s", style="Small.TLabel")
        speed_lbl.pack(side="left")
        task.ui_speed_lbl = speed_lbl

        # per-thread progressbars container (collapsible might be an enhancement)
        threads_frame = ttk.Frame(frame)
        threads_frame.pack(fill="x", pady=6)
        task.ui_thread_pbs = []
        for i in range(task.threads):
            lbl = ttk.Label(threads_frame, text=f"T{i+1}", width=4)
            lbl.grid(row=i, column=0, sticky="w", padx=2, pady=2)
            pb = ttk.Progressbar(threads_frame, length=520, mode="determinate")
            pb.grid(row=i, column=1, padx=6, pady=2, sticky="w")
            task.ui_thread_pbs.append(pb)

        # control buttons
        btn_row = ttk.Frame(frame)
        btn_row.pack(fill="x", pady=4)
        pause_btn = ttk.Button(btn_row, text="Pause", command=lambda t=task: self.toggle_pause(t))
        pause_btn.pack(side="left", padx=6)
        cancel_btn = ttk.Button(btn_row, text="Cancel", command=lambda t=task: self.cancel_task(t))
        cancel_btn.pack(side="left", padx=6)
        open_btn = ttk.Button(btn_row, text="Open Folder", command=lambda p=task.folder: os.startfile(p) if os.name == "nt" else os.system(f'xdg-open "{p}"'))
        open_btn.pack(side="right", padx=6)

        # bind ui references on task
        task.ui_frame = frame
        task.ui_pause_btn = pause_btn
        self.task_frames[task] = frame

    def update_task_ui(self, task: DownloadTask):
        """Called from background threads; schedule UI update via after."""
        def do_update():
            # update status label
            task.status_label.config(text=task.status)
            # set overall progress max if known
            if task.file_size:
                task.ui_overall_pb.config(maximum=task.file_size)
                task.ui_overall_pb['value'] = task.total_downloaded

            # update per-thread progress bars
            for i, pb in enumerate(task.ui_thread_pbs):
                # expected range size
                if i < len(task.ranges):
                    s, e = task.ranges[i]
                    expected = e - s + 1
                    pb.config(maximum=expected)
                    pb['value'] = task.thread_progress[i]
                else:
                    pb.config(maximum=1)
                    pb['value'] = 0

            # speed
            sp = task.get_speed()
            task.ui_speed_lbl.config(text=f"Speed: {format_bytes(int(sp))}/s")

            # update pause button text
            if task.status == "Paused":
                task.ui_pause_btn.config(text="Resume")
            elif task.status == "Running":
                task.ui_pause_btn.config(text="Pause")
            else:
                task.ui_pause_btn.config(text="Pause")
        try:
            self.root.after(0, do_update)
        except Exception:
            pass

    def on_task_complete(self, task: DownloadTask):
        messagebox.showinfo("Download Complete", f"{task.file_name} finished.")
        # final UI update
        self.update_task_ui(task)

    # Controls
    def toggle_pause(self, task: DownloadTask):
        if task.status == "Running":
            task.pause()
            self.update_task_ui(task)
        elif task.status == "Paused":
            task.resume(on_update_callback=lambda: self.update_task_ui(task), on_complete_callback=lambda: self.on_task_complete(task))
            self.update_task_ui(task)

    def cancel_task(self, task: DownloadTask):
        if messagebox.askyesno("Cancel", f"Cancel download {task.file_name}?"):
            task.cancel()
            # cleanup part files and meta
            try:
                for p in task.part_files:
                    if os.path.exists(p):
                        os.remove(p)
                if os.path.exists(task.meta_path):
                    os.remove(task.meta_path)
            except Exception:
                pass
            task.status = "Cancelled"
            self.update_task_ui(task)

    def pause_all(self):
        for t in self.tasks:
            t.pause()
            self.update_task_ui(t)

    def resume_all(self):
        for t in self.tasks:
            t.resume(on_update_callback=lambda t=t: self.update_task_ui(t), on_complete_callback=lambda t=t: self.on_task_complete(t))

    def cancel_all(self):
        if messagebox.askyesno("Cancel All", "Cancel all downloads?"):
            for t in self.tasks:
                t.cancel()
                self.update_task_ui(t)

if __name__ == "__main__":
    root = tk.Tk()
    app = DownloaderAppGUI(root)
    root.mainloop()
