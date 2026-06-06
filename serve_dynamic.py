import http.server
import socketserver
import threading
import time
import glob
import os
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).parent

def get_latest_mtime():
    files = glob.glob(str(BASE_DIR / '**' / '*.xlsx'), recursive=True)
    mtimes = []
    for f in files:
        if '~$' not in os.path.basename(f):
            try:
                mtimes.append(os.path.getmtime(f))
            except Exception:
                pass
    return max(mtimes) if mtimes else 0

def file_watcher():
    last_mtime = get_latest_mtime()
    while True:
        time.sleep(3)
        current_mtime = get_latest_mtime()
        if current_mtime > last_mtime:
            print(f"\n[Watcher] Detected changes in Excel files! Regenerating data.js...")
            try:
                 # Re-run extract_data.py to recreate data.js
                 subprocess.run(["python", "extract_data.py"])
                 
                 # Set last_mtime to now so we don't trigger again immediately
                 last_mtime = get_latest_mtime()
                 print("[Watcher] Dashboard data updated successfully. Frontend will auto-reload.")
            except Exception as e:
                 print("[Watcher] Error updating data:", e)

class CustomHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Disable caching to ensure frontend always sees the latest data.js Headers
        self.send_header('Cache-Control', 'no-store, must-revalidate')
        self.send_header('Expires', '0')
        super().end_headers()

if __name__ == "__main__":
    PORT = 8000
    
    # Start the background watcher thread
    watcher_thread = threading.Thread(target=file_watcher, daemon=True)
    watcher_thread.start()
    
    # Start the local HTTP server
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), CustomHTTPRequestHandler) as httpd:
        print("======== DYNAMIC DASHBOARD SERVER ========")
        print(f"Monitoring folder for new/changed Excel files...")
        print(f"Serving dashboard at http://localhost:{PORT}")
        print("==========================================")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down local server.")
