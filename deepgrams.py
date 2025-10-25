#!/usr/bin/env python3
"""
deepgram_stream.py
Stream raw PCM from ffmpeg (stdin) to Deepgram realtime WebSocket and print transcripts.

Usage:
    DEEPGRAM_API_KEY="your_key" python deepgram_stream.py
Or:
    ffmpeg ... pipe:1 | DEEPGRAM_API_KEY="your_key" python deepgram_stream.py
"""

import os
import sys
import asyncio
import websockets
import json
import base64
import signal
from datetime import datetime

# Config
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
if not DEEPGRAM_API_KEY:
    print("Set DEEPGRAM_API_KEY environment variable and re-run.", file=sys.stderr)
    sys.exit(1)

# Choose model and query params as desired
# You can change model=nova (or another) and add parameters like language=en-US, punctuate=true etc.
DG_URL = "wss://api.deepgram.com/v1/listen?encoding=linear16&sample_rate=16000&channels=1&language=en-US&punctuate=true&interim_results=true"
HEADERS = [("Authorization", f"Token {DEEPGRAM_API_KEY}")]

# Chunking: send ~100ms frames (16000 samples/sec * 2 bytes/sample * 0.1s = 3200 bytes)
BYTES_PER_FRAME = 3200

# Output file for transcriptions
OUTPUT_FILE = f"transcript_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

async def ffmpeg_reader_and_stream(websocket):
    """
    Read PCM from stdin (pipe) and send binary frames to Deepgram.
    If you prefer to spawn ffmpeg from here, replace sys.stdin.buffer reads with a subprocess stdout pipe.
    """
    loop = asyncio.get_running_loop()
    read = sys.stdin.buffer.read

    try:
        while True:
            chunk = await loop.run_in_executor(None, read, BYTES_PER_FRAME)
            if not chunk:
                # EOF, break out and then send a "Close" message to let Deepgram finalize
                break
            # Send raw binary audio chunk
            await websocket.send(chunk)
            # small sleep to avoid tight loop if underlying ffmpeg produces slowly
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        pass

async def receive_messages(websocket, output_file):
    """
    Receive transcription messages from Deepgram and print them.
    Deepgram sends JSON text frames with 'type' and 'channel' content.
    """
    try:
        with open(output_file, 'w') as f:
            f.write(f"Transcription started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
        
        async for message in websocket:
            # Messages are JSON strings
            try:
                data = json.loads(message)
            except Exception:
                # If not JSON, just print raw
                print("RAW:", message, flush=True)
                continue

            # Typical payload: { "type": "transcript" or "Results", "channel": {...} }
            msg_type = data.get("type")
            if msg_type == "Results" or msg_type == "transcript":
                channel = data.get("channel", {})
                alternatives = channel.get("alternatives", [])
                if alternatives:
                    alt = alternatives[0]
                    transcript = alt.get("transcript", "")
                    is_final = data.get("is_final", False) or data.get("speech_final", False)
                    confidence = alt.get("confidence")
                    
                    # Only write to file if it's final and has actual text
                    if is_final and transcript.strip():
                        with open(output_file, 'a') as f:
                            f.write(f"{transcript}\n")
                            f.flush()
                        print(f"[FINAL] {transcript} (conf={confidence:.2f})", flush=True)
                    elif transcript.strip():
                        # Still print partials to console
                        print(f"[PARTIAL] {transcript}", flush=True)
            else:
                # Print other events for debugging (but not to file)
                if msg_type != "Metadata":  # Skip metadata spam
                    print("EVENT:", json.dumps(data), flush=True)
        
        # WebSocket loop ended normally (connection closed)
        print(f"\nDEBUG: WebSocket connection closed by server", flush=True)
        print(f"Transcript saved to: {output_file}", flush=True)
    except asyncio.CancelledError:
        print(f"\nDEBUG: receive_messages cancelled", flush=True)
        print(f"Transcript saved to: {output_file}", flush=True)
        pass
    except Exception as e:
        print(f"DEBUG: receive_messages exception: {e}", flush=True)
        import traceback
        traceback.print_exc()

async def keep_alive_sender(websocket):
    """
    Send periodic keepalive JSON messages if there's silence; helps prevent timeouts.
    Deepgram docs recommend sending keepalive when audio is silent. (See docs)
    """
    try:
        while True:
            await asyncio.sleep(10)
            keep_msg = json.dumps({"type": "KeepAlive"})
            await websocket.send(keep_msg)
    except asyncio.CancelledError:
        pass

async def main():
    # Connect
    print("Connecting to Deepgram...", flush=True)
    print(f"Transcript will be saved to: {OUTPUT_FILE}", flush=True)
    async with websockets.connect(DG_URL, additional_headers=HEADERS, max_size=None) as websocket:
        print("Connected.", flush=True)

        # Start tasks: read stdin -> send audio; receive messages; keepalive
        send_task = asyncio.create_task(ffmpeg_reader_and_stream(websocket))
        recv_task = asyncio.create_task(receive_messages(websocket, OUTPUT_FILE))
        ka_task = asyncio.create_task(keep_alive_sender(websocket))

        # Wait until reader finishes (EOF) or tasks error
        done, pending = await asyncio.wait([send_task, recv_task], return_when=asyncio.FIRST_COMPLETED)

        # Debug: which task completed?
        if send_task in done:
            print("DEBUG: send_task completed", flush=True)
            if send_task.exception():
                print(f"DEBUG: send_task exception: {send_task.exception()}", flush=True)
        if recv_task in done:
            print("DEBUG: recv_task completed", flush=True)
            if recv_task.exception():
                print(f"DEBUG: recv_task exception: {recv_task.exception()}", flush=True)

        # If send_task finished (EOF), send a "Close" event, then allow server to finish processing
        if send_task in done:
            try:
                # signal end of audio
                await websocket.send(json.dumps({"type": "Close"}))
            except:
                pass

        # cancel tasks that are still running
        for t in pending:
            t.cancel()
        ka_task.cancel()
        # give some time for any final messages to arrive
        await asyncio.sleep(1)

if __name__ == "__main__":
    # graceful exit on Ctrl-C
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, loop.stop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Interrupted, exiting.")
