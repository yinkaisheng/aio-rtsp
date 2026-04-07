# aio-rtsp-toolkit

`aio-rtsp-toolkit` is an asyncio-based RTSP toolkit for Python. It provides:

- An RTSP client that exposes connection events, RTSP method timing, RTP packets, and assembled audio/video frames
- A lightweight RTSP/TCP server that publishes local media files from a directory tree

It is aimed at cases where "can play" is not enough and you also want visibility into protocol timing, packet flow, frame boundaries.

## Highlights

- One async event stream for connect, RTSP, RTP, video, audio, and close events
- Per-method timing for `OPTIONS`, `DESCRIBE`, `SETUP`, `PLAY`, and `TEARDOWN`
- H.264 / H.265 video frame splicing to raw Annex B NAL units
- Audio frame extraction for AAC, G.711 A-law, and G.711 mu-law style RTSP streams
- Simple embedded RTSP server for local files
- Session API that works cleanly with `async with`

Timing values such as `session_elapsed`, RTSP `elapsed`, and RTP `recv_tick` are process-local monotonic durations. They are not wall-clock timestamps.

## Installation

Base install:

```shell
pip install aio-rtsp-toolkit
```

Optional extras:

```shell
pip install aio-rtsp-toolkit[server]
pip install aio-rtsp-toolkit[audio]
pip install aio-rtsp-toolkit[all]
```

- `[server]`: installs `av` for RTSP file serving
- `[audio]`: installs `numpy`, `sounddevice`, and `av` for audio playback helpers
- `[all]`: installs all optional dependencies

## Quick Start

### Client

If the RTSP stream requires authentication, include credentials in the URL, for example `rtsp://user:password@192.168.1.122:554/stream`. If the username or password contains reserved characters such as `@`, `:`, or `/`, URL-encode them first.

```python
import asyncio
import aio_rtsp_toolkit as aiortsp


async def main():
    async with aiortsp.RtspSession("rtsp://127.0.0.1:8554/zhongli.wav", timeout=5) as session:
        async for event in session.iter_events():
            if isinstance(event, aiortsp.ConnectResultEvent):
                print("connected:", event.local_addr, "elapsed:", event.elapsed)
            elif isinstance(event, aiortsp.RtspMethodEvent):
                print(event.method, event.status_code, event.elapsed, event.session_elapsed)
            elif isinstance(event, aiortsp.RtpPacketEvent):
                pass
            elif isinstance(event, aiortsp.VideoFrameEvent):
                print("video ts=", event.frame.timestamp, "size=", len(event.frame.data))
            elif isinstance(event, aiortsp.AudioFrameEvent):
                print("audio ts=", event.frame.timestamp, "samples=", event.frame.sample_count)
            elif isinstance(event, aiortsp.ClosedEvent):
                print("closed after", event.session_elapsed, "seconds")


asyncio.run(main())
```

### Server

Publish a directory recursively:

```shell
python server_demo.py --dir ./media --host 0.0.0.0 --port 8554
```

Each file becomes an RTSP resource under the same relative path:

```text
rtsp://127.0.0.1:8554/morning_h264.mp4
rtsp://127.0.0.1:8554/subdir/example.wav
```

Run the server from Python:

```python
import asyncio
import aio_rtsp_toolkit as aiortsp


async def main():
    await aiortsp.serve("./media", host="0.0.0.0", port=8554)


asyncio.run(main())
```

## RTSP Server

Current server behavior:

- Transport is RTSP over TCP with interleaved RTP/RTCP only
- Files are resolved relative to the configured root directory
- Files outside the root are rejected
- Unsupported codecs are skipped rather than transcoded

### Supported Inputs

Accepted file extensions: `.mp4`, `.mkv`, `.wav` and `.aac`.

Supported media handling:

- Video: H.264 and H.265
- AAC audio: served as `mpeg4-generic`
- WAV audio: PCM WAV input is resampled with PyAV and served as `PCMA/8000/1`
- `PCMU` is not advertised by the current server implementation

### Loop Control

Use the `play_count` query parameter:

```text
rtsp://127.0.0.1:8554/zhongli.wav?play_count=1
rtsp://127.0.0.1:8554/zhongli.wav?play_count=2
rtsp://127.0.0.1:8554/zhongli.wav?play_count=0
```

- `play_count=1`: play once, then close
- `play_count=2`: play twice, then close
- `play_count=0`: loop forever
- omitted `play_count`: same as `0`

## RTSP Client

### Session Notes

- Use `enable_video=False` or `enable_audio=False` for single-media sessions
- At least one of `enable_video` or `enable_audio` must be `True`
- `iter_events(stop_event)` accepts `threading.Event` or `asyncio.Event`
- `VideoFrameEvent.frame.data` contains raw Annex B video data

### PyAV Decode Example

Use this pattern if you want to decode video frames after `DESCRIBE`:

```python
import asyncio
import fractions

import av
import aio_rtsp_toolkit as aiortsp


async def main():
    codec = None
    time_base = fractions.Fraction(1, 90000)

    async with aiortsp.RtspSession("rtsp://127.0.0.1:8554/morning_h264.mp4", timeout=5) as session:
        async for event in session.iter_events():
            if isinstance(event, aiortsp.RtspMethodEvent) and event.method == "DESCRIBE":
                video_sdp = event.response.sdp.get("video", {})
                codec_name = video_sdp.get("codec_name", "").lower()
                if codec_name:
                    av_codec_name = aiortsp.HEVCCodecName if codec_name == aiortsp.H265CodecName else codec_name
                    codec = av.CodecContext.create(av_codec_name, "r")
                    time_base = fractions.Fraction(1, video_sdp.get("clock_rate", 90000))
                    for key in ("sps", "pps"):
                        extra = video_sdp.get(key)
                        if extra:
                            codec.parse(extra)

            elif isinstance(event, aiortsp.VideoFrameEvent) and codec is not None:
                for packet in codec.parse(event.frame.data):
                    packet.pts = packet.dts = event.frame.timestamp
                    packet.time_base = time_base
                    for decoded in codec.decode(packet):
                        print("decoded video frame pts=", decoded.pts)


asyncio.run(main())
```

### Optional Audio Playback Helpers

`aio_rtsp_toolkit.audio_playback.SoundDeviceAudioPlayer` can decode and play audio frames.

- Install the `[audio]` extra for `numpy`, `sounddevice`, and `av`
- G.711 A-law and mu-law playback do not require PyAV
- AAC and AAC-LATM playback do require PyAV

## Demos

### PyQt Demo

`pyqt_demo.py` shows how to consume `RtspSession` events, decode video with PyAV, and play audio with `sounddevice`.

It also requires `PyQt5` in addition to the `[audio]` extra.

![PyQt Demo](images/pyqt5demo.gif)

```shell
python pyqt_demo.py
```

### CLI Demo

`cli_demo.py` logs RTSP timing, writes raw video to disk, and decodes frames with PyAV.

Install the `[all]` extra for PyAV, then install `Pillow` if you want to save the first decoded frame as an image.

```shell
python cli_demo.py -u rtsp://127.0.0.1:8554/morning_h264.mp4
```

## API Summary

### `RtspSession(rtsp_url, forward_address=None, timeout=4, log_type=RtspClientMsgType.RTSP, enable_video=True, enable_audio=True)`

High-level reusable RTSP session object. Use it with `async with` and consume events through `iter_events()`.

`forward_address` lets you keep the original RTSP URL while connecting through another TCP endpoint, such as a relay, tunnel, or forwarded host.

### `RtspSession.iter_events(stop_event=None) -> AsyncGenerator[RtspEvent, None]`

Starts the session, yields typed events, and closes the socket when iteration ends or fails.

### `open_session(rtsp_url, forward_address=None, timeout=4, log_type=RtspClientMsgType.RTSP, enable_video=True, enable_audio=True) -> RtspSession`

Convenience helper that returns a `RtspSession`.

### Event Types

- `ConnectResultEvent`
- `RtspMethodEvent`
- `RtpPacketEvent`
- `VideoFrameEvent`
- `AudioFrameEvent`
- `ClosedEvent`

### Exceptions

- `RtspError`
- `RtspConnectionError`
- `RtspTimeoutError`
- `RtspProtocolError`
- `RtspResponseError`

## Notes

If you only need playback, higher-level PyAV or ffmpeg-based wrappers may be simpler. This project is most useful when you need direct access to RTSP/RTP behavior, timing, and frame-level diagnostics.
