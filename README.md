# aio-rtsp

💡 Overview

aio-rtsp is an asynchronous RTSP client library for Python built on top of `asyncio` and `aio-sockets`. Unlike media-player-oriented wrappers, it exposes the protocol and media timeline directly, so you can observe RTSP method latency, RTP packet timing, raw audio/video frame boundaries, and frame-level receive cost in one place.

This library is useful when you need more than just playback: device diagnostics, stream quality analysis, first-frame measurement, packet loss inspection, custom decoders, and embedding RTSP session handling into your own async applications.

✨ Features

Typed Event Stream: Consume connection, RTSP, RTP, video frame, audio frame, and close events from one async iterator.

Protocol Timing Visibility: Measure RTSP connect latency and per-method elapsed time for `OPTIONS`, `DESCRIBE`, `SETUP`, `PLAY`, and `TEARDOWN`.

Frame-Level Metadata: Video and audio frames expose RTP timestamps, sequence ranges, receive cost, corruption hints, and missing sequence information.

H.264 and H.265 Frame Splicing: RTP payloads are reassembled into raw Annex B NAL units that can be passed into decoders such as PyAV.

Audio Frame Support: Includes frame extraction for raw audio, AAC, G.711 A-law, and G.711 mu-law style RTSP audio streams.

Embeddable Session API: Use `RtspSession` directly, integrate it with `async with`, and stop it from your own threading or asyncio control flow.

Tick Semantics: Timing values such as `session_elapsed`, RTSP `elapsed`, and RTP `recv_tick` are process-local monotonic times relative to process startup or session start. They are not wall-clock timestamps.

📦 Installation

Install from PyPI:

```shell
pip install aio-rtsp aio-sockets
```

Optional extras:

```shell
pip install aio-rtsp[audio]
pip install aio-rtsp[decode]
pip install aio-rtsp[playback]
```

Extra meanings:

`[audio]`: installs `numpy` and `sounddevice` for audio playback helpers.

`[decode]`: installs `av` for codecs that require packet decoding, such as AAC or AAC-LATM.

`[playback]`: installs both playback and decode dependencies.

If you are working from a local checkout instead of PyPI, install `aio-sockets` first and run your scripts from the repository root, or add the repository root to `PYTHONPATH`.

## Usage

### Basic Session Example

The example below opens an RTSP session and prints the main protocol and media events.

If the RTSP stream requires authentication, include the credentials in the URL, for example `rtsp://user:password@192.168.1.122:554/stream`. If the username or password contains reserved URL characters such as `@`, `:`, or `/`, encode them first.

```python
import asyncio
import aio_rtsp


async def main():
    async with aio_rtsp.RtspSession("rtsp://127.0.0.1:554/stream", timeout=10) as session:
        async for event in session.iter_events():
            if isinstance(event, aio_rtsp.ConnectResultEvent):
                print("connected:", event.local_addr, "elapsed:", event.elapsed)
            elif isinstance(event, aio_rtsp.RtspMethodEvent):
                print(event.method, event.status_code, event.elapsed)
            elif isinstance(event, aio_rtsp.VideoFrameEvent):
                frame = event.frame
                print("video", frame.timestamp, frame.first_seq.num, frame.last_seq.num)
            elif isinstance(event, aio_rtsp.AudioFrameEvent):
                frame = event.frame
                print("audio", frame.timestamp, frame.sample_rate, frame.sample_count)
            elif isinstance(event, aio_rtsp.ClosedEvent):
                print("session closed after", event.elapsed, "seconds")


asyncio.run(main())
```

### Video-Only or Audio-Only Sessions

You can limit the session to only one media type at setup time.

```python
import asyncio
import aio_rtsp


async def video_only():
    async with aio_rtsp.RtspSession(
        "rtsp://127.0.0.1:554/stream",
        enable_video=True,
        enable_audio=False,
    ) as session:
        async for event in session.iter_events():
            if isinstance(event, aio_rtsp.VideoFrameEvent):
                print("video frame", event.frame.timestamp)


async def audio_only():
    async with aio_rtsp.RtspSession(
        "rtsp://127.0.0.1:554/stream",
        enable_video=False,
        enable_audio=True,
    ) as session:
        async for event in session.iter_events():
            if isinstance(event, aio_rtsp.AudioFrameEvent):
                print("audio frame", event.frame.timestamp)


asyncio.run(video_only())
```

### Stop a Session from Another Thread

`iter_events()` accepts any stop object that provides `is_set()`, so both `threading.Event` and `asyncio.Event` work.

```python
import asyncio
import threading
import aio_rtsp


async def main():
    stop_event = threading.Event()

    async with aio_rtsp.open_session("rtsp://127.0.0.1:554/stream", timeout=10) as session:
        async for event in session.events(stop_event):
            print(event.event, event.session_elapsed)
            if event.session_elapsed > 5:
                stop_event.set()


asyncio.run(main())
```

### Decode Raw Video Frames with PyAV

`VideoFrameEvent.frame.data` contains Annex B byte stream data, which can be passed into PyAV after you initialize the codec from the SDP information.

```python
import asyncio
import fractions
import av
import aio_rtsp


async def main():
    codec = None
    time_base = fractions.Fraction(1, 90000)

    async with aio_rtsp.RtspSession("rtsp://127.0.0.1:554/stream", timeout=10) as session:
        async for event in session.iter_events():
            if isinstance(event, aio_rtsp.RtspMethodEvent) and event.method == "DESCRIBE":
                video_sdp = event.response.sdp.get("video", {})
                codec_name = video_sdp.get("codec_name", "").lower()
                if codec_name:
                    av_codec_name = aio_rtsp.HEVCCodecName if codec_name == aio_rtsp.H265CodecName else codec_name
                    codec = av.CodecContext.create(av_codec_name, "r")
                    time_base = fractions.Fraction(1, video_sdp.get("clock_rate", 90000))
                    for key in ("sps", "pps"):
                        extra = video_sdp.get(key)
                        if extra:
                            codec.parse(extra)

            if isinstance(event, aio_rtsp.VideoFrameEvent) and codec is not None:
                for packet in codec.parse(event.frame.data):
                    packet.pts = packet.dts = event.frame.timestamp
                    packet.time_base = time_base
                    for decoded in codec.decode(packet):
                        print("decoded video frame pts=", decoded.pts)


asyncio.run(main())
```

### Optional Audio Playback Helpers

The package also includes `aio_rtsp.audio_playback.SoundDeviceAudioPlayer`.

- It requires `numpy` and `sounddevice` for playback output.
- It does not require PyAV for G.711 A-law or mu-law playback.
- It does require PyAV for codecs such as AAC or AAC-LATM.

If an optional dependency is missing, the module raises a runtime error with the exact `pip install` command to use.

### PyQt Playback Demo

This repository includes `pyqt_demo.py`, which shows how to consume `RtspSession` events, decode video with PyAV, and play audio with `sounddevice`.

![PyQt Demo](images/pyqt5demo.png)

```shell
python pyqt_demo.py
```

### CLI Decode Demo

This repository also includes `cli_demo.py`, which logs RTSP timing, writes raw video to disk, and decodes frames with PyAV.

```shell
python cli_demo.py -u rtsp://127.0.0.1:554/stream
```

## API

### `RtspSession(rtsp_url, forward_address=None, timeout=4, log_type=RtspClientMsgType.RTSP, enable_video=True, enable_audio=True)`

Create a reusable RTSP session object. Use it with `async with` and consume events through `iter_events()` or `events()`.

At least one of `enable_video` or `enable_audio` must be `True`.

### `RtspSession.iter_events(stop_event=None) -> AsyncGenerator[RtspEvent, None]`

Start the RTSP session, yield typed events, and close the underlying socket when iteration finishes or fails.

### `open_session(rtsp_url, forward_address=None, timeout=4, log_type=RtspClientMsgType.RTSP, enable_video=True, enable_audio=True) -> RtspSession`

Small helper that returns a `RtspSession` instance.

At least one of `enable_video` or `enable_audio` must be `True`.

### `ConnectResultEvent`

Emitted once after TCP connect attempt. Includes `local_addr`, `exception`, `elapsed`, and `success`.

### `RtspMethodEvent`

Emitted for each RTSP method response. Includes `method`, `media_type`, `response`, `status_code`, and `elapsed`.

### `RtpPacketEvent`

Emitted for each interleaved RTP packet. Includes `channel`, `media_channel`, and parsed `rtp`.

### `VideoFrameEvent`

Emitted for each spliced video frame. `frame` is a `VideoFrame` object with RTP timing and sequence metadata.

### `AudioFrameEvent`

Emitted for each extracted audio frame. `frame` is an `AudioFrame` object with sample rate, channel count, and sample count.

### `ClosedEvent`

Emitted once when the session finishes teardown and the RTSP connection is closing.

### Exceptions

`RtspError` is the base exception type.

`RtspConnectionError` is raised when the TCP connection cannot be established.

`RtspTimeoutError` is raised when an RTSP or RTP wait operation times out.

`RtspProtocolError` is raised for parse or transport failures.

`RtspResponseError` is raised when an RTSP method completes with a non-200 status.

## Notes

This project is designed around observability of the RTSP and RTP pipeline, not around replacing FFmpeg or PyAV as a general media stack.

If you only need playback, PyAV or ffmpeg-based wrappers may be simpler. If you need protocol timing, raw RTP frame boundaries, and frame-level receive diagnostics, this library gives you direct access to those details.

The current transport path is RTSP over TCP with interleaved RTP/RTCP.
