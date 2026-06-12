# aio-rtsp-toolkit

[English](README.md)

`aio-rtsp-toolkit` 是一个基于 asyncio 的 Python RTSP 工具包，提供：

- RTSP 客户端：暴露连接事件、RTSP 方法耗时、RTP 数据包，以及组装好的音视频帧
- 轻量级 RTSP/TCP 服务器：从目录树发布本地媒体文件

适用于不仅需要「能播放」，还需要观察协议时序、数据包流向和帧边界的场景。

## 特性

- 单一异步事件流，涵盖连接、RTSP、RTP、视频、音频与关闭事件
- 对 `OPTIONS`、`DESCRIBE`、`SETUP`、`PLAY`、`TEARDOWN` 等方法分别计时
- H.264 / H.265 视频帧拼接为原始 Annex B NAL 单元
- 从 AAC、G.711 A-law、G.711 mu-law 等 RTSP 流中提取音频帧
- 用于本地文件的简易嵌入式 RTSP 服务器
- 可与 `async with` 顺畅配合的会话 API

`session_elapsed`、RTSP `elapsed`、RTP `recv_tick` 等时间值为进程内单调时钟时长，不是墙钟时间戳。

## 安装

基础安装：

```shell
pip install aio-rtsp-toolkit
```

会安装三个命令行入口：

- `rtsp-client-cli`
- `rtsp-client-pyqt5`
- `rtsp-server`

在 Windows 上，它们会安装到当前 Python 环境的 `Scripts` 目录下的 `.exe` 启动器；在 Linux 和 macOS 上，则安装到当前 Python 环境的 `bin` 目录。

可选扩展：

```shell
pip install aio-rtsp-toolkit[server]
pip install aio-rtsp-toolkit[decode]
pip install aio-rtsp-toolkit[audio]
pip install aio-rtsp-toolkit[all]
```

- `[server]`：安装 `av`，用于 RTSP 文件服务
- `[decode]`：安装 `numpy` 和 `av`，用于音频 PCM 解码辅助
- `[audio]`：安装 `numpy`、`sounddevice` 和 `av`，用于音频播放辅助
- `[all]`：安装全部可选依赖

在 Linux 上，音频播放除 Python 包 `sounddevice` 外，还需要系统 PortAudio 运行时。未安装 PortAudio 时，`rtsp-client-pyqt5` 和 `rtsp-client-cli --audio-mode play` 会在运行时失败。

在 Ubuntu 上可这样安装：

```shell
sudo apt update
sudo apt install -y libportaudio2
```

## 快速开始

### 客户端

若 RTSP 流需要认证，请在 URL 中包含凭据，例如 `rtsp://user:password@192.168.1.122:554/stream`。若用户名或密码包含 `@`、`:`、`/` 等保留字符，请先进行 URL 编码。

`transport="udp"` 用于服务器以 UDP 单播发送 RTP（例如 VLC 默认 RTSP 模式）。默认值为 `transport="tcp"`。

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

### 服务器

递归发布一个目录：

```shell
rtsp-server --dir ./media --host 0.0.0.0 --port 8554
```

每个文件都会成为相同相对路径下的 RTSP 资源：

```text
rtsp://127.0.0.1:8554/morning_h264.mp4
rtsp://127.0.0.1:8554/subdir/example.wav
```

在 Python 中运行服务器：

```python
import asyncio
import aio_rtsp_toolkit.server as server


async def main():
    await server.serve("./media", host="0.0.0.0", port=8554)


asyncio.run(main())
```

## RTSP 服务器

当前服务器行为：

- 传输方式支持 RTSP/TCP 交织 RTP/RTCP，以及 UDP 单播 RTP/AVP
- 文件相对于配置根目录解析
- 拒绝根目录外的文件
- 不支持的编解码器会跳过，不会转码
- 未实现会话超时协商
- 对已配置轨道发送最简 RTCP 发送方报告（SR）与 BYE 包

### 支持的输入

接受的文件扩展名：`.mp4`、`.mkv`、`.wav`、`.aac`。

支持的媒体处理：

- 视频：H.264 与 H.265
- AAC 音频：以 `mpeg4-generic` 提供
- WAV 音频：PCM WAV 输入经 PyAV 重采样后，以 `PCMA/8000/1` 提供
- 当前服务器实现不会宣告 `PCMU`

### 循环控制

使用 `play_count` 查询参数：

```text
rtsp://127.0.0.1:8554/zhongli.wav?play_count=1
rtsp://127.0.0.1:8554/zhongli.wav?play_count=2
rtsp://127.0.0.1:8554/zhongli.wav?play_count=0
```

- `play_count=1`：播放一次后关闭
- `play_count=2`：播放两次后关闭
- `play_count=0`：无限循环
- `play_count<0` 或未指定 `play_count`：播放一次后关闭

## RTSP 客户端

### 会话说明

- `transport="udp"` 使用 UDP 单播 RTP/AVP；默认为 `transport="tcp"`（交织 RTP/RTCP）
- 单路媒体会话可使用 `enable_video=False` 或 `enable_audio=False`
- `enable_video` 与 `enable_audio` 至少有一个必须为 `True`
- `iter_events(stop_event)` 可接受 `threading.Event` 或 `asyncio.Event`
- `VideoFrameEvent.frame.data` 包含原始 Annex B 视频数据

### PyAV 解码示例

若要在 `DESCRIBE` 之后解码视频帧，可使用如下模式：

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

### 可选音频解码 / 播放辅助

`aio_rtsp_toolkit.audio_decode.AudioPCMDecoder` 可将音频帧解码为 PCM `s16le`。

- `feed()` 返回 `PCMFrame` 对象列表
- `PCMFrame.pcm_bytes` 包含交错的 PCM 字节
- 设置 `output_sample_rate` 可重采样到目标采样率
- `output_sample_rate=None` 则保持源采样率

`aio_rtsp_toolkit.audio_playback.SoundDeviceAudioPlayer` 在 `audio_decode` 解码器之上构建，并立即播放解码后的 PCM。

- 仅 PCM 解码请安装 `[decode]` 扩展；解码 + 播放请安装 `[audio]`
- G.711 A-law 与 mu-law 播放不需要 PyAV
- AAC 与 AAC-LATM 播放需要 PyAV
- Linux 上播放还需要系统 PortAudio 运行时

## 示例

### PyQt 示例

`pyqt_demo.py` 演示如何消费 `RtspSession` 事件、用 PyAV 解码视频，并用 `sounddevice` 播放音频。

除 `[audio]` 扩展外，还需要安装 `PyQt5`；Linux 上还需要系统 PortAudio 运行时。

![PyQt Demo](images/pyqt5demo.gif)

```shell
rtsp-client-pyqt5
```

### CLI 示例

`cli_demo.py` 记录 RTSP 耗时、将原始视频写入磁盘，并用 PyAV 解码帧。

你仍可在脚本中修改默认源地址，同时也支持运行时控制：

```shell
rtsp-client-cli -u rtsp://127.0.0.1:8554/morning_h264.mp4
rtsp-client-cli -u rtsp://127.0.0.1:8554/morning_h264.mp4 --audio-mode decode --audio-rate 16000
rtsp-client-cli -u rtsp://127.0.0.1:8554/morning_h264.mp4 --save-audio "" # disable audio saving
rtsp-client-cli -u rtsp://127.0.0.1:8554/morning_h264.mp4 --audio-mode play --audio-rate 16000 --session-id s01 --log-prefix cam01
```

- `--audio-mode decode`：将音频解码为 PCM
- `--audio-mode play`：解码并播放音频
- `--audio-rate 16000`：将输出 PCM 重采样到 `16000`
- 省略 `--audio-rate`：不重采样，保持源采样率
- `--save-audio audio.wav`：将解码音频保存为 WAV，默认为 `audio.wav`
- `--save-audio ""`：禁用音频保存
- `--session-id s01`：设置 RTSP/音频日志使用的会话 ID
- `--log-prefix cam01`：设置 RTSP/音频日志使用的日志前缀

安装 `[all]` 扩展以使用 PyAV；若要将首帧解码结果保存为图片，还需安装 `Pillow`。Linux 上 `--audio-mode play` 同样需要系统 PortAudio 运行时。

```shell
rtsp-client-cli -u rtsp://127.0.0.1:8554/morning_h264.mp4
```

## API 摘要

### `RtspSession(rtsp_url, forward_address=None, timeout=4, session_id=None, enable_video=True, enable_audio=True, transport="tcp", log_type=RtspClientMsgType.RTSP, log_prefix='')`

高层可复用 RTSP 会话对象。配合 `async with` 使用，通过 `iter_events()` 消费事件。

`transport` 选择 SETUP 时的 RTP 传输方式：`"tcp"` 为 RTSP 连接上的交织 RTP/RTCP（默认）；`"udp"` 为 UDP 单播 RTP/AVP。

`forward_address` 可在保留原始 RTSP URL 的同时，通过另一 TCP 端点连接（例如中继、隧道或转发主机）。

### `RtspSession.iter_events(stop_event=None) -> AsyncGenerator[RtspEvent, None]`

启动会话，产出类型化事件，并在迭代结束或失败时关闭套接字。

### `open_session(rtsp_url, forward_address=None, timeout=4, session_id=None, enable_video=True, enable_audio=True, transport="tcp", log_type=RtspClientMsgType.RTSP, log_prefix='') -> RtspSession`

返回 `RtspSession` 的便捷辅助函数。

### 事件类型

- `ConnectResultEvent`
- `RtspMethodEvent`
- `RtpPacketEvent`
- `VideoFrameEvent`
- `AudioFrameEvent`
- `ClosedEvent`

### 异常

- `RtspError`
- `RtspConnectionError`
- `RtspTimeoutError`
- `RtspProtocolError`
- `RtspResponseError`

## 说明

若你只需要播放功能，更高层的 PyAV 或 ffmpeg 封装可能更简单。本项目最适合需要直接访问 RTSP/RTP 行为、时序与帧级诊断信息的场景。
