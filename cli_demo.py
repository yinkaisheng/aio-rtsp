#!/usr/bin/env python3

import argparse
import asyncio
import fractions
import traceback
import wave
from typing import Optional, Set, Tuple

import av

import aio_sockets as aio
import aio_rtsp_toolkit as aiortsp
from aio_rtsp_toolkit.tick import Tick
import aio_rtsp_toolkit.audio_decode as audio_decode
import aio_rtsp_toolkit.audio_playback as audio_playback

from log_util import Fore, config_logger, logger


aio.aio_sockets.logger = logger
aiortsp.types.logger = logger


DEFAULT_URL = "rtsp://127.0.0.1:8554/morning_h264.mp4"
AUDIO_MODE = "decode"
AUDIO_OUTPUT_SAMPLE_RATE = None
AUDIO_SAVE_AUDIO_PATH = "audio.wav"
DEFAULT_SESSION_ID = "s01"
DEFAULT_LOG_PREFIX = "cam01"


class WaveAudioSaver:
    def __init__(self, file_path: str, session_id: str = "", log_prefix: str = "") -> None:
        self.file_path = file_path
        self.session_id = session_id
        self.log_prefix = log_prefix
        self.writer: Optional[wave.Wave_write] = None
        self.sample_rate = 0
        self.channels = 0

    def write(self, pcm_frame: audio_decode.PCMFrame) -> None:
        if self.writer is None:
            self.writer = wave.open(self.file_path, "wb")
            self.writer.setnchannels(pcm_frame.channels)
            self.writer.setsampwidth(2)
            self.writer.setframerate(pcm_frame.sample_rate)
            self.sample_rate = pcm_frame.sample_rate
            self.channels = pcm_frame.channels
            log_tag = f"[{self.log_prefix}|{self.session_id}] " if self.session_id and self.log_prefix else f"[{self.session_id}] " if self.session_id else ""
            logger.info(
                f"{log_tag}{Tick.process_tick():.3f} {Fore.Cyan}save audio enabled{Fore.Reset}, "
                f"path={self.file_path}, sample_rate={self.sample_rate}, channels={self.channels}"
            )
        elif self.sample_rate != pcm_frame.sample_rate or self.channels != pcm_frame.channels:
            raise ValueError(
                "WAV output format changed unexpectedly: "
                f"{self.sample_rate}/{self.channels} -> {pcm_frame.sample_rate}/{pcm_frame.channels}"
            )
        self.writer.writeframes(pcm_frame.pcm_bytes)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None


async def run_demo(
    rtsp_url: str,
    forward_address: Optional[Tuple[str, int]],
    play_time: int,
    timeout: int,
    save_first_frame_path: str,
    audio_mode: str,
    audio_output_sample_rate: Optional[int],
    save_audio_path: str,
    session_id: Optional[str],
    log_prefix: str,
) -> None:
    stop_event = asyncio.Event()
    codec: Optional[av.codec.CodecContext] = None
    codec_name_lower = ""
    video_time_base = fractions.Fraction(1, 90000)
    first_decoded_tick = 0.0
    last_video_timestamp: Optional[int] = None
    decoded_timestamps: Set[int] = set()
    first_frame_saved = False
    decode_fail_count = 0
    video_rtp_count = 0
    audio_rtp_count = 0
    log_every_count = 30
    audio_pcm_count = 0
    audio_handler: audio_decode.AudioPCMDecoder
    audio_saver: Optional[WaveAudioSaver] = None

    if audio_mode == "play":
        audio_handler = audio_playback.SoundDeviceAudioPlayer(
            output_sample_rate=audio_output_sample_rate,
            session_id=session_id,
            log_prefix=log_prefix,
        )
    else:
        audio_handler = audio_decode.AudioPCMDecoder(
            output_sample_rate=audio_output_sample_rate,
            session_id=session_id,
            log_prefix=log_prefix,
        )

    if save_audio_path:
        audio_saver = WaveAudioSaver(save_audio_path, session_id=session_id or "", log_prefix=log_prefix)

    log_tag = f"[{log_prefix}|{session_id}] " if session_id and log_prefix else f"[{session_id}] " if session_id else ""

    logger.info(
        f"{log_tag}{Tick.process_tick():.3f} {Fore.Cyan}audio mode={audio_mode}{Fore.Reset}, "
        f"output_sample_rate={audio_output_sample_rate or 'source'}, "
        f"save_audio={save_audio_path or 'disabled'}"
    )

    try:
        async with aiortsp.open_session(
            rtsp_url,
            forward_address=forward_address,
            timeout=timeout,
            session_id=session_id,
            log_prefix=log_prefix,
            log_type=aiortsp.RtspClientMsgType.Exception
            | aiortsp.RtspClientMsgType.ConnectResult
            | aiortsp.RtspClientMsgType.Closed
            | aiortsp.RtspClientMsgType.RTSP,
        ) as session:
            session = session  # type: aiortsp.RtspSession
            async for event in session.iter_events(stop_event):
                if isinstance(event, aiortsp.ConnectResultEvent):
                    if event.success:
                        logger.info(
                            f"{Tick.process_tick():.3f} {Fore.Green}RTSP connected{Fore.Reset}, "
                            f"local_addr={event.local_addr}, elapsed={event.elapsed}s"
                        )
                    else:
                        logger.error(
                            f"{Tick.process_tick():.3f} {Fore.Red}RTSP connect failed{Fore.Reset}, "
                            f"exception={event.exception!r}"
                        )
                        break
                elif isinstance(event, aiortsp.RtspMethodEvent):
                    logger.info(
                        f"{Tick.process_tick():.3f} {Fore.Green}RTSP {event.method}{Fore.Reset}, "
                        f"status={event.status_code}, elapsed={event.elapsed:.3f}s"
                    )
                    if event.method == "DESCRIBE" and isinstance(event.response, aiortsp.RtspResponse):
                        video_sdp = event.response.sdp.get("video") if event.response.sdp else None
                        if video_sdp:
                            video_clock_rate = video_sdp["clock_rate"]
                            video_time_base = fractions.Fraction(1, video_clock_rate)
                            codec_name_lower = str(video_sdp["codec_name"]).lower()
                            av_codec_name = "hevc" if codec_name_lower == aiortsp.H265CodecName else codec_name_lower
                            codec = av.CodecContext.create(av_codec_name, "r")
                        audio_sdp = event.response.sdp.get("audio") if event.response.sdp else None
                        if audio_sdp:
                            audio_handler.configure(audio_sdp)
                elif isinstance(event, aiortsp.RtpPacketEvent):
                    if event.media_channel == "video_rtp":
                        video_rtp_count += 1
                        if video_rtp_count % log_every_count == 1:
                            logger.info(
                                f"{Tick.process_tick():.3f} {Fore.Green}video rtp{Fore.Reset} "
                                f"seq={event.rtp.sequence_number} ts={event.rtp.timestamp}"
                            )
                    elif event.media_channel == "audio_rtp":
                        audio_rtp_count += 1
                        if audio_rtp_count % log_every_count == 1:
                            logger.info(
                                f"{Tick.process_tick():.3f} {Fore.Green}audio rtp{Fore.Reset} "
                                f"seq={event.rtp.sequence_number} ts={event.rtp.timestamp}"
                            )
                elif isinstance(event, aiortsp.VideoFrameEvent):
                    vframe = event.frame
                    if vframe is None:
                        continue
                    logger.info(
                        f"{Tick.process_tick():.3f} {Fore.Green}video frame{Fore.Reset} "
                        f"ts={vframe.timestamp} nalu_type={vframe.nalu_type} size={len(vframe.data)} "
                        f"corrupt={vframe.is_corrupt}"
                    )
                    if codec is None:
                        continue
                    if codec_name_lower == aiortsp.H264CodecName:
                        if not (aiortsp.H264RTPNalUnitType.IDR <= vframe.nalu_type <= aiortsp.H264RTPNalUnitType.PPS):
                            continue
                    elif codec_name_lower == aiortsp.H265CodecName:
                        if not (aiortsp.H265RTPNalUnitType.BLA_W_LP <= vframe.nalu_type <= aiortsp.H265RTPNalUnitType.SUFFIX_SEI_NUT):
                            continue
                    packets = codec.parse(vframe.data)
                    pts = vframe.timestamp
                    duration = 0 if last_video_timestamp is None else max(0, pts - last_video_timestamp)
                    last_video_timestamp = pts
                    for packet in packets:
                        packet.pts = packet.dts = pts
                        packet.duration = duration
                        packet.time_base = video_time_base
                        try:
                            decoded_frames = codec.decode(packet)
                        except Exception as ex:
                            decode_fail_count += 1
                            logger.error(
                                f"{Tick.process_tick():.3f} {Fore.Red}video decode error{Fore.Reset}, "
                                f"pts={packet.pts}, ex={ex!r}"
                            )
                            continue
                        for decoded_frame in decoded_frames:
                            decoded_timestamps.add(int(decoded_frame.pts or 0))
                            if not first_frame_saved:
                                rgb_array = decoded_frame.to_ndarray(format="rgb24")
                                import PIL.Image
                                image = PIL.Image.fromarray(rgb_array)
                                image.save(save_first_frame_path, "JPEG")
                                first_frame_saved = True
                                logger.info(
                                    f"{Tick.process_tick():.3f} {Fore.Cyan}saved first frame{Fore.Reset}, "
                                    f"path={save_first_frame_path}"
                                )
                            if first_decoded_tick == 0:
                                first_decoded_tick = aiortsp.Tick.process_tick()
                elif isinstance(event, aiortsp.AudioFrameEvent):
                    aframe = event.frame
                    if aframe is None:
                        continue
                    logger.info(
                        f"{Tick.process_tick():.3f} {Fore.Green}audio frame{Fore.Reset} "
                        f"codec={aframe.codec_name_lower} ts={aframe.timestamp} size={len(aframe.data)}"
                    )
                    pcm_frames = audio_handler.feed(aframe)
                    for pcm_frame in pcm_frames:
                        audio_pcm_count += 1
                        if audio_saver is not None:
                            audio_saver.write(pcm_frame)
                        if first_decoded_tick == 0:
                            first_decoded_tick = aiortsp.Tick.process_tick()
                elif isinstance(event, aiortsp.RtspTimeoutError):
                    logger.error(
                        f"{Tick.process_tick():.3f} {Fore.Red}rtsp timeout{Fore.Reset}: {event!r} "
                        f"session_elapsed={event.session_elapsed}\n"
                        f"{''.join(traceback.format_exception(type(event), event, event.__traceback__))}"
                        "the above exception was handled"
                    )
                    break
                elif isinstance(event, aiortsp.RtspProtocolError):
                    logger.error(
                        f"{Tick.process_tick():.3f} {Fore.Red}rtsp protocol error{Fore.Reset}: {event!r} "
                        f"session_elapsed={event.session_elapsed}\n"
                        f"{''.join(traceback.format_exception(type(event), event, event.__traceback__))}"
                        "the above exception was handled"
                    )
                    break
                elif isinstance(event, aiortsp.RtspResponseError):
                    logger.error(
                        f"{Tick.process_tick():.3f} {Fore.Red}rtsp response error{Fore.Reset}: {event!r} "
                        f"session_elapsed={event.session_elapsed}\n"
                        f"{''.join(traceback.format_exception(type(event), event, event.__traceback__))}"
                        "the above exception was handled"
                    )
                    break
                elif isinstance(event, Exception):
                    logger.error(
                        f"{Tick.process_tick():.3f} {Fore.Red}other exception{Fore.Reset}: {event!r}\n"
                        f"{''.join(traceback.format_exception(type(event), event, event.__traceback__))}"
                        "the above exception was handled"
                    )
                    break
                elif isinstance(event, aiortsp.ClosedEvent):
                    message = (
                        f"{Tick.process_tick():.3f} {Fore.Green}session closed{Fore.Reset}, "
                        f"reason={event.reason}, exception={event.exception!r}"
                    )
                    if event.exception is not None:
                        message += (
                            "\n"
                            f"{''.join(traceback.format_exception(type(event.exception), event.exception, event.exception.__traceback__))}"
                        )
                    logger.info(message)
                    break

                if first_decoded_tick > 0 and aiortsp.Tick.process_tick() - first_decoded_tick >= play_time:
                    # await session.close()
                    if not stop_event.is_set():
                        logger.info(f"{Tick.process_tick():.3f} {Fore.Cyan}trigger stop{Fore.Reset}")
                        stop_event.set()
    finally:
        if audio_saver is not None:
            audio_saver.close()
        audio_handler.close()

    logger.info(
        f"{Tick.process_tick():.3f} done, video_rtp_count={video_rtp_count}, "
        f"audio_rtp_count={audio_rtp_count}, decoded_frames={len(decoded_timestamps)}, "
        f"audio_pcm_frames={audio_pcm_count}, decode_fail_count={decode_fail_count}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", default=DEFAULT_URL, help=f"rtsp url, default: {DEFAULT_URL}")
    parser.add_argument(
        "-f",
        "--forward",
        default="",
        help="forward address, e.g. 127.0.0.1:8080; default disabled",
    )
    parser.add_argument("-t", "--time", type=int, default=5, help="play time after first decoded frame")
    parser.add_argument("-to", "--timeout", type=int, default=5, help="network timeout")
    parser.add_argument(
        "--save-first-frame",
        default="first_frame.jpg",
        help="path for the first decoded video frame JPEG",
    )
    parser.add_argument(
        "--audio-mode",
        choices=("decode", "play"),
        default=AUDIO_MODE,
        help=f"audio handling mode, default: {AUDIO_MODE}",
    )
    parser.add_argument(
        "--audio-rate",
        type=int,
        default=AUDIO_OUTPUT_SAMPLE_RATE,
        help="output pcm sample rate, default keeps source sample rate",
    )
    parser.add_argument(
        "--save-audio",
        default=AUDIO_SAVE_AUDIO_PATH,
        help=f"save decoded audio to wav, default: {AUDIO_SAVE_AUDIO_PATH}; use empty string to disable",
    )
    parser.add_argument(
        "--session-id",
        default=DEFAULT_SESSION_ID,
        help=f"session id used for RTSP/audio logs, default: {DEFAULT_SESSION_ID}",
    )
    parser.add_argument(
        "--log-prefix",
        default=DEFAULT_LOG_PREFIX,
        help=f"log prefix used for RTSP/audio logs, default: {DEFAULT_LOG_PREFIX}",
    )
    args = parser.parse_args()

    forward_addr = None
    if args.forward:
        index = args.forward.rfind(":")
        if index <= 0 or index == len(args.forward) - 1:
            raise ValueError(f"invalid --forward value: {args.forward!r}")
        forward_addr = (args.forward[:index], int(args.forward[index + 1 :]))

    config_logger(logger, "info")
    logger.info(args)
    asyncio.run(
        run_demo(
            rtsp_url=args.url,
            forward_address=forward_addr,
            play_time=args.time,
            timeout=args.timeout,
            save_first_frame_path=args.save_first_frame,
            audio_mode=args.audio_mode,
            audio_output_sample_rate=args.audio_rate,
            save_audio_path=args.save_audio,
            session_id=args.session_id,
            log_prefix=args.log_prefix,
        )
    )


if __name__ == "__main__":
    main()
