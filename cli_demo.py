#!/usr/bin/env python3

import asyncio
import io
import fractions
import queue
import traceback
import threading
import wave
from typing import Any, List, Optional

import av
import PIL.Image

import aio_sockets as aio
import aio_rtsp_toolkit as aiortsp
from aio_rtsp_toolkit import (AudioFrameEvent, ClosedEvent, ConnectResultEvent, RtpPacketEvent, RtspMethodEvent,
                              Tick, VideoFrameEvent, open_session)
from log_util import Fore, logger, config_logger


aiortsp.client.logger = logger


# 'decode': decode audio to PCM
# 'play': decode audio and play it immediately
AUDIO_MODE = 'decode'
# e.g. 16000; None means keep the source sample rate and do not resample
AUDIO_OUTPUT_SAMPLE_RATE = None
AUDIO_SAVE_AUDIO_PATH = 'audio.wav'
DEFAULT_SESSION_ID = 's01'
DEFAULT_LOG_PREFIX = 'cam01'


class WaveAudioSaver:
    def __init__(self, file_path: str, session_id: str = '', log_prefix: str = ''):
        self.file_path = file_path
        self.session_id = session_id
        self.log_prefix = log_prefix
        self.writer: Optional[wave.Wave_write] = None
        self.sample_rate = 0
        self.channels = 0

    def write(self, pcm_frame) -> None:
        if self.writer is None:
            self.writer = wave.open(self.file_path, 'wb')
            self.writer.setnchannels(pcm_frame.channels)
            self.writer.setsampwidth(2)
            self.writer.setframerate(pcm_frame.sample_rate)
            self.sample_rate = pcm_frame.sample_rate
            self.channels = pcm_frame.channels
            log_tag = f'[{self.log_prefix}|{self.session_id}]' if self.session_id and self.log_prefix else f'[{self.session_id}]' if self.session_id else ''
            logger.info(
                f'{log_tag} {Tick.process_tick()} {Fore.Cyan}save audio enabled{Fore.Reset}, '
                f'path={self.file_path}, sample_rate={self.sample_rate}, channels={self.channels}'
                if log_tag else
                f'{Tick.process_tick()} {Fore.Cyan}save audio enabled{Fore.Reset}, '
                f'path={self.file_path}, sample_rate={self.sample_rate}, channels={self.channels}'
            )
        elif self.sample_rate != pcm_frame.sample_rate or self.channels != pcm_frame.channels:
            raise ValueError(
                f'WAV output format changed unexpectedly: '
                f'{self.sample_rate}/{self.channels} -> {pcm_frame.sample_rate}/{pcm_frame.channels}'
            )
        self.writer.writeframes(pcm_frame.pcm_bytes)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None


async def aplay_with_queue(rtsp_url: str, forward_address: aio.IPAddress, stop_event: threading.Event,
                           que: queue.Queue, timeout:int, log_type: int,
                           session_id: Optional[str], log_prefix: str):
    try:
        async with open_session(rtsp_url, forward_address, timeout,
                                session_id=session_id, log_type=log_type,
                                log_prefix=log_prefix) as session:
            async for event in session.iter_events(stop_event):
                que.put(event)
    except Exception as ex:
        que.put(ex)
        stop_event.set()


def pyav_play(rtsp_url: str, forward_address: aio.IPAddress, play_time: int, timeout: int,
              log_type: int = aiortsp.DEFAULT_LOG_TYPE, audio_mode: str = AUDIO_MODE,
              audio_output_sample_rate: Optional[int] = AUDIO_OUTPUT_SAMPLE_RATE,
              save_audio_path: str = AUDIO_SAVE_AUDIO_PATH,
              session_id: Optional[str] = DEFAULT_SESSION_ID,
              log_prefix: str = DEFAULT_LOG_PREFIX):

    stop_event = threading.Event()
    que = queue.Queue()

    def network_thread(rtsp_url: str, forward_address: aio.IPAddress, stop_event: threading.Event,
                       que: queue.Queue, timeout: int, log_type: int,
                       session_id: Optional[str], log_prefix: str):
        asyncio.run(aplay_with_queue(
            rtsp_url, forward_address, stop_event, que, timeout, log_type, session_id, log_prefix
        ))

    th = threading.Thread(
        target=network_thread,
        args=(rtsp_url, forward_address, stop_event, que, timeout, log_type, session_id, log_prefix),
    )
    th.start()

    codec: av.codec.CodecContext = None #av.CodecContext.create("h264", "r")
    if audio_mode == 'play':
        from aio_rtsp_toolkit.audio_playback import SoundDeviceAudioPlayer
        audio_handler = SoundDeviceAudioPlayer(
            output_sample_rate=audio_output_sample_rate,
            session_id=session_id,
            log_prefix=log_prefix,
        )
    else:
        from aio_rtsp_toolkit.audio_decode import AudioPCMDecoder
        audio_handler = AudioPCMDecoder(
            output_sample_rate=audio_output_sample_rate,
            session_id=session_id,
            log_prefix=log_prefix,
        )
    first_decoded_tick = 0
    timestamps = []
    is_key_frame: dict[int, bool] = {}
    is_corrupt_frame: dict[int, bool] = {}
    video_time_base = fractions.Fraction(1, 90000)
    got_i_frame = False
    fout: io.TextIOWrapper = None
    codec_name_lower = ''
    input_video_frames: List[aiortsp.VideoFrame] = []
    output_video_frame_timestmaps = set()
    decode_fail_count = 0
    last_decoded_frame: av.VideoFrame = None
    audio_saver = None
    auido_rtp_count = 0
    video_rtp_count = 0
    log_every_count = 30
    try:
        if save_audio_path:
            audio_saver = WaveAudioSaver(save_audio_path, session_id=session_id, log_prefix=log_prefix)
        log_tag = f'[{log_prefix}|{session_id}]' if session_id and log_prefix else f'[{session_id}]' if session_id else ''
        logger.info(
            f'{log_tag} {Tick.process_tick()} {Fore.Cyan}audio mode={audio_mode}{Fore.Reset}, '
            f'output_sample_rate={audio_output_sample_rate or "source"}, '
            f'save_audio={save_audio_path or "disabled"}'
            if log_tag else
            f'{Tick.process_tick()} {Fore.Cyan}audio mode={audio_mode}{Fore.Reset}, '
            f'output_sample_rate={audio_output_sample_rate or "source"}, '
            f'save_audio={save_audio_path or "disabled"}'
        )
        while True:
            event = que.get()
            if isinstance(event, ConnectResultEvent):
                if event.exception is None:
                    logger.info(f'{Tick.process_tick()} {Fore.Green}RTSP Connected{Fore.Reset}'
                        f', local addr: {event.local_addr}, cost={event.elapsed}s')
                else:
                    logger.error(f'{Tick.process_tick()} {Fore.Red}RTSP Connect failed{Fore.Reset}'
                        f', local addr: {event.local_addr}, cost={event.elapsed}s, ex={event.exception.__class__.__module__}.{event.exception!r}')
                if event.local_addr is None:
                    break
            elif isinstance(event, RtspMethodEvent):
                logger.info(f'{Tick.process_tick()} {Fore.Green}RTSP {event.method}{Fore.Reset} session_elapsed={event.session_elapsed}')
                if event.method == 'DESCRIBE':
                    if event.status_code == 200:
                        video_sdp = event.response.sdp.get('video', None)
                        if video_sdp:
                            video_clock_rate = video_sdp['clock_rate']
                            video_time_base = fractions.Fraction(1, video_clock_rate)
                            codec_name_lower = video_sdp['codec_name'].lower()
                            av_codec_name = aiortsp.HEVCCodecName if codec_name_lower == aiortsp.H265CodecName else codec_name_lower
                            codec = av.CodecContext.create(av_codec_name, 'r')
                            fout = open(f'rtsp.{codec_name_lower}', 'wb')
                            sps = video_sdp.get('sps', None)
                            if sps:
                                fout.write(sps)
                                packets: List[av.Packet] = codec.parse(sps) # packets will be empty list
                            pps = video_sdp.get('pps', None)
                            if pps:
                                fout.write(pps)
                                packets: List[av.Packet] = codec.parse(pps) # packets will be empty list
                        audio_sdp = event.response.sdp.get('audio', None)
                        if audio_sdp:
                            audio_handler.configure(audio_sdp)
            elif isinstance(event, RtpPacketEvent):
                if event.media_channel == 'audio_rtp':
                    auido_rtp_count += 1
                    if auido_rtp_count % log_every_count == 1:
                        logger.info(f'{Tick.process_tick()} {Fore.Green}audio {event.rtp}{Fore.Reset}'
                                    f' session_elapsed={event.session_elapsed} log every {log_every_count} packets')
                elif event.media_channel == 'video_rtp':
                    video_rtp_count += 1
                    if video_rtp_count % log_every_count == 1:
                        logger.info(f'{Tick.process_tick()} {Fore.Green}video {event.rtp}{Fore.Reset}'
                                    f' session_elapsed={event.session_elapsed} log every {log_every_count} packets')
            elif isinstance(event, VideoFrameEvent):
                vframe = event.frame
                if fout:
                    fout.write(vframe.data)
                timestamps.append(vframe.timestamp)
                if len(timestamps) > 2:
                    timestamps.pop(0)
                if vframe.is_corrupt:
                    # is_corrupt is inaccurate for first I frame
                    # an I frame may have mutil VideoFrame objects: VPS, SPS, PPS, SEI, IDR frame
                    # if VPS, SPS, PPS, SEI are completely missing, we don't set is_corrupt for the last IDR frame
                    # but the last IDR frame will decode failed
                    is_corrupt_frame[vframe.timestamp] = True
                if not got_i_frame:
                    if codec_name_lower == aiortsp.H264CodecName:
                        if not (aiortsp.client.H264RTPNalUnitType.IDR <=vframe.nalu_type <= aiortsp.client.H264RTPNalUnitType.PPS):
                            logger.info(f'{Tick.process_tick()} skip raw {vframe} session_elapsed={event.session_elapsed}')
                            continue
                    elif codec_name_lower == aiortsp.H265CodecName:
                        if not (aiortsp.client.H265RTPNalUnitType.BLA_W_LP <=vframe.nalu_type <= aiortsp.client.H265RTPNalUnitType.SUFFIX_SEI_NUT):
                            logger.info(f'{Tick.process_tick()} skip raw {vframe} session_elapsed={event.session_elapsed}')
                            continue
                    if is_corrupt_frame.get(vframe.timestamp, False):
                        logger.warning(f'{Tick.process_tick()} {Fore.Yellow}skip corrupt raw {vframe}{Fore.Reset} session_elapsed={event.session_elapsed}')
                        continue
                logger.info(f'{Tick.process_tick()} {Fore.Green}raw {vframe}{Fore.Reset} session_elapsed={event.session_elapsed}')
                input_video_frames.append(vframe)
                if codec_name_lower == aiortsp.H264CodecName:
                    is_key_frame[vframe.timestamp] = aiortsp.client.H264RTPNalUnitType.IDR <=vframe.nalu_type <= aiortsp.client.H264RTPNalUnitType.PPS
                    got_i_frame = True
                elif codec_name_lower == aiortsp.H265CodecName:
                    is_key_frame[vframe.timestamp] = aiortsp.client.H265RTPNalUnitType.BLA_W_LP <=vframe.nalu_type <= aiortsp.client.H265RTPNalUnitType.SUFFIX_SEI_NUT
                    got_i_frame = True
                if codec:
                    packets: List[av.Packet] = codec.parse(vframe.data)
                    # an I frame may have multiple VideoFrame objects, they have the same timestamp but different nalu_types
                    # ffmpeg will merge them into one packet
                    for packet in packets:
                        packet.pts = packet.dts = timestamps[0]
                        packet.duration = timestamps[1] - timestamps[0]
                        packet.time_base = video_time_base
                        packet.is_keyframe = is_key_frame[packet.pts]
                        packet.is_corrupt = is_corrupt_frame.get(packet.pts, False)
                        logger.info(f'  {Tick.process_tick()} packet is_key={packet.is_keyframe}, pts={packet.pts}, dts={packet.dts}, duration={packet.duration}'
                              f', is_corrupt={packet.is_corrupt}, is_discard={packet.is_discard}, size={packet.size}')
                        try:
                            decoded_frames: List[av.VideoFrame] = codec.decode(packet)
                            for decoded_frame in decoded_frames:
                                if first_decoded_tick == 0:
                                    first_decoded_tick = Tick.process_tick()
                                    # save first decoded frame to image file
                                    # bgr_array = decoded_frame.to_ndarray(format='bgr24') # to opencv bgr24
                                    rgb_array = decoded_frame.to_ndarray(format='rgb24')
                                    image = PIL.Image.fromarray(rgb_array)
                                    image.save("first_frame.jpg", "JPEG")
                                last_decoded_frame = decoded_frame
                                output_video_frame_timestmaps.add(decoded_frame.pts)
                                color = Fore.Green if decoded_frame.key_frame else ''
                                logger.info(f'    {Tick.process_tick()} {color}video pict_type={decoded_frame.pict_type}, pts={decoded_frame.pts}'
                                      f', format={decoded_frame.format}, time={decoded_frame.time:.3f}{Fore.Reset}')
                                is_key_frame.pop(decoded_frame.pts, None)
                        except Exception as ex:
                            # if first I frame is not complete or no I frame before P frames
                            if output_video_frame_timestmaps: # if decode failed after an output frame, assume failed
                                decode_fail_count += 1
                            logger.error(f'{Tick.process_tick()} {Fore.Red}decode {packet.pts} error{Fore.Reset}, ex={ex!r}')
            elif isinstance(event, AudioFrameEvent):
                aframe = event.frame
                logger.info(f'{Tick.process_tick()} {Fore.Green}audio {aframe}{Fore.Reset} session_elapsed={event.session_elapsed}')
                pcm_frames = audio_handler.feed(aframe)
                for pcm_frame in pcm_frames:
                    if audio_saver:
                        audio_saver.write(pcm_frame)
                    if audio_mode == 'decode':
                         if first_decoded_tick == 0:
                            first_decoded_tick = Tick.process_tick()
                        # logger.info(f'  {Tick.process_tick()} pcm samples={pcm_frame.samples_per_channel}, '
                        #     f'sample_rate={pcm_frame.sample_rate}, channels={pcm_frame.channels}, '
                        #     f'bytes={len(pcm_frame.pcm_bytes)}')
            elif isinstance(event, ClosedEvent):
                logger.info(f'{Tick.process_tick()} {Fore.Green}RtspClientMsgType.Closed{Fore.Reset} session_elapsed={event.session_elapsed}')
                break
            elif isinstance(event, aiortsp.RtspTimeoutError):
                logger.error(f'{Tick.process_tick()} {Fore.Red}rtsp timeout{Fore.Reset}: {event!r} session_elapsed={event.session_elapsed}'
                    f'\n{"".join(traceback.format_exception(type(event), event, event.__traceback__))}')
                break
            elif isinstance(event, aiortsp.RtspConnectionError):
                logger.error(f'{Tick.process_tick()} {Fore.Red}rtsp connection error{Fore.Reset}: {event!r} session_elapsed={event.session_elapsed}'
                    f'\n{"".join(traceback.format_exception(type(event), event, event.__traceback__))}')
                break
            elif isinstance(event, Exception):
                logger.error(f'{Tick.process_tick()} {Fore.Red}other exception{Fore.Reset}: {event!r}'
                    f'\n{"".join(traceback.format_exception(type(event), event, event.__traceback__))}')
                break
            if first_decoded_tick > 0 and Tick.process_tick() - first_decoded_tick >= play_time:
                if not stop_event.is_set():
                    logger.info(f'{Tick.process_tick()} {Fore.Cyan}trigger stop{Fore.Reset}, first_decoded_tick={first_decoded_tick}')
                    stop_event.set()
                    # break # optional, will recv RtspClientMsgType.Closed if not break

        max_recv_cost = 0
        max_interval = 0
        frames2: List[aiortsp.VideoFrame] = []
        for vframe in input_video_frames:
            if len(frames2) == 0:
                frames2.append(vframe)
            elif len(frames2) == 1:
                if vframe.timestamp == frames2[0].timestamp:
                    vframe.first_seq = frames2[0].first_seq
                    frames2[0] = vframe
                else:
                    frames2.append(vframe)
            else:
                if vframe.timestamp == frames2[1].timestamp:
                    vframe.first_seq = frames2[1].first_seq
                    frames2[1] = vframe
                else:
                    interval = frames2[1].last_seq.tick - frames2[0].last_seq.tick
                    if interval > max_interval:
                        max_interval = round(interval, 6)
                    recv_cost = frames2[0].last_seq.tick - frames2[0].first_seq.tick
                    if recv_cost > max_recv_cost:
                        max_recv_cost = round(recv_cost, 6)
                    frames2[0] = frames2[1]
                    frames2[1] = vframe
        logger.info(f'{Tick.process_tick()} frames not decoded: {is_key_frame}')
        logger.info(f'{Tick.process_tick()} frames corrupted: {is_corrupt_frame}')
        logger.info(f'{Tick.process_tick()} frames max_recv_cost={max_recv_cost}, max_interval={max_interval}, decode_fail_count={decode_fail_count}')
        if last_decoded_frame is not None:
            img = last_decoded_frame.to_ndarray(format='bgr24')
            logger.info(f'{Tick.process_tick()} image shape={img.shape}')
    finally:
        if fout:
            fout.close()
        if audio_saver:
            audio_saver.close()
        audio_handler.close()
        stop_event.set()
        th.join(timeout=2.0)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help='rtsp url')
    parser.add_argument('-f', '--forward', default='', help='forward address')
    parser.add_argument('-t', '--time', type=int, default=5, help='play time')
    parser.add_argument('-to', '--timeout', type=int, default=5, help='timeout')
    parser.add_argument('--audio-mode', choices=('decode', 'play'), default=AUDIO_MODE,
                        help=f'audio handling mode, default: {AUDIO_MODE}')
    parser.add_argument('--audio-rate', type=int, default=AUDIO_OUTPUT_SAMPLE_RATE,
                        help='output pcm sample rate, default keeps source sample rate')
    parser.add_argument('--save-audio', default=AUDIO_SAVE_AUDIO_PATH,
                        help=f'save decoded audio to wav, default: {AUDIO_SAVE_AUDIO_PATH}; use empty string to disable')
    parser.add_argument('--session-id', default=DEFAULT_SESSION_ID,
                        help=f'session id used for RTSP/audio logs, default: {DEFAULT_SESSION_ID}')
    parser.add_argument('--log-prefix', default=DEFAULT_LOG_PREFIX,
                        help=f'log prefix used for RTSP/audio logs, default: {DEFAULT_LOG_PREFIX}')

    args = parser.parse_args()

    forward_addr = None # ('127.0.0.1', 8080)
    if args.forward:
        index = args.forward.rfind(':')
        forward_addr = (args.forward[:index], int(args.forward[index+1:]))

    config_logger(logger, 'info', log_dir='logs', log_file='cli_demo.log')
    pyav_play(
        args.url,
        forward_addr,
        args.time,
        args.timeout,
        aiortsp.DEFAULT_LOG_TYPE,
        audio_mode=args.audio_mode,
        audio_output_sample_rate=args.audio_rate,
        save_audio_path=args.save_audio,
        session_id=args.session_id,
        log_prefix=args.log_prefix,
    )
