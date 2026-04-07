import os, sys
import asyncio
import queue
import traceback
import threading
from typing import Tuple, List, AsyncGenerator, Any

import aio_sockets as aio
import aio_rtsp_toolkit as aiortsp
from aio_rtsp_toolkit import (AudioFrameEvent, ClosedEvent, ConnectResultEvent, RtpPacketEvent, RtspMethodEvent,
                              Tick, VideoFrameEvent, open_session)
from aio_rtsp_toolkit.audio_playback import SoundDeviceAudioPlayer
from log_util import Fore, log


aio.aio_sockets.logfunc = log
aiortsp.client.logfunc = log


async def aplay_with_queue(rtsp_url: str, forward_address: aio.IPAddress, stop_event: threading.Event,
                           que: queue.Queue, timeout:int, log_type: int):
    try:
        async with open_session(rtsp_url, forward_address, timeout, log_type) as session:
            async for event in session.iter_events(stop_event):
                que.put(event)
    except Exception as ex:
        que.put(ex)
        stop_event.set()


def pyav_play(rtsp_url: str, forward_address: aio.IPAddress, play_time: int, timeout: int,
              log_type: int = aiortsp.RtspClientMsgType.RTSP):
    import io
    import fractions
    import av
    import PIL.Image

    stop_event = threading.Event()
    que = queue.Queue()

    def network_thread(rtsp_url: str, forward_address: aio.IPAddress, stop_event: threading.Event,
                       que: queue.Queue, timeout: int, log_type: int):
        asyncio.run(aplay_with_queue(rtsp_url, forward_address, stop_event, que, timeout, log_type))

    th = threading.Thread(target=network_thread, args=(rtsp_url, forward_address, stop_event, que, timeout, log_type))
    th.start()

    codec: av.codec.CodecContext = None #av.CodecContext.create("h264", "r")
    audio_player = SoundDeviceAudioPlayer(log)
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
    try:
        while True:
            event = que.get()
            if isinstance(event, ConnectResultEvent):
                if event.exception is None:
                    log(f'{Tick.process_tick()} {Fore.Green}RTSP Connected{Fore.Reset}'
                        f', local addr: {event.local_addr}, cost={event.elapsed}s')
                else:
                    log(f'{Tick.process_tick()} {Fore.Red}RTSP Connect failed{Fore.Reset}'
                        f', local addr: {event.local_addr}, cost={event.elapsed}s, ex={event.exception.__class__.__module__}.{event.exception!r}')
                if event.local_addr is None:
                    break
            elif isinstance(event, RtspMethodEvent):
                log(f'{Tick.process_tick()} {Fore.Green}RTSP {event.method}{Fore.Reset}')
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
                            audio_player.configure(audio_sdp)
            elif isinstance(event, RtpPacketEvent):
                log(f'{Tick.process_tick()} {Fore.Green}{event.media_channel} {event.rtp}{Fore.Reset}')
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
                        if not (5 <=vframe.nalu_type <= 8):
                            log(f'{Tick.process_tick()} skip raw {vframe}')
                            continue
                    elif codec_name_lower == aiortsp.H265CodecName:
                        if not (16 <=vframe.nalu_type <= 40):
                            log(f'{Tick.process_tick()} skip raw {vframe}')
                            continue
                    if is_corrupt_frame.get(vframe.timestamp, False):
                        log(f'{Tick.process_tick()} {Fore.Yellow}skip corrupt raw {vframe}{Fore.Reset}')
                        continue
                log(f'{Tick.process_tick()} {Fore.Green}raw {vframe}{Fore.Reset}')
                input_video_frames.append(vframe)
                if codec_name_lower == aiortsp.H264CodecName:
                    is_key_frame[vframe.timestamp] = 5 <= vframe.nalu_type <= 8
                    got_i_frame = True
                elif codec_name_lower == aiortsp.H265CodecName:
                    is_key_frame[vframe.timestamp] = 16 <= vframe.nalu_type <= 40
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
                        log(f'  {Tick.process_tick()} packet is_key={packet.is_keyframe}, pts={packet.pts}, dts={packet.dts}, duration={packet.duration}'
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
                                log(f'    {Tick.process_tick()} {color}video pict_type={decoded_frame.pict_type}, pts={decoded_frame.pts}'
                                      f', format={decoded_frame.format}, time={decoded_frame.time:.3f}{Fore.Reset}')
                                is_key_frame.pop(decoded_frame.pts, None)
                        except Exception as ex:
                            # if first I frame is not complete or no I frame before P frames
                            if output_video_frame_timestmaps: # if decode failed after an output frame, assume failed
                                decode_fail_count += 1
                            log(f'{Tick.process_tick()} {Fore.Red}decode {packet.pts} error{Fore.Reset}, Exception={ex!r}')
            elif isinstance(event, AudioFrameEvent):
                aframe = event.frame
                log(f'{Tick.process_tick()} {Fore.Green}audio {aframe}{Fore.Reset}')
                audio_player.feed(aframe)
            elif isinstance(event, ClosedEvent):
                log(f'{Tick.process_tick()} {Fore.Green}RtspClientMsgType.Closed{Fore.Reset}')
                break
            elif isinstance(event, aiortsp.RtspTimeoutError):
                log(f'{Tick.process_tick()} {Fore.Red}rtsp timeout{Fore.Reset}: {event!r}'
                    f'\n{"".join(traceback.format_exception(event))}')
                break
            elif isinstance(event, aiortsp.RtspConnectionError):
                log(f'{Tick.process_tick()} {Fore.Red}rtsp connection error{Fore.Reset}: {event!r}'
                    f'\n{"".join(traceback.format_exception(event))}')
                break
            elif isinstance(event, Exception):
                log(f'{Tick.process_tick()} {Fore.Red}other exception{Fore.Reset}: {event!r}'
                    f'\n{"".join(traceback.format_exception(event))}')
                break
            if first_decoded_tick > 0 and Tick.process_tick() - first_decoded_tick >= play_time:
                if not stop_event.is_set():
                    log(f'{Tick.process_tick()} {Fore.Cyan}trigger stop{Fore.Reset}, first_decoded_tick={first_decoded_tick}')
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
        log(f'{Tick.process_tick()} frames not decoded: {is_key_frame}')
        log(f'{Tick.process_tick()} frames corrupted: {is_corrupt_frame}')
        log(f'{Tick.process_tick()} frames max_recv_cost={max_recv_cost}, max_interval={max_interval}, decode_fail_count={decode_fail_count}')
        if last_decoded_frame is not None:
            img = last_decoded_frame.to_ndarray(format='bgr24')
            log(f'{Tick.process_tick()} image shape={img.shape}')
    finally:
        if fout:
            fout.close()
        audio_player.close()
        stop_event.set()
        th.join(timeout=2.0)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', help='rtsp url')
    parser.add_argument('-f', '--forward', default='', help='forward address')
    parser.add_argument('-t', '--time', type=int, default=5, help='play time')
    parser.add_argument('-to', '--timeout', type=int, default=5, help='timeout')

    args = parser.parse_args()
    if args.url is None:
        log(f'{os.path.basename(__file__)} -u rtsp://...')
        sys.exit(1)

    forward_addr = None # ('127.0.0.1', 8080)
    if args.forward:
        index = args.forward.rfind(':')
        forward_addr = (args.forward[:index], int(args.forward[index+1:]))

    log_type = aiortsp.RtspClientMsgType.ConnectResult | aiortsp.RtspClientMsgType.RTSP
    pyav_play(args.url, forward_addr, args.time, args.timeout, log_type)
