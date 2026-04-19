from __future__ import annotations

import asyncio
import contextlib
import struct
import time
from typing import List, Tuple

from .server_media import H264CodecName, H265CodecName, extract_nalus, normalize_codec_name
from .server_session_types import StreamTrackState, _is_disconnect_error


MAX_RTP_PAYLOAD = 1400


class RtspRtpMixin:
    async def _send_video_packet(
        self,
        state: StreamTrackState,
        data: bytes,
        media_time: float,
        duration_seconds: float,
        is_keyframe: bool,
    ) -> None:
        timestamp = int(round(media_time * state.info.clock_rate))
        codec_name = normalize_codec_name(state.info.codec_name)
        nalus = extract_nalus(codec_name, data, state.info.nal_length_size)
        if not nalus:
            return
        self._log_first_frame(state, timestamp, len(data))
        rtp_payloads: List[Tuple[bytes, int]] = []
        if state.info.parameter_sets and (is_keyframe or not state.sent_codec_config):
            for name in ("vps", "sps", "pps"):
                nalu = state.info.parameter_sets.get(name)
                if nalu:
                    rtp_payloads.extend(self._packetize_video_nalu(nalu, marker=0, codec_name=codec_name))
            state.sent_codec_config = True
        for index, nalu in enumerate(nalus):
            marker = 1 if index == len(nalus) - 1 else 0
            rtp_payloads.extend(self._packetize_video_nalu(nalu, marker=marker, codec_name=codec_name))
        await self._send_paced_rtp_payloads(state, rtp_payloads, timestamp, media_time, max(duration_seconds, 0.0))

    async def _send_aac_packet(self, state: StreamTrackState, data: bytes, media_time: float, duration_seconds: float) -> None:
        timestamp = int(round(media_time * state.info.clock_rate))
        self._log_first_frame(state, timestamp, len(data))
        au_header = (len(data) << 3).to_bytes(2, "big")
        payload = struct.pack("!H", 16) + au_header + data
        await self._send_paced_rtp_payloads(state, [(payload, 1)], timestamp, media_time, max(duration_seconds, 0.0))

    async def _send_raw_audio_packet(self, state: StreamTrackState, data: bytes, media_time: float, duration_seconds: float) -> None:
        timestamp = int(round(media_time * state.info.clock_rate))
        self._log_first_frame(state, timestamp, len(data))
        await self._send_paced_rtp_payloads(state, [(data, 1)], timestamp, media_time, max(duration_seconds, 0.0))

    async def _send_wav_audio_packet(self, state: StreamTrackState, data: bytes, sample_cursor: int) -> None:
        self._log_first_frame(state, sample_cursor, len(data))
        media_time = sample_cursor / float(state.info.clock_rate or 1)
        await self._sleep_until_media_time(state, media_time)
        await self._send_rtp_packet(state, data, timestamp=sample_cursor, marker=1)

    async def _send_live_audio_packet(self, state: StreamTrackState, data: bytes, sample_cursor: int) -> None:
        self._log_first_frame(state, sample_cursor, len(data))
        await self._send_rtp_packet(state, data, timestamp=sample_cursor, marker=1)

    def _log_first_frame(self, state: StreamTrackState, timestamp: int, size: int) -> None:
        if state.logged_first_frame:
            return
        state.logged_first_frame = True
        self.logger.info(
            f"{self.peername} first {state.info.media_type} frame "
            f"codec={state.info.codec_name} track={state.info.control} "
            f"timestamp={timestamp} size={size}"
        )

    async def _sleep_until_media_time(self, state: StreamTrackState, media_time: float) -> None:
        now = time.perf_counter()
        if state.first_send_time is None:
            state.first_send_time = now
        target = state.first_send_time + max(0.0, media_time)
        state.next_send_time = target if state.next_send_time is None else max(state.next_send_time, target)
        delay = state.next_send_time - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def _send_paced_rtp_payloads(
        self,
        state: StreamTrackState,
        payloads: List[Tuple[bytes, int]],
        timestamp: int,
        media_time: float,
        duration_seconds: float,
    ) -> None:
        if not payloads:
            return
        await self._sleep_until_media_time(state, media_time)
        count = len(payloads)
        gap = duration_seconds / count if count > 1 and duration_seconds > 0 else 0.0
        for index, (payload, marker) in enumerate(payloads):
            if index > 0 and gap > 0:
                next_target = (state.next_send_time or time.perf_counter()) + gap
                delay = next_target - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                state.next_send_time = next_target
            await self._send_rtp_packet(state, payload, timestamp=timestamp, marker=marker)

    def _packetize_video_nalu(self, nalu: bytes, marker: int, codec_name: str) -> List[Tuple[bytes, int]]:
        if len(nalu) <= MAX_RTP_PAYLOAD:
            return [(nalu, marker)]
        if codec_name == H264CodecName:
            return self._packetize_h264_fu_a(nalu, marker)
        if codec_name == H265CodecName:
            return self._packetize_h265_fu(nalu, marker)
        return []

    def _packetize_h264_fu_a(self, nalu: bytes, marker: int) -> List[Tuple[bytes, int]]:
        nal_header = nalu[0]
        fu_indicator = (nal_header & 0xE0) | 28
        nal_type = nal_header & 0x1F
        payload = memoryview(nalu)[1:]
        max_fragment = MAX_RTP_PAYLOAD - 2
        offset = 0
        packets: List[Tuple[bytes, int]] = []
        while offset < len(payload):
            size = min(max_fragment, len(payload) - offset)
            end = offset + size >= len(payload)
            fu_header = nal_type | (0x80 if offset == 0 else 0) | (0x40 if end else 0)
            chunk = bytes([fu_indicator, fu_header]) + payload[offset:offset + size].tobytes()
            packets.append((chunk, 1 if end and marker else 0))
            offset += size
        return packets

    def _packetize_h265_fu(self, nalu: bytes, marker: int) -> List[Tuple[bytes, int]]:
        if len(nalu) < 3:
            return []
        nal_header0 = nalu[0]
        nal_header1 = nalu[1]
        nal_type = (nal_header0 >> 1) & 0x3F
        fu_indicator0 = (nal_header0 & 0x81) | (49 << 1)
        payload = memoryview(nalu)[2:]
        max_fragment = MAX_RTP_PAYLOAD - 3
        offset = 0
        packets: List[Tuple[bytes, int]] = []
        while offset < len(payload):
            size = min(max_fragment, len(payload) - offset)
            end = offset + size >= len(payload)
            fu_header = nal_type | (0x80 if offset == 0 else 0) | (0x40 if end else 0)
            chunk = bytes([fu_indicator0, nal_header1, fu_header]) + payload[offset:offset + size].tobytes()
            packets.append((chunk, 1 if end and marker else 0))
            offset += size
        return packets

    async def _send_rtp_packet(self, state: StreamTrackState, payload: bytes, timestamp: int, marker: int) -> None:
        if state.rtp_channel is None:
            return
        header = struct.pack(
            "!BBHII",
            0x80,
            (0x80 if marker else 0x00) | (state.info.payload_type & 0x7F),
            state.sequence_number & 0xFFFF,
            timestamp & 0xFFFFFFFF,
            state.ssrc & 0xFFFFFFFF,
        )
        state.sequence_number = (state.sequence_number + 1) & 0xFFFF
        state.packet_count += 1
        state.octet_count += len(payload)
        state.last_rtp_timestamp = timestamp & 0xFFFFFFFF
        state.last_rtp_send_time = time.time()
        await self._send_interleaved_frame(state.rtp_channel, header + payload)
        await self._maybe_send_rtcp_sr(state)

    async def _send_interleaved_frame(self, channel: int, payload: bytes) -> None:
        interleaved = b"$" + bytes([channel]) + struct.pack("!H", len(payload)) + payload
        async with self.write_lock:
            try:
                await self.sock.send(interleaved)
            except Exception as ex:
                if _is_disconnect_error(ex):
                    raise ConnectionResetError(*getattr(ex, "args", ())) from ex
                raise

    async def _maybe_send_rtcp_sr(self, state: StreamTrackState) -> None:
        if state.rtcp_channel is None or state.packet_count <= 0 or state.last_rtp_send_time is None:
            return
        now = time.time()
        if state.last_rtcp_send_time is not None and now - state.last_rtcp_send_time < 5.0:
            return
        ntp_seconds = now + 2208988800
        ntp_msw = int(ntp_seconds) & 0xFFFFFFFF
        ntp_lsw = int((ntp_seconds - int(ntp_seconds)) * (1 << 32)) & 0xFFFFFFFF
        packet = struct.pack(
            "!BBHIIIIII",
            0x80,
            200,
            6,
            state.ssrc & 0xFFFFFFFF,
            ntp_msw,
            ntp_lsw,
            state.last_rtp_timestamp & 0xFFFFFFFF,
            state.packet_count & 0xFFFFFFFF,
            state.octet_count & 0xFFFFFFFF,
        )
        await self._send_interleaved_frame(state.rtcp_channel, packet)
        state.last_rtcp_send_time = now
        self.logger.debug(
            f"{self.peername} sent rtcp sr track={state.info.control} "
            f"rtcp_channel={state.rtcp_channel} packets={state.packet_count} octets={state.octet_count} "
            f"rtp_timestamp={state.last_rtp_timestamp}"
        )

    async def _send_rtcp_bye(self) -> None:
        for state in self.track_states.values():
            if state.rtcp_channel is None:
                continue
            packet = struct.pack("!BBHI", 0x81, 203, 1, state.ssrc & 0xFFFFFFFF)
            self.logger.info(
                f"{self.peername} sent rtcp bye track={state.info.control} "
                f"rtcp_channel={state.rtcp_channel} ssrc={state.ssrc:#x}"
            )
            with contextlib.suppress(Exception):
                await self._send_interleaved_frame(state.rtcp_channel, packet)

    def _handle_interleaved_frame(self, channel: int, payload: bytes) -> None:
        if not payload:
            return
        self._mark_activity()
        state = next((item for item in self.track_states.values() if item.rtcp_channel == channel), None)
        if state is not None:
            self._handle_rtcp_packet(state, payload)

    def _handle_rtcp_packet(self, state: StreamTrackState, payload: bytes) -> None:
        offset = 0
        while offset + 4 <= len(payload):
            v_p_count, packet_type, length_words = struct.unpack_from("!BBH", payload, offset)
            if ((v_p_count >> 6) & 0x03) != 2:
                return
            packet_size = (length_words + 1) * 4
            if offset + packet_size > len(payload):
                return
            body = payload[offset + 4:offset + packet_size]
            if packet_type == 201 and len(body) >= 20:
                sender_ssrc, reportee_ssrc = struct.unpack_from("!II", body, 0)
                fraction_lost = body[8]
                highest_seq = struct.unpack_from("!I", body, 12)[0]
                jitter = struct.unpack_from("!I", body, 16)[0]
                self.logger.info(
                    f"{self.peername} rtcp rr track={state.info.control} "
                    f"sender_ssrc={sender_ssrc:#x} reportee_ssrc={reportee_ssrc:#x} "
                    f"fraction_lost={fraction_lost} highest_seq={highest_seq} jitter={jitter}"
                )
            elif packet_type == 202 and len(body) >= 4:
                source_ssrc = struct.unpack_from("!I", body, 0)[0]
                self.logger.info(f"{self.peername} rtcp sdes track={state.info.control} ssrc={source_ssrc:#x}")
            elif packet_type == 203 and len(body) >= 4:
                source_ssrc = struct.unpack_from("!I", body, 0)[0]
                self.logger.info(f"{self.peername} rtcp bye track={state.info.control} ssrc={source_ssrc:#x}")
            offset += packet_size
