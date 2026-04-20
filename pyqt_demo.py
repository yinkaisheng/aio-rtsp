import asyncio
import fractions
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import List, Optional

import av
from PyQt5 import QtCore, QtGui, QtWidgets

import aio_sockets as aio
import aio_rtsp_toolkit as aiortsp
from aio_rtsp_toolkit.tick import Tick
import aio_rtsp_toolkit.audio_decode as audio_decode
import aio_rtsp_toolkit.audio_playback as audio_playback

from log_util import config_logger, logger


aio.aio_sockets.logger = logger
aiortsp.types.logger = logger

DEFAULT_SESSION_ID = 'qt01'
DEFAULT_LOG_PREFIX = 'pyqt'


class VideoLabel(QtWidgets.QLabel):
    def __init__(self, parent: Optional[QtWidgets.QWidget] = None) -> None:
        super().__init__(parent)
        self._pixmap = QtGui.QPixmap()
        self.setAlignment(QtCore.Qt.AlignCenter)
        self.setMinimumHeight(320)
        self.setStyleSheet('background-color: #111; color: #ddd; border: 1px solid #444;')
        self.setText('No Video')

    def set_frame(self, image: QtGui.QImage) -> None:
        self._pixmap = QtGui.QPixmap.fromImage(image)
        self._refresh()

    def clear_frame(self) -> None:
        self._pixmap = QtGui.QPixmap()
        self.setPixmap(QtGui.QPixmap())
        self.setText('No Video')

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._refresh()

    def _refresh(self) -> None:
        if self._pixmap.isNull():
            return
        scaled = self._pixmap.scaled(self.size(), QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
        self.setText('')
        self.setPixmap(scaled)


class RtspPlayerThread(QtCore.QThread):
    image_ready = QtCore.pyqtSignal(object)
    clear_video = QtCore.pyqtSignal()
    log_ready = QtCore.pyqtSignal(str)
    playing_changed = QtCore.pyqtSignal(bool)
    first_media_frame = QtCore.pyqtSignal()

    def __init__(
        self,
        rtsp_url: str,
        timeout: int,
        enable_video: bool = True,
        enable_audio: bool = True,
        session_id: str = DEFAULT_SESSION_ID,
        log_prefix: str = DEFAULT_LOG_PREFIX,
        parent: Optional[QtCore.QObject] = None,
    ) -> None:
        super().__init__(parent)
        self.rtsp_url = rtsp_url
        self.timeout = timeout
        self.enable_video = enable_video
        self.enable_audio = enable_audio
        self.stop_event = threading.Event()
        self.session_id = session_id
        self.log_prefix = log_prefix

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        self.playing_changed.emit(True)
        try:
            self._play()
        except Exception as ex:
            self.log_ready.emit(f'{Tick.process_tick():.3f} playback thread error: {ex!r}\n{traceback.format_exc()}')
        finally:
            self.playing_changed.emit(False)

    def _play(self) -> None:
        msg_queue = queue.Queue()  # type: queue.Queue[object]

        def network_thread() -> None:
            try:
                asyncio.run(self._play_with_queue(msg_queue))
            except Exception as ex:
                msg_queue.put(ex)
                self.stop_event.set()

        th = threading.Thread(target=network_thread, daemon=True)
        th.start()

        codec = None
        audio_player = audio_playback.SoundDeviceAudioPlayer(
            session_id=self.session_id,
            log_prefix=self.log_prefix,
        )
        timestamps = []  # type: List[int]
        is_key_frame = {}  # type: dict[int, bool]
        is_corrupt_frame = {}  # type: dict[int, bool]
        video_time_base = fractions.Fraction(1, 90000)
        codec_name_lower = ''
        got_i_frame = False
        logged_first_video_frame = False
        logged_first_audio_frame = False
        started_media_timer = False

        try:
            while True:
                event = msg_queue.get()

                if isinstance(event, aiortsp.ConnectResultEvent):
                    if event.exception is None:
                        self.log_ready.emit(
                            f'{Tick.process_tick():.3f} RTSP connected, local addr={event.local_addr}, cost={event.elapsed}s'
                            f', session_elapsed={event.session_elapsed}s'
                        )
                    else:
                        self.log_ready.emit(
                            f'{Tick.process_tick():.3f} RTSP connect failed, local addr={event.local_addr}, cost={event.elapsed}s'
                            f', session_elapsed={event.session_elapsed}s'
                            f', ex={event.exception!r}'
                        )
                    if event.local_addr is None:
                        break

                elif isinstance(event, aiortsp.RtspMethodEvent):
                    self._log_rtsp(event)
                    if event.method == 'DESCRIBE' and event.status_code == 200:
                        video_sdp = (event.response.sdp or {}).get('video', {})
                        video_clock_rate = video_sdp.get('clock_rate', 90000)
                        video_time_base = fractions.Fraction(1, video_clock_rate)
                        codec_name_lower = str(video_sdp.get('codec_name', '')).lower()
                        if video_sdp:
                            self.log_ready.emit(
                                f'{Tick.process_tick():.3f} video stream: codec={video_sdp.get("codec_name", "unknown")}, clock_rate={video_clock_rate}'
                            )
                        if codec_name_lower:
                            av_codec_name = aiortsp.HEVCCodecName if codec_name_lower == aiortsp.H265CodecName else codec_name_lower
                            codec = av.CodecContext.create(av_codec_name, 'r')
                            for key in ('sps', 'pps', 'vps'):
                                extra = video_sdp.get(key)
                                if extra:
                                    codec.parse(extra)
                        audio_sdp = (event.response.sdp or {}).get('audio', {})
                        if audio_sdp:
                            self.log_ready.emit(
                                f'{Tick.process_tick():.3f} audio stream: codec={audio_sdp.get("codec_name", "unknown")}, '
                                f'sample_rate={audio_sdp.get("clock_rate", 0)}, channels={audio_sdp.get("channel", 1)}'
                            )
                            audio_player.configure(audio_sdp)

                elif isinstance(event, aiortsp.VideoFrameEvent):
                    vframe = event.frame
                    if vframe is None:
                        continue
                    timestamps.append(vframe.timestamp)
                    if len(timestamps) > 2:
                        timestamps.pop(0)

                    if vframe.is_corrupt:
                        is_corrupt_frame[vframe.timestamp] = True

                    if not got_i_frame:
                        if codec_name_lower in (aiortsp.H264CodecName, aiortsp.H265CodecName) and not vframe.is_key_frame:
                            continue
                        if is_corrupt_frame.get(vframe.timestamp, False):
                            self.log_ready.emit(f'{Tick.process_tick():.3f} skip corrupt raw frame ts={vframe.timestamp}')
                            continue

                    if codec_name_lower in (aiortsp.H264CodecName, aiortsp.H265CodecName):
                        is_key_frame[vframe.timestamp] = bool(vframe.is_key_frame)
                        got_i_frame = True

                    if codec is None:
                        continue

                    packets = codec.parse(vframe.data)
                    for packet in packets:
                        packet.pts = packet.dts = timestamps[0]
                        packet.duration = timestamps[1] - timestamps[0] if len(timestamps) > 1 else 0
                        packet.time_base = video_time_base
                        packet.is_keyframe = is_key_frame.get(packet.pts, False)
                        packet.is_corrupt = is_corrupt_frame.get(packet.pts, False)
                        try:
                            decoded_frames = codec.decode(packet)
                        except Exception as ex:
                            self.log_ready.emit(f'{Tick.process_tick():.3f} decode error pts={packet.pts}, ex={ex!r}')
                            continue

                        for decoded_frame in decoded_frames:
                            is_key_frame.pop(int(decoded_frame.pts or 0), None)
                            rgb = decoded_frame.to_ndarray(format='rgb24')
                            height, width, _ = rgb.shape
                            if not logged_first_video_frame:
                                if not started_media_timer:
                                    self.first_media_frame.emit()
                                    started_media_timer = True
                                self.log_ready.emit(
                                    f'{Tick.process_tick():.3f} first video frame: width={width}, height={height}'
                                    f', session_elapsed={event.session_elapsed}s'
                                )
                                logged_first_video_frame = True
                            bytes_per_line = width * 3
                            image = QtGui.QImage(
                                rgb.data, width, height, bytes_per_line, QtGui.QImage.Format_RGB888
                            ).copy()
                            self.image_ready.emit(image)

                elif isinstance(event, aiortsp.AudioFrameEvent):
                    aframe = event.frame
                    if aframe is None:
                        continue
                    if not logged_first_audio_frame:
                        if not started_media_timer:
                            self.first_media_frame.emit()
                            started_media_timer = True
                        duration_ms = 0.0
                        if aframe.sample_rate:
                            duration_ms = aframe.sample_count * 1000.0 / aframe.sample_rate
                        self.log_ready.emit(
                            f'{Tick.process_tick():.3f} first audio frame: sample_rate={aframe.sample_rate}'
                            f', duration_ms={duration_ms:.2f}, session_elapsed={event.session_elapsed}s'
                        )
                        logged_first_audio_frame = True
                    audio_player.feed(aframe)

                elif isinstance(event, aiortsp.ClosedEvent):
                    message = (
                        f'{Tick.process_tick():.3f} rtsp closed, session_elapsed={event.session_elapsed}s, '
                        f'reason={event.reason}, exception={event.exception!r}'
                    )
                    if event.exception is not None:
                        message += (
                            '\n'
                            f'{"".join(traceback.format_exception(type(event.exception), event.exception, event.exception.__traceback__))}'
                        )
                    self.log_ready.emit(message)
                    self.clear_video.emit()
                    break

                elif isinstance(event, aiortsp.RtspTimeoutError):
                    self.log_ready.emit(
                        f'{Tick.process_tick():.3f} rtsp timeout: {event!r}\n'
                        f'{"".join(traceback.format_exception(type(event), event, event.__traceback__))}'
                        f'the above exception was handled'
                    )
                    self.clear_video.emit()
                    break

                elif isinstance(event, Exception):
                    self.log_ready.emit(
                        f'{Tick.process_tick():.3f} other exception: {event!r}\n'
                        f'{"".join(traceback.format_exception(type(event), event, event.__traceback__))}'
                        f'the above exception was handled'
                    )
                    self.clear_video.emit()
                    break
        finally:
            self.stop_event.set()
            audio_player.close()
            th.join(timeout=2.0)

    async def _play_with_queue(self, msg_queue: queue.Queue) -> None:
        async with aiortsp.open_session(
            self.rtsp_url,
            timeout=self.timeout,
            enable_video=self.enable_video,
            enable_audio=self.enable_audio,
            log_type=aiortsp.RtspClientMsgType.Exception
            | aiortsp.RtspClientMsgType.ConnectResult
            | aiortsp.RtspClientMsgType.Closed
            | aiortsp.RtspClientMsgType.RTSP,
            session_id=self.session_id,
            log_prefix=self.log_prefix,
        ) as session:
            async for event in session.iter_events(self.stop_event):
                if isinstance(event, aiortsp.RtpPacketEvent):
                    continue
                msg_queue.put(event)

    def _log_rtsp(self, event: aiortsp.RtspMethodEvent) -> None:
        self.log_ready.emit(
            f'{Tick.process_tick():.3f} RTSP {event.method} status={event.status_code}'
            f', cost={event.elapsed}s, session_elapsed={event.session_elapsed}s'
        )


class MainWindow(QtWidgets.QWidget):
    SESSION_DIR = Path.home() / '.aio-rtsp-toolkit'
    SESSION_FILE = SESSION_DIR / 'rtsp_session.txt'
    MAX_HISTORY_URLS = 20
    RTSP_TIMEOUT = 5

    def __init__(self) -> None:
        super().__init__()
        self.player_thread = None  # type: Optional[RtspPlayerThread]
        self.play_sequence = 0
        self._build_ui()
        self._load_last_rtsp_url()

    def _build_ui(self) -> None:
        self.setWindowTitle('RTSP PyQt Demo')
        self.resize(1280, 900)

        self.video_label = VideoLabel()

        self.url_edit = QtWidgets.QComboBox()
        self.url_edit.setEditable(True)
        self.url_edit.setInsertPolicy(QtWidgets.QComboBox.NoInsert)
        self.url_edit.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self.url_edit.setMinimumContentsLength(30)
        self.url_edit.setCurrentText('rtsp://')
        self.url_edit.lineEdit().setPlaceholderText('RTSP address')

        self.timeout_spin = QtWidgets.QSpinBox()
        self.timeout_spin.setRange(1, 3600)
        self.timeout_spin.setValue(self.RTSP_TIMEOUT)
        self.timeout_spin.setSuffix(' s')

        self.video_check = QtWidgets.QCheckBox('Video')
        self.video_check.setChecked(True)
        self.audio_check = QtWidgets.QCheckBox('Audio')
        self.audio_check.setChecked(True)
        self.duration_label = QtWidgets.QLabel('')

        self.play_button = QtWidgets.QPushButton('Play')
        self.clear_log_button = QtWidgets.QPushButton('Clear Log')
        self.stop_button = QtWidgets.QPushButton('Stop')
        self.stop_button.setEnabled(False)

        self.log_edit = QtWidgets.QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        self.log_edit.setPlaceholderText('Key events will appear here')
        self.log_edit.setMaximumBlockCount(1000)

        form_layout = QtWidgets.QHBoxLayout()
        form_layout.addWidget(QtWidgets.QLabel('RTSP URL'))
        form_layout.addWidget(self.url_edit, 1)
        form_layout.addWidget(QtWidgets.QLabel('Timeout'))
        form_layout.addWidget(self.timeout_spin)
        form_layout.addWidget(self.video_check)
        form_layout.addWidget(self.audio_check)
        form_layout.addWidget(self.duration_label)
        form_layout.addWidget(self.play_button)
        form_layout.addWidget(self.stop_button)
        form_layout.addWidget(self.clear_log_button)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(self.video_label, 3)
        layout.addLayout(form_layout)
        layout.addWidget(self.log_edit, 2)

        self.play_elapsed_timer = QtCore.QElapsedTimer()
        self.play_duration_timer = QtCore.QTimer(self)
        self.play_duration_timer.setInterval(200)
        self.play_duration_timer.timeout.connect(self._update_play_duration)

        self.play_button.clicked.connect(self.start_play)
        self.clear_log_button.clicked.connect(self.log_edit.clear)
        self.stop_button.clicked.connect(lambda: self.stop_play())

    def start_play(self) -> None:
        rtsp_url = self.url_edit.currentText().strip()
        if not rtsp_url:
            self.append_log('Please enter an RTSP URL')
            return
        if not self.video_check.isChecked() and not self.audio_check.isChecked():
            self.append_log('Select at least one media type')
            return

        self._save_last_rtsp_url(rtsp_url)

        self.stop_play(wait=True, clear_log=False)
        self.video_label.clear_frame()
        self._reset_play_duration()
        media_mode = []
        if self.video_check.isChecked():
            media_mode.append('video')
        if self.audio_check.isChecked():
            media_mode.append('audio')
        self.append_log(
            f'start play: url={rtsp_url}, timeout={self.timeout_spin.value()}s, media={"+".join(media_mode)}'
        )
        self.play_sequence += 1
        session_id = f'qt{self.play_sequence:02d}'

        self.player_thread = RtspPlayerThread(
            rtsp_url,
            self.timeout_spin.value(),
            enable_video=self.video_check.isChecked(),
            enable_audio=self.audio_check.isChecked(),
            session_id=session_id,
            log_prefix=DEFAULT_LOG_PREFIX,
            parent=self,
        )
        self.player_thread.image_ready.connect(self.video_label.set_frame)
        self.player_thread.clear_video.connect(self.video_label.clear_frame)
        self.player_thread.log_ready.connect(self.append_log)
        self.player_thread.playing_changed.connect(self.on_playing_changed)
        self.player_thread.first_media_frame.connect(self.on_first_media_frame)
        self.player_thread.start()

    def stop_play(self, wait: bool = False, clear_log: bool = False) -> None:
        if clear_log:
            self.log_edit.clear()

        if self.player_thread is None:
            return

        self.append_log('stop requested')
        self.player_thread.stop()
        if wait:
            self.player_thread.wait(2000)
        if not self.player_thread.isRunning():
            self.player_thread.deleteLater()
            self.player_thread = None

    def on_playing_changed(self, playing: bool) -> None:
        self.play_button.setEnabled(not playing)
        self.stop_button.setEnabled(playing)
        if not playing:
            self.play_duration_timer.stop()
        if not playing and self.player_thread is not None and not self.player_thread.isRunning():
            self.player_thread.deleteLater()
            self.player_thread = None

    def append_log(self, message: str) -> None:
        self.log_edit.appendPlainText(message)
        scrollbar = self.log_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def on_first_media_frame(self) -> None:
        if self.play_elapsed_timer.isValid():
            return
        self.play_elapsed_timer.start()
        self._update_play_duration()
        self.play_duration_timer.start()

    def _reset_play_duration(self) -> None:
        self.play_duration_timer.stop()
        self.play_elapsed_timer.invalidate()
        self.duration_label.setText('')

    def _update_play_duration(self) -> None:
        if not self.play_elapsed_timer.isValid():
            self.duration_label.setText('')
            return
        total_ms = self.play_elapsed_timer.elapsed()
        total_seconds = total_ms // 1000
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        self.duration_label.setText(f'{hours:02}:{minutes:02}:{seconds:02}')

    def _load_last_rtsp_url(self) -> None:
        try:
            if self.SESSION_FILE.exists():
                lines = [
                    line.strip()
                    for line in self.SESSION_FILE.read_text(encoding='utf-8').splitlines()
                    if line.strip()
                ]
                if lines:
                    self.url_edit.clear()
                    self.url_edit.addItems(lines[:self.MAX_HISTORY_URLS])
                    self.url_edit.setCurrentIndex(0)
        except OSError:
            pass

    def _save_last_rtsp_url(self, rtsp_url: str) -> None:
        try:
            self.SESSION_DIR.mkdir(parents=True, exist_ok=True)
            history = [rtsp_url]
            for index in range(self.url_edit.count()):
                item = self.url_edit.itemText(index).strip()
                if item and item != rtsp_url:
                    history.append(item)
            history = history[:self.MAX_HISTORY_URLS]
            self.url_edit.clear()
            self.url_edit.addItems(history)
            self.url_edit.setCurrentIndex(0)
            self.SESSION_FILE.write_text('\n'.join(history) + '\n', encoding='utf-8')
        except OSError as ex:
            self.append_log(f'save session failed: {ex!r}')

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        self.stop_play(wait=True)
        super().closeEvent(event)


def main() -> int:
    config_logger(logger, 'info')
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec_()


if __name__ == '__main__':
    sys.exit(main())
