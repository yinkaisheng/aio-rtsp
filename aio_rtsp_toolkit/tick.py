import time


class Tick:
    """Monotonic timing helper used for local process-relative measurements.

    All values produced by this class are process-local elapsed times derived
    from ``time.perf_counter()``. They are not wall-clock timestamps and should
    not be compared across different processes or machines.
    """

    get_tick = time.perf_counter
    _offset_tick = get_tick()

    @staticmethod
    def process_tick() -> float:
        """Return seconds elapsed since this Python process initialized Tick."""

        return round(Tick.get_tick() - Tick._offset_tick, 6)

    def __init__(self):
        self.reset()

    def reset(self):
        self.start_tick = self.get_tick()
        self.last_tick = None

    def update(self) -> None:
        self.last_tick = self.get_tick()

    def since_start(self) -> float:
        self.last_tick = self.get_tick()
        return round(self.last_tick - self.start_tick, 6)

    def since_last(self) -> float:
        now = self.get_tick()
        cost = round(now - (self.last_tick or self.start_tick), 6)
        self.last_tick = now
        return cost

    def check_interval(self, interval: float) -> bool:
        if self.last_tick is None:
            self.last_tick = self.get_tick()
            return True
        now = self.get_tick()
        if now - self.last_tick >= interval:
            self.last_tick = now
            return True
        return False

    def reset_interval(self):
        self.last_tick = None

    def __str__(self):
        return f'elapsed(last={self.since_last()},total={self.last_tick - self.start_tick})'

    def __repr__(self):
        return f'<{self.__class__.__name__} at 0x{id(self):08} {self}>'
