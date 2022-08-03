import time


class Timer:
    """Timer class. Counts time elapsed in seconds.

    Example usage:
        timer = Timer("step 1")
        with timer:
            # call a function here
        # then, timer.elapsed will hold how long the function took (in seconds)
    """

    def __init__(self, description, allow_print=True, logger=None):
        self.description = description
        self.allow_print = allow_print
        self.t0 = None
        self.elapsed = None
        self.logger = logger

    def __enter__(self):
        self.t0 = time.perf_counter()

    def __exit__(self, type=None, value=None, traceback=None):
        self.elapsed = time.perf_counter() - self.t0
        msg = f"{self.description} {self.elapsed / 60} minutes and {self.elapsed % 60}  seconds elapsed."

        if self.logger is not None:
            self.logger.info(msg)

        if self.allow_print:
            print(msg)
