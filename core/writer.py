from queue import Queue, Empty
from threading import Thread, Event, Lock
import logging


'''
Currently if stop event is set then run() exits and the data left in the shared buffer is discarded.
So, if we want to safely stop the entire program without discarding any data already in the shared buffer,
there will have to be some logic inside AcquisitionWorker that lets the writer threads flush and write all
the remaining data inside the shared buffers before terminating the threads.
'''


class Writer(Thread):
    '''
    Writes channel data to h5 file.
    '''
    def __init__(self, ch: int, flush_size: int, write_buffer: Queue, stop_event: Event):
        super().__init__(daemon=True)
        self.ch = ch
        self.flush_size = flush_size
        self.write_buffer = write_buffer
        self.stop_event = stop_event
        self.filename = f"some_name_ch_{self.ch}"
        self.local_buffer = []

    def write_h5(self):
        '''
        Write local buffer to h5 file and then clear local buffer.

        ***This needs to be implemented.***
        '''
        pass

    def run(self):
        '''
        Writer hot loop.
        '''
        logging.info(f"Writer thread started (ch {self.ch}).")
        try:
            while not self.stop_event.is_set():
                # First load data from shared buffer into local buffer
                for _ in range(self.flush_size):
                    try:
                        self.local_buffer.append(self.write_buffer.get_nowait())
                    except Empty:   # exit for loop if shared buffer is empty
                        break

                # If no data was added to local buffer, don't write to h5 file
                if len(self.local_buffer) == 0:
                    continue

                # Write all data in local buffer to h5 file
                self.write_h5()

        except Exception as e:
            logging.exception(f"Fatal error in Writer (ch {self.ch}): {e}")

        # When stop_event() is set, call cleanup()
        self.cleanup()
        logging.info(f"Writer thread exited cleanly (ch {self.ch}).")

    def cleanup(self):
        '''
        Handles cleanup of writer thread and h5 file.

        ***This needs to be implemented.***
        '''
        pass
