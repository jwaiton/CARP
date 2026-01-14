from queue import Queue, Empty
from threading import Thread, Event, Lock
import logging
import tables as tb

import core.df_classes as df_class
import core.io as io


class Writer(Thread):
    '''
    Writes channel data to h5 file.
    '''
    def __init__(self,
                 ch           : int,
                 flush_size   : int,
                 write_buffer : Queue,
                 stop_event   : Event,
                 rec_config   : dict,
                 dig_config   : dict,
                 TIMESTAMP    : str,):
        '''
        TIMESTAMP should be provided to all channels identically before the
        writer threads are initialised.
        '''

        super().__init__(daemon=True)
        self.ch = ch
        self.flush_size = flush_size
        self.write_buffer = write_buffer
        self.stop_event = stop_event
        self.rec_config = rec_config
        self.dig_config = dig_config
        self.filename = f"some_name_ch_{self.ch}"
        self.local_buffer = []
        self.wf_size   = None

        if 'file_name' in self.rec_config:
            file_path = self.rec_config['file_name']
            file_path = f'{file_path}_{self.ch}_{TIMESTAMP}.h5'
        else:
            file_path = f'{self.ch}_{TIMESTAMP}.h5'
        # initialise the h5, one per channel, each handled on a separate thread
        try:
            self.h5file = tb.open_file(f'{file_path}', mode='a')
        except FileNotFoundError as e:
            logging.error(f'FileNotFoundError: Cannot create output file at path {file_path}')
            exit()
        # configs written
        io.create_config_table(self.h5file, self.rec_config, 'rec_conf', 'recording config')
        io.create_config_table(self.h5file, self.dig_config, 'dig_conf', 'digitiser config')
        # raw waveform group constructed
        self.rwf_group = self.h5file.create_group('/', f'ch_{ch}', 'raw waveform')


    def write_h5(self):
        '''
        Write local buffer to h5 file and then clear local buffer.

        assumption is that the local buffer contains tuples of:
        (waveform_size, ADCs, event_no)
        where ADCs is the actual raw waveform array
        '''

        for wf_size, rwf, evt in self.local_buffer:
        # if we know the size of the waveforms already, don't create the class again.
            if self.wf_size is None:
                self.wf_size = wf_size
                self.rwf_class = df_class.return_rwf_class(self.dig_config['dig_gen'], self.wf_size)
                self.rwf_table = self.h5file.create_table(self.rwf_group, 'rwf', self.rwf_class, "raw waveforms")
                self.rows      =  self.rwf_table.row

            self.rows['evt_no'] = evt
            self.rows['rwf']    = rwf
            self.rows.append()

        self.local_buffer.clear()

        # flush as fast as the buffer provides
        self.rwf_table.flush()
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
        '''
        # close the h5 file
        self.h5file.close()
        pass
