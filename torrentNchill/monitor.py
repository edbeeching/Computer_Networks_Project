'''
    Monitor Class for progress bar
    Created by Edward Beeching 05/03/2017
    # Provide a progress bar that monitors the progress of the file transfer.
    # Displays information such as:
        # % completion
        # Download Rate
        # Upload Rate

    # Needs the following information:
        # Part size
        # Number of parts
        # 

'''
from threading import Thread
import progressbar
import queue
import time
import kbhit
import logging


class Monitor(Thread):
    def __init__(self, have_parts, total_parts, in_queue, out_queue):
        # Initialise the thread
        Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.parts_recieved = have_parts
        self.bar = progressbar.ProgressBar(max_value=total_parts, redirect_stdout=True)




    def run(self):
        kb = kbhit.KBHit()
        print('Starting monitor, press q to exit')
        self.bar.update(self.parts_recieved)
        while True:
            if not self.in_queue.empty():
                message = self.in_queue.get()

                if message['msg'] == 'PARTS':
                    self.parts_recieved += 1
                    self.bar.update(message['parts'])
                if message['msg'] == 'END':
                    break
            else:
                if kb.kbhit():
                    c = kb.getch()
                    if c is 'q':  # Press q to exit
                        print('q pressed')
                        self.out_queue.put({'msg': 'CLOSE'})
                        self.bar.finish()
                        logging.info('MONITOR: Returning')
                        return
                        #break
                time.sleep(0.1)


if __name__ == '__main__':

    in_queue2 = queue.Queue()
    out_queue2 = queue.Queue()

    parts = 200
    monitor = Monitor(100, 200, in_queue2, out_queue2)
    monitor.start()

    print('Starting monitor, press q to exit')

    for i in range(100, 201):
        if not out_queue2.empty():
            message = out_queue2.get()
            print(message)
            exit()
        in_queue2.put({'msg': 'PARTS', 'parts': i})
        time.sleep(0.1)

    in_queue2.put({'msg': 'END'})

    monitor.join()
