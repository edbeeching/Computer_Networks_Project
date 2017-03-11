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
class Monitor(Thread):
    def __init__(self, parts_needed, in_queue, out_queue):
        # Initialise the thread
        Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.parts_recieved = 0
        self.bar = progressbar.ProgressBar(max_value=parts_needed)


    def run(self):
        while True:
            message = self.in_queue.get()

            if message['msg'] == 'PART':
                self.parts_recieved += 1
                self.bar.update(self.parts_recieved)
            if message['msg'] == 'END':
                break



if __name__ == '__main__':

    in_queue2 = queue.Queue()
    out_queue2 = queue.Queue()

    parts = 200
    monitor = Monitor(parts, in_queue2, out_queue2)
    monitor.start()

    for i in range(parts):
        in_queue2.put({'msg': 'PART'})
        time.sleep(0.1)

    in_queue2.put({'msg': 'END'})

    monitor.join()
