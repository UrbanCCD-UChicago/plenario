import threading
import time
from plenario_worker.utilities import log


class WorkerBoss(object):

    def __init__(self):

        self.active_worker_count = 0
        self.do_work = True
        self.protected = False
        self.workers = dict()

    def check_on_worker_threads(self, worker_threads):

        for name, thread in worker_threads.items():
            if thread.is_alive():
                continue
            worker_name = thread.getName()
            self.workers[worker_name] = "Dead"

    def rescue(self, worker_threads, worker_fn):
        for name, thread in worker_threads.items():
            if thread.is_alive():
                continue
            # Note that we cannot actually restart a thread. What we are doing
            # is creating a new replacement thread with the same name.
            thread = threading.Thread(target=worker_fn, name=name, args=[name])
            thread.start()
            worker_threads[name] = thread

            log("Revived {}.".format(name), "WORKER BOSS")
            self.workers[name] = "Revived"

    def terminate(self, worker_threads):
        while any(thread.is_alive() for thread in worker_threads.values()):
            for name, thread in worker_threads.items():
                if not thread.is_alive():
                    continue
                thread.join(5)
                self.workers[name] = "Terminated"
            time.sleep(5)
            self.report()

    def report(self):

        result = (
            "Report\n"
            "-----------------------\n"
            "Active Worker Count: {}\n"
            "Do Work: {}\n"
            "Protected: {}\n"
            "-----------------------\n"
            .format(self.active_worker_count, self.do_work, self.protected)
        )

        for name, status in self.workers.items():
            result += "{}: {}\n".format(name, status)
        result += "-----------------------"

        log(result, "WORKER BOSS")

    def stop_working(self, signum, frame):
        log("Received termination signal.", "WORKER BOSS")
        log("signum: {}, frame: {}".format(signum, frame), "WORKER BOSS")
        self.do_work = False
