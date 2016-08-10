import subprocess
import re


def is_process_running(process_name):
    ps = subprocess.Popen('ps -ef | grep "{}"'.format(process_name), shell=True, stdout=subprocess.PIPE)
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()

    grep_count = len(re.findall('grep', output))
    if len(re.findall(process_name, output)) > grep_count:
        return True
    else:
        return False


if __name__ == '__main__':

    print is_process_running("worker.py")
