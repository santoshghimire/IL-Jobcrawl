import os
import time
import signal
import logging
from subprocess import PIPE
from eventlet.green.subprocess import Popen, TimeoutExpired


class JSScraperRunner(object):
    def __init__(self, log):
        super(JSScraperRunner, self).__init__()
        self.log = log
        self.script_name = 'js_puppeteer/crawl.js'
        # self.script_name = 'js_puppeteer/debug.js'
        self._node_binary = '/usr/bin/node'
        self.TIMEOUT = 300

    def run(self, url, fname):
        if not os.path.isfile(self.script_name):
            self.log.error('Script not found (script_name={})'.format(self.script_name))
            return

        args = [self._node_binary, self.script_name, url, fname]
        return self.process_commands(args)

    def process_commands(self, args):
        self.log.info('Calling %s', args)
        t0 = time.time()

        success = False
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        try:
            try:
                res = p.wait(self.TIMEOUT)
            except TimeoutExpired:
                self.log.error('JS crawler timed out, killing it..')
                self._stop_process(p)

                res = -1  # uses -1 to signal timeout error
        finally:
            # ensure the script will finish due to crash or signals
            p.poll()
            output = (p.stdout.read() or '').strip()
            self.log.info("Output of %s => %s", args, output)
            if output and 'Saved' in output:
                success = True

            if not success:
                self.log.info("JS Crawler stdout: %s", output)
            error = p.stderr.read()
            if error:
                self.log.info("JS Crawler stderr: %s", error)
            if p.returncode is None:
                self.log.error('JS Crawler still alive, killing it..')
                self._stop_process(p)

        t1 = time.time()

        if str(res) != '0':
            self.log.error("Failed call to %s(%s), result=%s, time={:.03f}s".format(t1 - t0),
                           self.script_name, args, res)
            return False
        else:
            self.log.info('Success call to %s(%s), result=%s, time={:.03f}s'
                          .format(t1 - t0), self.script_name, args, res)
            return success

    def _stop_process(self, p, tries=2):
        t0 = time.time()
        p.poll()

        while p.returncode is None and tries:
            os.kill(p.pid, signal.SIGTERM)

            self.log.info('Sent SIGTERM to Node script (pid=%d)', p.pid)

            for _step in range(50):
                time.sleep(0.1)
                p.poll()
                if p.returncode is not None:
                    break
            else:
                tries -= 1

        if p.returncode is None:
            os.kill(p.pid, signal.SIGKILL)

            self.log.info('Sent SIGKILL to Node script (pid=%d)', p.pid)

            for _step in range(50):
                time.sleep(0.1)
                p.poll()
                if p.returncode is not None:
                    break

            assert p.returncode is not None, 'Failed to kill Node script'

        self.log.warning('Node script stopped (pid=%d)', p.pid)
        return time.time() - t0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = JSScraperRunner(log=logging)
    # url = 'https://www.alljobs.co.il/SearchResultsGuest.aspx?page=1&position=&type=&freetxt=&city=&region='
    url = 'https://www.jobmaster.co.il/jobs/?l=%D7%A9%D7%A8%D7%95%D7%9F'
    fname = "test_file2.html"
    runner.run(url, fname)
