#!/usr/bin/env python3
"""IL-Jobcrawl daily ops: run checks, routine fixes and the once-a-day attention email.

Run with the project venv's python (paths are resolved from this file):
  python ops/jobcrawl_ops.py check --checkpoint morning|final [--dry-run]
  python ops/jobcrawl_ops.py notify [--dry-run] [--force]
  python ops/jobcrawl_ops.py status
  python ops/jobcrawl_ops.py logs <spider> [--lines N] [--errors]
  python ops/jobcrawl_ops.py rerun <spider>
  python ops/jobcrawl_ops.py kill <spider>
  python ops/jobcrawl_ops.py cleanup-orphans
  python ops/jobcrawl_ops.py attention-add --kind pr|decision|info --title "..." [--detail "..."] [--url PR_URL]

`check` compares each spider's saved jobs today with yesterday's, reads the day's scrapy log to explain
failures, and applies routine fixes: kill hung runs, clean up leftover Xvfb/Chrome, and rerun a failed
spider once while there is still time before the daily endtime. Anything that needs a human is queued, and
`notify` emails the queue at most once a day, only when it is not empty.

State (status files, attention queue, rerun counters) lives in $JOBCRAWL_OPS_HOME (default ~/jobcrawl-ops),
outside the repo. Its config.json holds {"notify_to": "<email address>"}.
"""
import argparse
import collections
import datetime as dt
import fcntl
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil

APP_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = APP_DIR / 'logs'
SCRAPY = Path(sys.executable).with_name('scrapy')
OPS_HOME = Path(os.environ.get('JOBCRAWL_OPS_HOME', Path.home() / 'jobcrawl-ops'))
PR_URL_RE = re.compile(r'^https://github\.com/santoshghimire/IL-Jobcrawl/pull/\d+$')

# spider -> DB Site value, cron start (UTC), longest normal runtime, first checkpoint that checks it
SPIDERS = {
    'jobmaster': dict(site='JobMaster', start='00:00', max_minutes=90, checkpoint='morning'),
    'drushim': dict(site='Drushim', start='04:00', max_minutes=240, checkpoint='morning'),
    'alljobs': dict(site='AllJobs', start='07:00', max_minutes=340, checkpoint='final'),  # runs until the endtime
    'jobnet': dict(site='JobNet', start='10:00', max_minutes=60, checkpoint='final'),
}
REPORT_SPIDER = 'left'  # builds the per-site Excel files into the daily email at 12:45 UTC
SPIDER_FILES = {'left_company_check': 'left'}  # spider module name -> spider name, where they differ
LOW_RATIO = 0.5  # flag a day with fewer than half of the reference day's jobs
AUTO_RERUNS = 1  # reruns `check` may start per spider per day
MAX_RERUNS = 2  # reruns allowed per spider per day, including ones requested via `rerun`
DISK_LIMIT = 0.85
KEEP_DAYS = 30

# Known log patterns -> likely cause, used to explain a failure without reading the whole log
KNOWN_CAUSES = [
    (r'XStartTimeoutError', 'Xvfb did not start - leftover Xvfb/Chrome processes on the host?'),
    (r'status code 4\d\d|status=4\d\d|\(4\d\d\) <GET', 'Site returned HTTP 4xx - URL or API may have changed'),
    (r'Found 0 listing links', 'No listing links on the homepage - site layout may have changed'),
    (r'OperationalError|Access denied|Can\'t connect to MySQL', 'Database connection problem'),
    (r'End run because endtime is reached|Reached Endtime', 'Run hit the daily endtime cutoff'),
    (r'TimeoutException|timed out', 'Page loads timing out - site slow or blocking'),
]

LINE_RE = re.compile(r'^(\d\d-\d\d \d\d:\d\d:\d\d),\d+ p(\d+) \{([^}]*)\} \[(\w+)\] - (.*)$')
EXC_RE = re.compile(r'^([A-Za-z_][\w.]*(?:Error|Exception|Exit|Interrupt))(?::.*)?$')
MARKERS = ('Email Successfully Sent via', 'Job list email sent', 'All done', 'Sending email with mailjet api failed')


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False))
    tmp.replace(path)


def say(msg):
    print('{} {}'.format(utcnow().strftime('%Y-%m-%d %H:%M:%S'), msg), flush=True)


# ---------------------------------------------------------------- data sources

def db_counts(days=8):
    """{date: {Site: jobs saved}} for the last `days` days (Crawl_Date is dd/mm/YYYY)."""
    import pymysql
    from scrapy.utils.project import get_project_settings
    s = get_project_settings()
    conn = pymysql.connect(host=s.get('MYSQL_HOST'), user=s.get('MYSQL_USER'), passwd=s.get('MYSQL_PASSWORD'),
                           db=s.get('MYSQL_DBNAME'), charset='utf8mb4')
    out = collections.defaultdict(dict)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT Crawl_Date, Site, COUNT(*) FROM sites_datas "
                        "WHERE STR_TO_DATE(Crawl_Date, %s) >= CURDATE() - INTERVAL %s DAY GROUP BY Crawl_Date, Site",
                        ('%d/%m/%Y', days))
            for crawl_date, site, n in cur.fetchall():
                out[dt.datetime.strptime(crawl_date, '%d/%m/%Y').date()][site] = n
    finally:
        conn.close()
    return out


def reference_count(counts, site, day):
    """Yesterday's count, or the most recent earlier non-zero day if yesterday saved nothing."""
    for back in range(1, 8):
        d = day - dt.timedelta(days=back)
        n = counts.get(d, {}).get(site, 0)
        if n:
            return {'date': str(d), 'jobs': n}
    return None


def log_path(day):
    return LOG_DIR / 'scrapy_log_output_{:%Y-%m-%d}.txt'.format(day)


def iter_log(day):
    """Yield (pid, line, prefix match or None) for the day's scrapy log.

    Continuation lines (tracebacks, stats dumps) belong to the pid of the last prefixed line."""
    path = log_path(day)
    if not path.exists():
        return
    pid = None
    with path.open(errors='replace') as f:
        for line in f:
            m = LINE_RE.match(line)
            if m:
                pid = m.group(2)
            if pid:
                yield pid, line.rstrip('\n'), m


def parse_runs(day):
    """One record per scrapy process in the day's log: spider, times, how it ended, items, errors."""
    runs = {}
    for pid, line, m in iter_log(day):
        run = runs.setdefault(pid, dict(pid=int(pid), spider=None, start=None, end=None, closed=None, items=None,
                                        errors=collections.Counter(), error_lines=[], exceptions=[], critical=0,
                                        spider_errors=0, markers=set(), missing_files=[]))
        if m:
            ts, _, where, level, msg = m.groups()
            run['start'] = run['start'] or ts
            run['end'] = ts
            sm = re.search(r'/spiders/(\w+)\.py:', where)
            if sm and not run['spider']:
                run['spider'] = SPIDER_FILES.get(sm.group(1), sm.group(1))
            dm = re.match(r'Total Dropped Item count for (\w+)', msg)
            if dm:
                run['spider'] = dm.group(1)
            if msg.startswith('Spider closed ('):
                run['closed'] = msg[len('Spider closed ('):].rstrip(')')
                if run['items'] is None:
                    run['items'] = 0  # scrapy leaves item_scraped_count out of the stats when it is 0
            if level in ('ERROR', 'CRITICAL'):
                run['errors'][re.sub(r'\d+', 'N', msg)[:160]] += 1  # grouped for "top errors"
                run['error_lines'] = (run['error_lines'] + [msg[:300]])[-50:]  # raw, for KNOWN_CAUSES
            if level == 'CRITICAL':
                run['critical'] += 1
            if msg.startswith('Spider error processing'):
                run['spider_errors'] += 1
            for marker in MARKERS:
                if msg.startswith(marker):
                    run['markers'].add(marker)
            fm = re.match(r'(\w+) file not found, skip', msg)
            if fm:
                run['missing_files'].append(fm.group(1))
        else:
            im = re.search(r"'item_scraped_count': (\d+)", line)
            if im:
                run['items'] = int(im.group(1))
            sm = re.search(r'/spiders/(\w+)\.py"', line)
            if sm and not run['spider']:
                run['spider'] = SPIDER_FILES.get(sm.group(1), sm.group(1))
            em = EXC_RE.match(line.strip())
            if em:
                run['exceptions'].append(line.strip()[:300])
    return runs


def spider_procs(name=None):
    """Running `scrapy crawl <spider>` processes as (spider, psutil.Process)."""
    found = []
    for p in psutil.process_iter(['pid', 'cmdline']):
        cmd = p.info['cmdline'] or []
        if 'crawl' in cmd and any(Path(c).name == 'scrapy' for c in cmd):
            i = cmd.index('crawl')
            sp = cmd[i + 1] if i + 1 < len(cmd) else None
            if name is None or sp == name:
                found.append((sp, p))
    return found


def orphan_procs():
    """Xvfb / Chrome / chromedriver / puppeteer node processes owned by this user."""
    me = psutil.Process().username()
    found = []
    for p in psutil.process_iter(['pid', 'name', 'cmdline', 'username']):
        if p.info['username'] != me:
            continue
        n = p.info['name'] or ''
        if n in ('Xvfb', 'chromedriver') or n.startswith('chrome') or (
                n == 'node' and 'crawl.js' in ' '.join(p.info['cmdline'] or [])):
            found.append(p)
    return found


# ---------------------------------------------------------------- actions

def kill_tree(proc):
    """SIGTERM (twice, for scrapy's graceful then forced shutdown), then SIGKILL; children go too."""
    try:
        kids = proc.children(recursive=True)
    except psutil.NoSuchProcess:
        return
    for wait in (30, 15):
        try:
            proc.send_signal(signal.SIGTERM)
        except psutil.NoSuchProcess:
            break
        if not psutil.wait_procs([proc], timeout=wait)[1]:
            break
    else:
        try:
            proc.kill()
            psutil.wait_procs([proc], timeout=5)
        except psutil.NoSuchProcess:
            pass
    terminate_all(kids)


def terminate_all(procs):
    for p in procs:
        try:
            p.terminate()
        except psutil.NoSuchProcess:
            pass
    for p in psutil.wait_procs(procs, timeout=5)[1]:
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass


def rerun_count(day, name):
    return load_json(OPS_HOME / 'reruns.json', {}).get(str(day), {}).get(name, 0)


def start_spider(name, day):
    """Start a spider detached, the same way cron does, and count the rerun."""
    with open(LOG_DIR / 'cron_{}.log'.format(name), 'ab') as log:
        subprocess.Popen([str(SCRAPY), 'crawl', name], cwd=str(APP_DIR), stdin=subprocess.DEVNULL,
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    state = load_json(OPS_HOME / 'reruns.json', {})
    state = {d: v for d, v in state.items() if d >= str(day - dt.timedelta(days=KEEP_DAYS))}
    state.setdefault(str(day), {})[name] = state.get(str(day), {}).get(name, 0) + 1
    save_json(OPS_HOME / 'reruns.json', state)


def rerun_blocker(name, day, limit):
    from jobcrawl.endtime_check import reached_endtime
    if spider_procs(name):
        return 'already running'
    if reached_endtime():
        return 'past the daily endtime, a run now would stop immediately'
    if rerun_count(day, name) >= limit:
        return 'rerun limit reached for today ({})'.format(limit)
    return None


# ---------------------------------------------------------------- check

def diagnose(name, sruns, items_today, ref):
    """Short, human-readable explanation of what the logs say about the spider's runs today."""
    notes = []
    if not sruns:
        notes.append('No log lines for {} today - cron may not have started it, or it died before logging.'.format(name))
    text_blob = []
    for r in sruns[-2:]:
        notes.append('run p{pid} {start} -> {end} UTC: ended={ended}, items scraped={items}'.format(
            pid=r['pid'], start=r['start'], end=r['end'],
            ended=r['closed'] or 'no "Spider closed" (crashed or killed)',
            items=r['items'] if r['items'] is not None else 'n/a'))
        if r['exceptions']:
            notes.append('last exception: ' + r['exceptions'][-1])
        if r['errors']:
            notes.append('top errors: ' + '; '.join('{}x {}'.format(n, msg) for msg, n in r['errors'].most_common(3)))
        text_blob += r['exceptions'] + r['error_lines']
    if sruns and sruns[-1]['closed'] == 'finished' and not sruns[-1]['items']:
        notes.append('likely cause: spider finished without scraping anything - selectors/API may have changed')
    blob = '\n'.join(text_blob)
    for pattern, cause in KNOWN_CAUSES:
        if re.search(pattern, blob):
            notes.append('likely cause: ' + cause)
    if ref and items_today and items_today < LOW_RATIO * ref['jobs']:
        notes.append('saved {} jobs vs {} on {} ({:.0%})'.format(items_today, ref['jobs'], ref['date'],
                                                                 items_today / ref['jobs']))
    return notes


def evaluate(name, cfg, runs, counts, day):
    """counts is None when the database is unreachable: job counts are then unknown and only the log is used."""
    sruns = sorted((r for r in runs.values() if r['spider'] == name), key=lambda r: r['start'])
    items_today = counts.get(day, {}).get(cfg['site'], 0) if counts is not None else None
    ref = reference_count(counts, cfg['site'], day) if counts is not None else None
    procs = spider_procs(name)
    res = dict(spider=name, site=cfg['site'], cron_start=cfg['start'], jobs_saved_today=items_today, reference=ref,
               runs=[dict(pid=r['pid'], start=r['start'], end=r['end'], ended=r['closed'], items_scraped=r['items'],
                          errors=sum(r['errors'].values()), exceptions=r['exceptions'][-1:]) for r in sruns],
               running=[], issues=[])
    for _, p in procs:
        minutes = int((time.time() - p.create_time()) / 60)
        res['running'].append(dict(pid=p.pid, minutes=minutes))
        if minutes > cfg['max_minutes']:
            res['issues'].append('hung: running {} min (normal max {})'.format(minutes, cfg['max_minutes']))
    if not procs:
        if not sruns:
            res['issues'].append('did not run today')
        if items_today == 0:
            res['issues'].append('0 jobs saved today')
        elif ref and items_today < LOW_RATIO * ref['jobs']:
            res['issues'].append('suspiciously low: {} jobs vs {} on {}'.format(items_today, ref['jobs'], ref['date']))
        # handled exceptions (logger.exception + retry) are normal; these mean the run itself broke
        if any(r['critical'] or r['spider_errors'] for r in sruns):
            res['issues'].append('crashed: unhandled exception in the spider')
        elif sruns and not sruns[-1]['closed']:
            res['issues'].append('last run ended without "Spider closed" (killed or died)')
    res['status'] = 'issue' if res['issues'] else ('running' if procs else 'ok')
    res['diagnosis'] = diagnose(name, sruns, items_today, ref) if res['issues'] else []
    return res


def check_report(runs):
    lruns = [r for r in runs.values() if r['spider'] == REPORT_SPIDER]
    markers = set().union(*(r['markers'] for r in lruns)) if lruns else set()
    res = dict(ran=bool(lruns), issues=[], missing_files=sorted({f for r in lruns for f in r['missing_files']}))
    if not lruns:
        res['issues'].append('report spider did not run today')
    elif 'Job list email sent' not in markers or 'Email Successfully Sent via' not in markers:
        res['issues'].append('report email not confirmed in the log')
    if 'Sending email with mailjet api failed' in markers:
        res['issues'].append('report email failed via both SMTP and Mailjet')
    if res['missing_files']:
        res['issues'].append('report sent without: ' + ', '.join(res['missing_files']))
    exc = [e for r in lruns for e in r['exceptions']]
    if exc:
        res['issues'].append('report spider crashed: ' + exc[-1])
    return res


def queue_attention(key, kind, title, details=(), url=None):
    items = load_json(OPS_HOME / 'attention.json', [])
    for it in items:
        if it['key'] == key:
            if not it.get('sent_at'):
                it.update(title=title, details=list(details), url=url)
            break
    else:
        items.append(dict(key=key, kind=kind, title=title, details=list(details), url=url,
                          created_at=utcnow().isoformat(), sent_at=None, reminded_at=None))
    cutoff = (utcnow() - dt.timedelta(days=KEEP_DAYS)).isoformat()
    items = [it for it in items if not it.get('sent_at') or it['created_at'] >= cutoff]
    save_json(OPS_HOME / 'attention.json', items)


def cmd_check(args):
    now = utcnow()
    day = now.date()
    checkpoint = args.checkpoint
    actions, dry = [], args.dry_run
    names = [n for n, c in SPIDERS.items() if checkpoint == 'final' or c['checkpoint'] == 'morning']

    def act(desc, fn, *a):
        actions.append(('would ' if dry else '') + desc)
        say(actions[-1])
        if not dry:
            fn(*a)

    # hung runs first, so they are evaluated as failed below
    for name in names:
        for _, p in spider_procs(name):
            minutes = (time.time() - p.create_time()) / 60
            if minutes > SPIDERS[name]['max_minutes']:
                act('kill hung {} (pid {}, {:.0f} min)'.format(name, p.pid, minutes), kill_tree, p)
    if not spider_procs():
        leftovers = orphan_procs()
        if leftovers:
            act('clean up {} leftover Xvfb/Chrome/node processes'.format(len(leftovers)), terminate_all, leftovers)

    import pymysql
    try:
        counts, db_error = db_counts(), None
    except pymysql.err.MySQLError as exc:
        counts, db_error = None, '{}: {}'.format(type(exc).__name__, str(exc)[:200])
        service = subprocess.run(['systemctl', 'is-active', 'mysql'], capture_output=True, text=True).stdout.strip()
        say('database unreachable ({}); mysql service: {}'.format(db_error, service or 'unknown'))
        if not dry:
            queue_attention('{}:database'.format(day), 'issue', 'database unreachable - spiders cannot save jobs',
                            [db_error, 'mysql service: ' + (service or 'unknown'),
                             'job counts unknown; spider checks below are from the logs only'])
    runs = parse_runs(day)
    spiders = {n: evaluate(n, SPIDERS[n], runs, counts, day) for n in names}

    for name, res in spiders.items():
        if res['status'] != 'issue':
            continue
        can_rerun = res['jobs_saved_today'] == 0 or any(i.startswith('hung') for i in res['issues'])
        if db_error:
            blocker = 'database unreachable - a rerun could not save anything'
        elif can_rerun:
            blocker = rerun_blocker(name, day, AUTO_RERUNS)
        else:
            blocker = 'partial run - rerun would repeat it'
        if not blocker:
            act('rerun {} ({})'.format(name, '; '.join(res['issues'])), start_spider, name, day)
            res['rerun_started'] = not dry
            continue
        res['rerun_blocked'] = blocker
        if rerun_count(day, name):
            res['issues'].append('still failing after {} rerun(s) today'.format(rerun_count(day, name)))
        title = '{}: {}'.format(name, '; '.join(res['issues']))
        if not dry:
            queue_attention('{}:{}'.format(day, name), 'issue', title, res['diagnosis'] + ['no rerun: ' + blocker])

    report = None
    if checkpoint == 'final':
        report = check_report(runs)
        if report['issues'] and not dry:
            queue_attention('{}:report'.format(day), 'issue', 'daily report: ' + '; '.join(report['issues']))

    usage = shutil.disk_usage('/')
    disk = usage.used / usage.total
    if disk > DISK_LIMIT and not dry:
        queue_attention('{}:disk'.format(day), 'issue', 'disk {:.0%} full'.format(disk))

    status = dict(date=str(day), checkpoint=checkpoint, generated_at=now.isoformat(), dry_run=dry,
                  jobs_today=None if counts is None else {
                      c['site']: counts.get(day, {}).get(c['site'], 0) for c in SPIDERS.values()},
                  db_error=db_error,
                  spiders=spiders, report=report, disk_used=round(disk, 3), actions=actions)
    if not dry:
        save_json(OPS_HOME / 'status' / '{}-{}.json'.format(day, checkpoint), status)
        save_json(OPS_HOME / 'status' / 'latest.json', status)
        prune(OPS_HOME / 'status')
    for name, res in spiders.items():
        say('{:<10} {:<8} jobs={:<6} ref={} {}'.format(name, res['status'], str(res['jobs_saved_today']),
                                                        (res['reference'] or {}).get('jobs', '-'),
                                                        '; '.join(res['issues'])))
        for note in res['diagnosis']:
            say('    ' + note)
        if res.get('rerun_blocked'):
            say('    no rerun: ' + res['rerun_blocked'])
    if report:
        say('report     {}'.format('; '.join(report['issues']) or 'ok'))


def prune(folder):
    cutoff = time.time() - KEEP_DAYS * 86400
    for f in folder.glob('*.json'):
        if f.name != 'latest.json' and f.stat().st_mtime < cutoff:
            f.unlink()


# ---------------------------------------------------------------- notify

def pr_is_open(url):
    import requests
    number = url.rstrip('/').rsplit('/', 1)[-1]
    try:
        r = requests.get('https://api.github.com/repos/santoshghimire/IL-Jobcrawl/pulls/' + number, timeout=15)
        return r.ok and r.json().get('state') == 'open'
    except (requests.RequestException, ValueError):
        return False


def cmd_notify(args):
    now = utcnow()
    day = str(now.date())
    state = load_json(OPS_HOME / 'notify_state.json', {})
    if state.get('last_sent') == day and not args.force:
        say('attention email already sent today; nothing to do')
        return
    final = load_json(OPS_HOME / 'status' / '{}-final.json'.format(day), None)
    if final is None and not args.dry_run:
        queue_attention('{}:checker'.format(day), 'issue', 'final check did not run today - status unknown')

    items = load_json(OPS_HOME / 'attention.json', [])
    new = [it for it in items if not it.get('sent_at')]
    reminders = [it for it in items if it['kind'] == 'pr' and it.get('sent_at') and not it.get('reminded_at')
                 and it.get('url') and now - dt.datetime.fromisoformat(it['sent_at']) >= dt.timedelta(days=3)
                 and pr_is_open(it['url'])]
    if not new and not reminders:
        say('nothing needs attention; no email')
        return

    lines = ['IL-Jobcrawl - {} item(s) need your attention.'.format(len(new) + len(reminders)), '']
    for it in new:
        lines.append('* ' + it['title'])
        lines += ['    ' + d for d in it.get('details') or []]
        if it.get('url'):
            lines.append('    ' + it['url'])
    for it in reminders:
        lines.append('* Reminder - PR still open: {} {}'.format(it['title'], it['url']))
    if final:
        jobs = final.get('jobs_today')
        lines += ['', 'Jobs saved today: ' + (', '.join('{} {}'.format(s, n) for s, n in jobs.items())
                                              if jobs else 'unknown (database unreachable)')]
        auto = [a for st in ('morning', 'final')
                for a in load_json(OPS_HOME / 'status' / '{}-{}.json'.format(day, st), {}).get('actions', [])]
        if auto:
            lines += ['Routine fixes applied today:'] + ['  - ' + a for a in auto]
    body = '\n'.join(lines) + '\n'
    subject = 'IL-Jobcrawl: {} item(s) need attention ({})'.format(len(new) + len(reminders), day)
    if args.dry_run:
        print('Subject: ' + subject + '\n\n' + body)
        return

    to = load_json(OPS_HOME / 'config.json', {}).get('notify_to')
    if not to:
        sys.exit('notify_to missing in {}'.format(OPS_HOME / 'config.json'))
    from jobcrawl.mailer import send_plain_email
    send_plain_email(subject, body, to=to)
    sent = now.isoformat()
    for it in items:
        if it in new:
            it['sent_at'] = sent
        if it in reminders:
            it['reminded_at'] = sent
    save_json(OPS_HOME / 'attention.json', items)
    save_json(OPS_HOME / 'notify_state.json', dict(last_sent=day))
    say('attention email sent: {} new, {} reminder(s)'.format(len(new), len(reminders)))


# ---------------------------------------------------------------- small commands (also used by the agent)

def cmd_status(args):
    print(json.dumps(load_json(OPS_HOME / 'status' / 'latest.json', {}), indent=2, ensure_ascii=False))


def cmd_logs(args):
    day = dt.date.fromisoformat(args.date) if args.date else utcnow().date()
    runs = sorted((r for r in parse_runs(day).values() if r['spider'] == args.spider), key=lambda r: r['start'])
    if not runs:
        sys.exit('no {} run in the {} log'.format(args.spider, day))
    pid = str(runs[-1]['pid'])
    lines = [line for p, line, m in iter_log(day) if p == pid]
    if args.errors:  # ERROR/CRITICAL lines plus the traceback lines that follow them
        keep, in_error = [], False
        for line in lines:
            m = LINE_RE.match(line)
            if m:
                in_error = m.group(4) in ('ERROR', 'CRITICAL')
            if in_error:
                keep.append(line)
        lines = keep
    print('\n'.join(lines[-min(args.lines, 1000):]))


def cmd_rerun(args):
    day = utcnow().date()
    blocker = rerun_blocker(args.spider, day, MAX_RERUNS)
    if blocker:
        sys.exit('not rerunning {}: {}'.format(args.spider, blocker))
    start_spider(args.spider, day)
    say('started {} (rerun {} of {} today)'.format(args.spider, rerun_count(day, args.spider), MAX_RERUNS))


def cmd_kill(args):
    procs = spider_procs(args.spider)
    if not procs:
        sys.exit('{} is not running'.format(args.spider))
    for _, p in procs:
        kill_tree(p)
        say('stopped {} (pid {})'.format(args.spider, p.pid))


def cmd_cleanup(args):
    if spider_procs():
        sys.exit('a spider is running; not touching Xvfb/Chrome')
    procs = orphan_procs()
    terminate_all(procs)
    say('cleaned up {} leftover processes'.format(len(procs)))


def cmd_attention_add(args):
    if args.url and not PR_URL_RE.match(args.url):
        sys.exit('url must be a pull request URL of this repo')

    def clean(s):
        return re.sub(r'[\x00-\x1f\x7f]', ' ', s).strip()[:300]
    title = clean(args.title)
    if not title:
        sys.exit('title is required')
    key = '{}:{}:{}'.format(utcnow().date(), args.kind, args.url or title)
    queue_attention(key, args.kind, title, [clean(d) for d in args.detail][:10], args.url)
    say('queued for the next attention email: ' + title)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='cmd', required=True)
    c = sub.add_parser('check')
    c.add_argument('--checkpoint', choices=['morning', 'final'], required=True)
    c.add_argument('--dry-run', action='store_true', help='report only: no fixes, no state changes')
    n = sub.add_parser('notify')
    n.add_argument('--dry-run', action='store_true', help='print the email instead of sending it')
    n.add_argument('--force', action='store_true', help='send even if an email already went out today')
    sub.add_parser('status')
    lg = sub.add_parser('logs')
    lg.add_argument('spider', choices=sorted(SPIDERS) + [REPORT_SPIDER])
    lg.add_argument('--lines', type=int, default=200)
    lg.add_argument('--errors', action='store_true', help='only ERROR/CRITICAL lines and their tracebacks')
    lg.add_argument('--date', help='YYYY-MM-DD (default today, UTC)')
    for name in ('rerun', 'kill'):
        sub.add_parser(name).add_argument('spider', choices=sorted(SPIDERS))
    sub.add_parser('cleanup-orphans')
    a = sub.add_parser('attention-add')
    a.add_argument('--kind', choices=['pr', 'decision', 'info'], required=True)
    a.add_argument('--title', required=True)
    a.add_argument('--detail', action='append', default=[])
    a.add_argument('--url')
    args = p.parse_args()

    os.chdir(APP_DIR)
    sys.path.insert(0, str(APP_DIR))
    handler = {'check': cmd_check, 'notify': cmd_notify, 'status': cmd_status, 'logs': cmd_logs,
               'rerun': cmd_rerun, 'kill': cmd_kill, 'cleanup-orphans': cmd_cleanup,
               'attention-add': cmd_attention_add}[args.cmd]
    if args.cmd in ('status', 'logs') or getattr(args, 'dry_run', False):
        return handler(args)
    OPS_HOME.mkdir(parents=True, exist_ok=True)
    with open(OPS_HOME / 'ops.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            handler(args)
        except Exception as exc:
            if args.cmd in ('check', 'notify'):
                queue_attention('{}:ops-{}'.format(utcnow().date(), args.cmd), 'issue',
                                'ops {} failed: {}: {}'.format(args.cmd, type(exc).__name__, str(exc)[:200]))
            raise


if __name__ == '__main__':
    main()
