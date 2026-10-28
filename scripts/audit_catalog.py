#!/usr/bin/env python3
"""Public Gamma catalog comparison. Stdlib, serial GETs, no accounts or CLOB.

No raw pages or full ID lists are persisted. Checkpoints are NOT resumable:
restarting is a new walk and must be separately authorized. `complete` means
keyset termination only; unresolved records still prevent eligibility certainty.
Rules independently transcribed from src/live.rs parse_market/discover.
"""
import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ORIGIN = 'https://gamma-api.polymarket.com'
MAX_BYTES = 32 * 1024 * 1024
MAX_PAGES = 10000
FLAGS = (('active', True), ('closed', False), ('enableOrderBook', True), ('acceptingOrders', True))


class AuditStop(Exception): pass
class AccessDenied(AuditStop): pass
class PageTooLarge(AuditStop): pass


def utc():
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Record:
    kind: str
    version: str = 'unknown'
    assets: tuple = ()
    provisional: bool = False
    malformed: bool = False
    reason: str = ''
    market_id: str = ''


def parse_market(m):
    """Preserve inactive precedence, nullable uncertainty, and canonical space."""
    if not isinstance(m, dict):
        return Record('unresolved', malformed=True, reason='market_not_object')
    version = m.get('version') if m.get('version') in ('v1', 'v2') else 'unknown'
    mid = m.get('id') if isinstance(m.get('id'), str) and m['id'] else ''
    def result(kind, reason='', **kwargs):
        return Record(kind, version=version, reason=reason, market_id=mid, **kwargs)
    for flag, expected in FLAGS:
        if type(m.get(flag)) is bool and m[flag] != expected:
            return result('excluded', 'explicit_' + flag)
    provisional = False
    for flag, _ in FLAGS:
        if m.get(flag) is None:
            provisional = True
        elif type(m[flag]) is not bool:
            return result('unresolved', 'malformed_flag_' + flag, malformed=True)
    if not mid:
        return result('unresolved', 'missing_market_id', malformed=True)
    value = m.get('clobTokenIds' if version == 'v1' else 'positionIds')
    if version == 'unknown' or value is None:
        return result('unresolved', 'canonical_unavailable_or_unsupported')
    try:
        if version == 'v1':
            if not isinstance(value, str): raise ValueError()
            ids = strict_json(value)
        else:
            ids = value
        if not isinstance(ids, list) or any(not isinstance(x, str) for x in ids):
            raise ValueError()
    except (ValueError, TypeError, RecursionError):
        return result('unresolved', 'malformed_canonical_ids', malformed=True)
    if not ids:
        return result('unresolved', 'canonical_unavailable_or_unsupported')
    if len(set(ids)) != len(ids) or any(not x.strip() or x != x.strip() for x in ids):
        return result('unresolved', 'duplicate_empty_or_padded_id', malformed=True)
    if m.get('outcomes') is not None:
        try:
            outcomes = strict_json(m['outcomes']) if isinstance(m['outcomes'], str) else m['outcomes']
            if not isinstance(outcomes, list) or len(outcomes) != len(ids): raise ValueError()
        except (ValueError, TypeError, RecursionError):
            return result('unresolved', 'malformed_or_misaligned_outcomes', malformed=True)
    return result('candidate', assets=tuple(ids), provisional=provisional)


def strict_json(value):
    def invalid(_): raise ValueError('nonstandard JSON constant')
    return json.loads(value, parse_constant=invalid)


def page_url(endpoint, limit, cursor, filters=()):
    if endpoint not in ('events', 'markets') or not 1 <= limit <= 100:
        raise ValueError('unsupported endpoint or limit')
    if any(k in {'closed','version','limit','after_cursor','offset'} for k, _ in filters):
        raise ValueError('reserved filter')
    query = list(filters) + [('closed','false'),('version','v1'),('version','v2'),('limit',str(limit))]
    if cursor is not None: query.append(('after_cursor', cursor))
    return ORIGIN + '/' + endpoint + '/keyset?' + urllib.parse.urlencode(query)


def next_cursor(page, page_len, limit):
    if 'next_cursor' not in page:
        if page_len >= limit: raise AuditStop('full_page_missing_cursor')
        return None
    value = page['next_cursor']
    if value is None or value == '': return None
    if not isinstance(value, str): raise AuditStop('malformed_cursor')
    return value


def digest(ids):
    h = hashlib.sha256()
    for value in sorted(ids): h.update(value.encode('utf-8') + b'\n')
    return h.hexdigest()


class Walk:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.ids = set()
        self.by_version = {'v1':set(), 'v2':set()}
        self.provisional = set()
        self.market_ids = set()
        self.counts = Counter()
        self.reasons = Counter()
        self.versions = {v:Counter() for v in ('v1','v2','unknown')}
        self.pages = 0
        self.limit = 100
        self.complete = False
        self.reason = 'not_started'
        self.started = None
        self.ended = None
        self.elapsed = 0.0
        self.cursor_present = False

    def add(self, m):
        r = parse_market(m)
        self.counts['market_records'] += 1
        if r.market_id:
            if r.market_id in self.market_ids: self.counts['duplicate_market_records'] += 1
            self.market_ids.add(r.market_id)
        self.versions[r.version][r.kind] += 1
        if r.kind == 'candidate':
            self.counts['candidate_market_records'] += 1
            self.ids.update(r.assets)
            self.by_version[r.version].update(r.assets)
            if r.provisional:
                self.counts['provisional_market_records'] += 1
                self.provisional.update(r.assets)
        else:
            self.counts['excluded_markets' if r.kind == 'excluded' else 'unresolved_markets'] += 1
            self.reasons[r.reason] += 1
            if r.malformed: self.counts['malformed_markets'] += 1

    def summary(self, include_digest=True):
        counts = {k:self.counts[k] for k in ('market_records','candidate_market_records',
                  'duplicate_market_records','provisional_market_records','unresolved_markets',
                  'malformed_markets','excluded_markets','unresolved_events')}
        value = dict(endpoint='/' + self.endpoint + '/keyset', complete=self.complete,
                    status=self.reason, started_at=self.started, ended_at=self.ended,
                    elapsed_seconds=round(self.elapsed,3), pages=self.pages,
                    current_limit=self.limit, cursor_pending=self.cursor_present,
                    **counts, distinct_market_ids=len(self.market_ids),
                    canonical_candidates=len(self.ids), provisional_candidates=len(self.provisional),
                    confirmed_candidates=len(self.ids)-len(self.provisional),
                    eligibility_fully_resolved=(self.complete and not self.counts['unresolved_markets']
                                               and not self.counts['unresolved_events'] and not self.provisional),
                    by_version={v:dict(c, canonical_candidates=len(self.by_version.get(v,())))
                                for v,c in self.versions.items()},
                    reasons=dict(sorted(self.reasons.items())))
        if include_digest:
            value.update(sorted_id_sha256=digest(self.ids),
                         sha256_scope='unique candidate ID strings sorted lexically; UTF-8 ID + LF; prefix if incomplete',
                         version_id_sha256={v:digest(ids) for v,ids in self.by_version.items()})
        return value

    def clear(self):
        self.ids.clear(); self.provisional.clear(); self.market_ids.clear()
        for ids in self.by_version.values(): ids.clear()


def walk(w, fetch, checkpoint, max_pages=MAX_PAGES):
    started = time.monotonic()
    w.started = utc(); w.reason = 'running'
    cursor = None
    visited = set()
    try:
        while w.pages < min(max_pages, MAX_PAGES):
            try:
                page = fetch(page_url(w.endpoint, w.limit, cursor))
            except PageTooLarge:
                if w.limit == 1: raise AuditStop('page_exceeds_32MiB_at_limit_1')
                w.limit = max(1, w.limit // 2)
                checkpoint()
                continue
            if not isinstance(page, dict) or not isinstance(page.get(w.endpoint), list):
                raise AuditStop('malformed_page_schema')
            records = page[w.endpoint]
            w.pages += 1
            if w.endpoint == 'events':
                for event in records:
                    if not isinstance(event, dict) or not isinstance(event.get('markets'), list):
                        w.counts['unresolved_events'] += 1
                    else:
                        for m in event['markets']: w.add(m)
            else:
                for m in records: w.add(m)
            cursor = next_cursor(page, len(records), w.limit)
            w.cursor_present = cursor is not None
            if cursor is None:
                w.complete = True; w.reason = 'terminal_cursor'
                break
            if cursor in visited: raise AuditStop('repeated_cursor')
            visited.add(cursor)
            w.elapsed = time.monotonic()-started
            checkpoint()
        if not w.complete: raise AuditStop('page_safety_limit')
    except AccessDenied:
        w.reason = 'access_denied_no_retry'
        raise
    except AuditStop as error:
        w.reason = str(error)
    finally:
        w.ended = utc(); w.elapsed = time.monotonic()-started
        visited.clear()
        checkpoint()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


class Client:
    def __init__(self, monotonic=time.monotonic, sleep=time.sleep, opener=None, deadline=None, max_bytes=MAX_BYTES):
        self.monotonic = monotonic; self.sleep = sleep
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.deadline = deadline if deadline is not None else monotonic()+3000
        self.last_end = None; self.requests = 0; self.retries = 0
        self.max_bytes = min(max_bytes, MAX_BYTES)

    def check(self):
        if self.monotonic() >= self.deadline: raise AuditStop('overall_deadline')

    def pause(self, seconds):
        self.check()
        if seconds >= self.deadline-self.monotonic(): raise AuditStop('overall_deadline_before_wait')
        self.sleep(seconds); self.check()

    def fetch(self, url):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != 'https' or parsed.netloc != 'gamma-api.polymarket.com' or parsed.path not in ('/events/keyset','/markets/keyset'):
            raise AuditStop('non_Gamma_endpoint_refused')
        for attempt in range(4):
            self.check()
            if self.last_end is not None:
                wait = .301 - (self.monotonic()-self.last_end)
                if wait > 0: self.pause(wait)
            retry_after = None
            try:
                request = urllib.request.Request(url, headers={'Accept':'application/json', 'Accept-Encoding':'identity',
                                                                'User-Agent':'orderbook-public-catalog-audit/1'})
                self.requests += 1
                with self.opener(request, timeout=max(.001,min(25,self.deadline-self.monotonic()))) as response:
                    if response.status != 200: raise AuditStop('unexpected_http_status')
                    length = response.headers.get('Content-Length')
                    if length and int(length) > self.max_bytes: raise PageTooLarge()
                    body = bytearray()
                    while True:
                        self.check()
                        chunk = response.read(min(65536,self.max_bytes+1-len(body)))
                        if not chunk: break
                        body.extend(chunk)
                        if len(body) > self.max_bytes: raise PageTooLarge()
                    self.check()
                    try: return strict_json(body)
                    except (ValueError,UnicodeError,RecursionError): raise AuditStop('invalid_page_json') from None
            except urllib.error.HTTPError as error:
                code = error.code
                header = error.headers.get('Retry-After','') if error.headers else ''
                error.close()
                if code in (401,403): raise AccessDenied('Gamma_HTTP_' + str(code)) from None
                if code not in (408,429,500,502,503,504): raise AuditStop('Gamma_HTTP_' + str(code)) from None
                if header.strip().isdigit(): retry_after = int(header.strip())
            except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError, http.client.IncompleteRead, http.client.RemoteDisconnected):
                pass
            finally:
                self.last_end = self.monotonic()
            if attempt == 3: raise AuditStop('transient_retries_exhausted')
            self.retries += 1
            self.pause(retry_after if retry_after is not None else min(8, 2**attempt))


def compare(events, markets):
    def direction(left, right):
        count = 0; samples=[]; by_version={}
        for version in ('v1','v2'):
            diff = left.by_version[version]-right.by_version[version]
            by_version[version] = len(diff)
            count += len(diff)
            # Bounded samples; no retained full difference sets on disk.
            import heapq
            for value in heapq.nsmallest(max(0,8-len(samples)),diff):
                samples.append(dict(id=value, version=version,
                                    reason='absent_from_other_version_candidate_set_scope_or_time_unresolved'))
        return dict(count=count, by_version=by_version, samples=samples)
    left=direction(events,markets); right=direction(markets,events)
    return dict(both_walks_complete=events.complete and markets.complete,
                events_only=left, markets_only=right,
                symmetric_difference=left['count']+right['count'],
                comparison_unit='(version, canonical ID); endpoint SHA256 uses unversioned collector candidate IDs',
                caveat='Sequential mutable catalog sweeps are not simultaneous snapshots. Differences can reflect walk-time changes, scope, provisional flags, or unresolved records; no exchange-universe certification.')


def write_json(path, value):
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
    temporary.replace(path)


def command(args):
    # Short-lived metadata children are synchronously reaped by subprocess.run.
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=10, check=False).stdout
    except (OSError,subprocess.TimeoutExpired): return 'metadata_unavailable'


def resources():
    processes = command(['ps','-axo','pid=,ppid=,pgid=,rss=,comm='])
    rows=[]; groups={}
    for line in processes.splitlines():
        fields=line.strip().split(None,4)
        if len(fields)!=5: continue
        try: pid,ppid,pgid,rss=map(int,fields[:4])
        except ValueError: continue
        name=Path(fields[4]).name
        rows.append(dict(pid=pid,ppid=ppid,pgid=pgid,rss_kib=rss,process=name))
        g=groups.setdefault(pgid,dict(pgid=pgid,rss_kib=0,processes=0))
        g['rss_kib']+=rss; g['processes']+=1
    return dict(at=utc(),memory_pressure=command(['memory_pressure','-Q']).strip()[:1500],
                largest_processes=sorted(rows,key=lambda x:-x['rss_kib'])[:12],
                largest_groups=sorted(groups.values(),key=lambda x:-x['rss_kib'])[:10],
                owner_collector=command(['ps','-p','2929','-o','pid=,ppid=,lstart=,rss=,comm=']).strip()[:1000])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--max-seconds',type=float,default=3000)
    args=parser.parse_args()
    if not 0 < args.max_seconds <= 3000: parser.error('max-seconds must be in (0,3000]')
    evidence=args.evidence_dir.resolve(); evidence.mkdir(parents=True,exist_ok=True)
    if (evidence/'ownership.json').exists(): parser.error('evidence already has a run; refusing duplicate audit')
    started=time.monotonic(); deadline=started+args.max_seconds
    pid=os.getpid()
    ownership=dict(owner='this catalog audit only',pid=pid,started_at=utc(),port=None,
                   process_start=command(['ps','-p',str(pid),'-o','lstart=']).strip(),
                   stop_command=f'kill -TERM {pid}',
                   foreground=True,daemons=False,servers=False,public_endpoints=[ORIGIN+'/events/keyset',ORIGIN+'/markets/keyset'],
                   max_seconds=args.max_seconds,minimum_request_gap_seconds=.3,request_attempts_per_page=4,
                   max_pages_per_endpoint=MAX_PAGES,max_page_bytes=MAX_BYTES,initial_limit=100,
                   preserved_collector_pid=2929,preserved_collector_start='2026-10-27 18:08:26 America/Toronto')
    write_json(evidence/'ownership.json',ownership)
    before=resources(); write_json(evidence/'resources-before.json',before)
    client=Client(deadline=deadline)
    walks=[Walk('events'),Walk('markets')]
    failure=None
    log_bytes=0
    def checkpoint():
        nonlocal log_bytes
        value=dict(at=utc(),requests=client.requests,retries=client.retries,
                   elapsed_seconds=round(time.monotonic()-started,3),walks=[w.summary(include_digest=False) for w in walks])
        write_json(evidence/'checkpoint.json',value)
        active=next((w for w in walks if w.reason=='running'),None)
        if active and active.pages and active.pages % 25 == 0:
            line=json.dumps(dict(at=utc(),endpoint=active.endpoint,pages=active.pages,
                                 markets=active.counts['market_records'],candidates=len(active.ids)))+'\n'
            if log_bytes+len(line.encode())<=1024*1024:
                with (evidence/'progress.jsonl').open('a') as out: out.write(line)
                log_bytes+=len(line.encode())
    def terminate(signum, frame): raise AuditStop('signal_' + str(signum))
    previous={s:signal.signal(s,terminate) for s in (signal.SIGINT,signal.SIGTERM)}
    try:
        for w in walks:
            client.check()
            walk(w,client.fetch,checkpoint)
            if not w.complete:
                failure=w.reason
                break
    except AccessDenied as error:
        failure=str(error)
    except AuditStop as error:
        failure=str(error)
    except Exception as error:
        # No exception payload: it could contain public records or URLs/cursors.
        failure='unexpected_' + type(error).__name__
    finally:
        for s,handler in previous.items(): signal.signal(s,handler)
        result=dict(started_at=ownership['started_at'],ended_at=utc(),
                    elapsed_seconds=round(time.monotonic()-started,3),
                    complete=all(w.complete for w in walks),stop_reason=failure,
                    requests=client.requests,retries=client.retries,
                    walks=[w.summary() for w in walks],comparison=compare(*walks),
                    collector_rules='src/live.rs::parse_market and discover as read before run; no prior catalog retention in this independent first sweep',
                    exclusions='No WS, CLOB, accounts, browsers, emails, raw pages, or full on-disk ID sets.')
        write_json(evidence/'summary.json',result)
        for w in walks: w.clear()
        after=resources(); write_json(evidence/'resources-after.json',after)
        write_json(evidence/'cleanup.json',dict(at=utc(),owned_pid=pid,ports=[],servers=[],
                    browser_pages=[],child_processes_remaining=[],in_memory_id_sets_cleared=True,
                    metadata_children='subprocess.run synchronously reaped; no persistent child',
                    log_bytes=log_bytes,preserved_collector_pid=2929,
                    uncertainty='System process/memory changes include concurrent owner work. Parent verifies PID exit after this process returns.'))
        print(json.dumps(dict(complete=result['complete'],stop_reason=failure,requests=client.requests,
                              walks=[dict(endpoint=w['endpoint'],pages=w['pages'],markets=w['market_records'],
                                          candidates=w['canonical_candidates'],complete=w['complete']) for w in result['walks']]),sort_keys=True),flush=True)
    return 0 if result['complete'] else 2


if __name__ == '__main__': raise SystemExit(main())
