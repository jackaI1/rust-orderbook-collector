"""Offline contract tests; no sockets, subprocesses, or public requests."""
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
import urllib.error
import urllib.parse

SPEC = importlib.util.spec_from_file_location('audit_catalog', Path(__file__).resolve().parents[1] / 'scripts/audit_catalog.py')
a = importlib.util.module_from_spec(SPEC) if SPEC and SPEC.loader and Path(SPEC.origin).exists() else None
if a:
    sys.modules[SPEC.name] = a
    SPEC.loader.exec_module(a)


def market(**updates):
    m = dict(id='m', version='v1', active=True, closed=False, enableOrderBook=True,
             acceptingOrders=True, clobTokenIds='["a","b"]', positionIds=['wrong'])
    m.update(updates)
    return m


class Clock:
    def __init__(self): self.now = 0.0
    def monotonic(self): return self.now
    def sleep(self, seconds): self.now += seconds


class Response(io.BytesIO):
    status = 200
    def __init__(self, body, headers=None):
        super().__init__(body)
        self.headers = headers or {}


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(a, 'audit implementation is missing')

    def test_version_selects_only_canonical_space(self):
        self.assertEqual(a.parse_market(market()).assets, ('a', 'b'))
        self.assertEqual(a.parse_market(market(version='v2')).assets, ('wrong',))
        self.assertEqual(a.parse_market(market(version='v3')).kind, 'unresolved')

    def test_explicit_inactive_short_circuits_unrelated_malformed_fields(self):
        for flag, value in [('active', False), ('closed', True), ('enableOrderBook', False), ('acceptingOrders', False)]:
            self.assertEqual(a.parse_market({flag: value, 'active_typo': 'bad'}).kind, 'excluded')
        self.assertEqual(a.parse_market(market(active=False, acceptingOrders='bad')).kind, 'excluded')

    def test_nullable_flags_are_provisional_not_confirmed_or_excluded(self):
        m = market(acceptingOrders=None)
        self.assertTrue(a.parse_market(m).provisional)
        del m['active']
        self.assertTrue(a.parse_market(m).provisional)

    def test_malformed_records_are_honestly_unresolved(self):
        bad = [None, [], market(id=7), market(active=1), market(active='true'),
               market(clobTokenIds=['a']), market(clobTokenIds='broken'),
               market(clobTokenIds='[1]'), market(clobTokenIds='["a","a"]'),
               market(clobTokenIds='[" a"]'), market(clobTokenIds='[""]'),
               market(outcomes='["yes"]'), market(outcomes={}),
               market(version='v2', positionIds='["a"]')]
        for m in bad:
            with self.subTest(m=m):
                r = a.parse_market(m)
                self.assertEqual(r.kind, 'unresolved')
                self.assertTrue(r.malformed)

    def test_missing_empty_canonical_ids_are_unresolved_without_invention(self):
        for ids in [None, '[]']:
            r = a.parse_market(market(clobTokenIds=ids))
            self.assertEqual(r.kind, 'unresolved')
            self.assertFalse(r.malformed)
        self.assertEqual(a.parse_market(market(outcomes='["yes","no"]')).kind, 'candidate')

    def test_filters_and_opaque_cursor_survive_encoding(self):
        url = a.page_url('events', 50, 'a+b/=?&', [('tag_id', '123'), ('tag_id', '456')])
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        self.assertEqual(q, {'closed':['false'], 'version':['v1','v2'], 'limit':['50'],
                             'after_cursor':['a+b/=?&'], 'tag_id':['123','456']})
        with self.assertRaises(ValueError): a.page_url('clob', 100, None)
        with self.assertRaises(ValueError): a.page_url('markets', 100, None, [('closed','true')])

    def test_terminal_and_malformed_cursor_rules(self):
        self.assertIsNone(a.next_cursor({'markets': []}, 0, 100))
        self.assertIsNone(a.next_cursor({'next_cursor': None}, 100, 100))
        self.assertIsNone(a.next_cursor({'next_cursor': ''}, 100, 100))
        self.assertEqual(a.next_cursor({'next_cursor': 'opaque'}, 100, 100), 'opaque')
        for page, n in [({},100), ({'next_cursor':7},0)]:
            with self.assertRaises(a.AuditStop): a.next_cursor(page,n,100)

    def walk(self, pages, endpoint='markets', max_pages=10000):
        seen = []
        def fetch(url):
            seen.append(urllib.parse.parse_qs(urllib.parse.urlsplit(url).query))
            value = pages.pop(0)
            if isinstance(value, Exception): raise value
            return value
        w = a.Walk(endpoint)
        a.walk(w, fetch, lambda: None, max_pages=max_pages)
        return w, seen

    def test_walk_flattens_all_event_markets_and_counts_every_record(self):
        w, _ = self.walk([{'events':[{'markets':[market(), market(id='n',acceptingOrders=None)]},
                                      {'markets':None}, {'markets':[market(active=False), market(active='bad')]}],
                          'next_cursor':None}], 'events')
        s = w.summary()
        self.assertTrue(s['complete'])
        self.assertEqual(s['market_records'],4)
        self.assertEqual(s['candidate_market_records'],2)
        self.assertEqual(s['canonical_candidates'],2)
        self.assertEqual(s['provisional_market_records'],1)
        self.assertEqual(s['provisional_candidates'],2)
        self.assertEqual(s['unresolved_markets'],1)
        self.assertEqual(s['malformed_markets'],1)
        self.assertEqual(s['excluded_markets'],1)
        self.assertEqual(s['unresolved_events'],1)
        self.assertFalse(s['eligibility_fully_resolved'])

    def test_walk_follows_dependent_cursor_and_deduplicates_ids(self):
        w, qs = self.walk([{'markets':[market()], 'next_cursor':'a+b'},
                           {'markets':[market()], 'next_cursor':None}])
        self.assertTrue(w.complete)
        self.assertEqual(qs[1]['after_cursor'], ['a+b'])
        self.assertEqual(w.summary()['canonical_candidates'],2)
        self.assertEqual(w.summary()['market_records'],2)
        self.assertEqual(w.summary()['duplicate_market_records'],1)
        self.assertEqual(w.summary()['sorted_id_sha256'], '911169ddaaf146aff539f58c26c489af3b892dff0fe283c1c264c65ae5aa59a2')

    def test_repeated_cursor_full_missing_schema_and_safety_never_complete(self):
        cases = [([{'markets':[], 'next_cursor':'x'},{'markets':[], 'next_cursor':'x'}],3),
                 ([{'markets':[market()]*100}],2), ([{'events':[]}],2),
                 ([{'markets':[], 'next_cursor':'x'}],1)]
        for pages, bound in cases:
            w, _ = self.walk(pages, max_pages=bound)
            self.assertFalse(w.complete)
            self.assertTrue(w.reason)

    def test_oversized_page_reduces_limit_at_same_cursor(self):
        w, qs = self.walk([{'markets':[], 'next_cursor':'next'}, a.PageTooLarge(),
                           {'markets':[], 'next_cursor':None}])
        self.assertTrue(w.complete)
        self.assertEqual([q['limit'] for q in qs], [['100'],['100'],['50']])
        self.assertEqual(qs[1]['after_cursor'], qs[2]['after_cursor'])

    def test_diff_uses_unique_assets_with_version_reason_and_eight_samples(self):
        left = a.Walk('events'); right = a.Walk('markets')
        for n in range(12): left.add(market(id=str(n), clobTokenIds=json.dumps([str(n)])))
        right.add(market(id='r',version='v2',positionIds=['r']))
        d = a.compare(left,right)
        self.assertEqual(d['symmetric_difference'],13)
        self.assertEqual(d['events_only']['count'],12)
        self.assertEqual(len(d['events_only']['samples']),8)
        self.assertEqual(d['events_only']['by_version'], {'v1':12,'v2':0})
        self.assertEqual(set(d['events_only']['samples'][0]), {'id','version','reason'})
        self.assertFalse(d['both_walks_complete'])

    def client(self, answers, cap=1024):
        clock = Clock(); times=[]
        def opener(request, timeout):
            times.append((clock.now, request.full_url))
            answer = answers.pop(0)
            if isinstance(answer, Exception): raise answer
            return answer
        return a.Client(clock.monotonic, clock.sleep, opener, deadline=100, max_bytes=cap), times, clock

    def test_denials_are_never_retried(self):
        for code in [401,403]:
            c, times, _ = self.client([urllib.error.HTTPError('url',code,'denied',{},io.BytesIO(b''))])
            with self.assertRaises(a.AccessDenied): c.fetch(a.page_url('markets',100,None))
            self.assertEqual(len(times),1)

    def test_retry_after_pacing_and_same_url(self):
        c, times, _ = self.client([urllib.error.HTTPError('url',429,'rate',{'Retry-After':'2'},io.BytesIO(b'')),
                                   Response(b'{"markets":[]}'), Response(b'{"markets":[]}')])
        url = a.page_url('markets',100,'opaque')
        c.fetch(url); c.fetch(url)
        self.assertEqual([v for _,v in times],[url]*3)
        self.assertGreaterEqual(times[1][0]-times[0][0],2)
        self.assertGreaterEqual(times[2][0]-times[1][0],.3)

    def test_transient_retries_bounded_and_nontransient_no_retry(self):
        errors = [urllib.error.HTTPError('url',503,'error',{},io.BytesIO(b'')) for _ in range(4)]
        c, times, _ = self.client(errors)
        with self.assertRaises(a.AuditStop): c.fetch(a.page_url('events',100,None))
        self.assertEqual(len(times),4)
        c, times, _ = self.client([urllib.error.HTTPError('url',400,'bad',{},io.BytesIO(b''))])
        with self.assertRaises(a.AuditStop): c.fetch(a.page_url('events',100,None))
        self.assertEqual(len(times),1)

    def test_body_bounds_and_redirect_do_not_trigger_another_request(self):
        for response in [Response(b'{}',{'Content-Length':'1000'}), Response(b'x'*11)]:
            c, times, _ = self.client([response],cap=10)
            with self.assertRaises(a.PageTooLarge): c.fetch(a.page_url('events',100,None))
            self.assertEqual(len(times),1)
        c, times, _ = self.client([urllib.error.HTTPError('url',302,'redirect',{'Location':'https://other'},io.BytesIO(b''))])
        with self.assertRaises(a.AuditStop): c.fetch(a.page_url('events',100,None))
        self.assertEqual(len(times),1)

    def test_checkpoint_summary_has_counts_without_id_sets_or_digests(self):
        w = a.Walk('events'); w.add(market())
        s = w.summary(include_digest=False)
        self.assertEqual(s['canonical_candidates'], 2)
        self.assertNotIn('sorted_id_sha256', s)
        self.assertNotIn('version_id_sha256', s)

    def test_truncated_body_is_bounded_transient_retry(self):
        import http.client
        class Truncated(Response):
            def read(self, size=-1): raise http.client.IncompleteRead(b'partial', 10)
        c, times, _ = self.client([Truncated(b''), Response(b'{"markets":[]}')])
        self.assertEqual(c.fetch(a.page_url('markets',100,None)), {'markets':[]})
        self.assertEqual(len(times), 2)

    def test_main_runs_once_serially_and_denial_skips_second_endpoint(self):
        import contextlib
        import tempfile
        from unittest.mock import patch
        evidence = Path(__file__).resolve().parents[1] / 'evidence/catalog-audit-v8'
        for denied in [False, True]:
            with tempfile.TemporaryDirectory(dir=evidence) as directory:
                calls=[]
                def open_page(request, timeout):
                    endpoint = urllib.parse.urlsplit(request.full_url).path
                    calls.append(endpoint)
                    if denied:
                        raise urllib.error.HTTPError(request.full_url,403,'denied',{},io.BytesIO(b''))
                    key = 'events' if endpoint == '/events/keyset' else 'markets'
                    records = [{'markets':[market()]}] if key == 'events' else [market()]
                    return Response(json.dumps({key:records,'next_cursor':None}).encode())
                with patch.object(sys,'argv',['audit','--evidence-dir',directory,'--max-seconds','10']), \
                     patch.object(a,'resources',return_value={'at':'offline'}), \
                     patch.object(a,'command',return_value='offline-start'), \
                     patch.object(a.urllib.request.OpenerDirector,'open',side_effect=open_page), \
                     contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(a.main(), 2 if denied else 0)
                summary=json.loads((Path(directory)/'summary.json').read_text())
                self.assertEqual(summary['complete'],not denied)
                self.assertEqual(calls,['/events/keyset'] if denied else ['/events/keyset','/markets/keyset'])
                self.assertEqual(summary['walks'][1]['status'],'not_started' if denied else 'terminal_cursor')
                self.assertTrue(json.loads((Path(directory)/'cleanup.json').read_text())['in_memory_id_sets_cleared'])
                with patch.object(sys,'argv',['audit','--evidence-dir',directory]), \
                     contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit): a.main()

    def test_deadline_stops_before_request_or_retry_after(self):
        c, times, clock = self.client([]); clock.now=100
        with self.assertRaises(a.AuditStop): c.fetch(a.page_url('markets',100,None))
        self.assertEqual(times,[])
        c, times, clock = self.client([urllib.error.HTTPError('url',429,'rate',{'Retry-After':'1000'},io.BytesIO(b''))])
        with self.assertRaises(a.AuditStop): c.fetch(a.page_url('markets',100,None))
        self.assertEqual(len(times),1)
        self.assertLessEqual(clock.now,100)


if __name__ == '__main__': unittest.main()
