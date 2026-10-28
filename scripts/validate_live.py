#!/usr/bin/env python3
"""Bounded read-only full-universe validation; owns/reaps its collector and temporary SBE output."""
import argparse,bisect,collections,datetime,hashlib,json,os,signal,subprocess,tempfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def ordinal_batch(ranges, starts, ordinal):
    """Locate a half-open output record range by ordinal, not timestamp alone."""
    index=bisect.bisect_right(starts,ordinal)-1
    if index>=0 and ordinal<ranges[index][1]:
        return ranges[index][2]
    return None

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=int,default=600)
    parser.add_argument('--report-dir',type=Path,required=True)
    parser.add_argument('--discovery-url')
    parser.add_argument('--audit-catalog',action='store_true')
    parser.add_argument('--rotate-secs',type=int)
    parser.add_argument('--replicas',type=int,choices=range(1,129))
    parser.add_argument('--rss-limit-mib',type=int,default=1536)
    parser.add_argument('--output-limit-mib',type=int,default=256)
    args=parser.parse_args()
    assert 30<=args.seconds<=4500
    assert 256<=args.rss_limit_mib<=6144 and 64<=args.output_limit_mib<=8192
    args.report_dir.mkdir(parents=True,exist_ok=True)
    logpath=args.report_dir/'collector.log';health=[];discovery=[];peak_rss=0;reason='duration';process=None
    start=time.monotonic();summary={}
    try:
        with tempfile.TemporaryDirectory(prefix='orderbook-live-') as temp,logpath.open('wb') as logs,open(os.devnull,'wb') as sink:
            output=Path(temp)/'live.sbe'
            command=[str(ROOT/'target/release/orderbook-collector'),'--no-stdout','--output',str(output),'live','--duration-secs',str(args.seconds+10)]
            if args.discovery_url:command += ['--discovery-url',args.discovery_url]
            if args.audit_catalog:command += ['--audit-catalog']
            if args.rotate_secs:command += ['--rotate-secs',str(args.rotate_secs)]
            if args.replicas:command += ['--replicas',str(args.replicas)]
            process=subprocess.Popen(command,stdout=sink,stderr=logs)
            stamp=subprocess.check_output(['ps','-p',str(process.pid),'-o','lstart='],text=True).strip()
            summary={'owner':'orderbook-live-validation','pid':process.pid,'start_time':stamp,'port':'outbound only, no listener',
                'stop_command':f'kill -INT {process.pid}','seconds_limit':args.seconds,'rss_limit_kib':args.rss_limit_mib*1024,'output_limit_bytes':args.output_limit_mib*1024*1024,
                'release_binary_sha256':hashlib.sha256((ROOT/'target/release/orderbook-collector').read_bytes()).hexdigest(),'command':['orderbook-collector']+command[1:],'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'all canonical candidates, including provisional nullable eligibility; no symbol limit'}
            (args.report_dir/'process.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
            offset=0
            while process.poll() is None:
                elapsed=time.monotonic()-start
                if logpath.stat().st_size>1536*1024:reason='log_limit';process.send_signal(signal.SIGINT);break
                if elapsed>=args.seconds:reason='duration_sigint';process.send_signal(signal.SIGINT);break
                rss=subprocess.run(['ps','-p',str(process.pid),'-o','rss='],capture_output=True,text=True).stdout.strip()
                if rss:peak_rss=max(peak_rss,int(rss))
                if peak_rss>args.rss_limit_mib*1024:reason='rss_limit';process.send_signal(signal.SIGINT);break
                if output.exists() and output.stat().st_size>args.output_limit_mib*1024*1024:reason='output_limit';process.send_signal(signal.SIGINT);break
                with logpath.open() as source:
                    source.seek(offset)
                    for line in source:
                        try:record=json.loads(line)
                        except json.JSONDecodeError:continue
                        if record.get('mode')=='live':health.append(record)
                        if record.get('event')=='discovery_complete':discovery.append(record)
                    offset=source.tell()
                time.sleep(2)
            process.wait(timeout=20)
            collection_seconds=time.monotonic()-start
            decode_started=time.monotonic()
            if process.returncode != 0 and reason == 'duration':reason='collector_error'
            if reason=='duration' and time.monotonic()-start<args.seconds-2:reason='early_clean_exit_or_external_signal'
            stop_record=args.report_dir/'stop.json'
            if stop_record.exists():
                stop=json.loads(stop_record.read_text())
                if stop.get('pid')==process.pid and stop.get('start_time')==stamp:reason=stop.get('reason','operator_stop')
            # Parse the final bounded log once to include shutdown and last samples.
            records=[]
            for line in logpath.read_text().splitlines():
                try:records.append(json.loads(line))
                except json.JSONDecodeError:pass
            health=[r for r in records if r.get('mode')=='live']
            discovery=[r for r in records if r.get('event')=='discovery_complete']
            shutdown=[r for r in records if r.get('mode')=='live_shutdown']
            minutes={r['received_ms']:{'received_ms':r['received_ms'],'universe':r['universe'],'emitted':r['symbols'],'missing':r['missing'],'duration_ms':r['duration_ms'],'decoded':0,'ordering_errors':0,'previous':None,
                'record_start':r.get('record_start'),'record_end':r.get('record_end')} for r in records if r.get('mode')=='minute_snapshot'}
            ranges=sorted((b['record_start'],b['record_end'],stamp) for stamp,b in minutes.items() if b['record_start'] is not None)
            starts=[r[0] for r in ranges]
            decoded=0;snapshots=0;deltas=0;symbols=set();zero_deletions=0;snapshot_batches=collections.Counter()
            if output.exists() and output.stat().st_size:
                with tempfile.TemporaryFile() as decoder_errors:
                    inspector=subprocess.Popen([str(ROOT/'target/release/orderbook-collector'),'inspect','--input',str(output)],stdout=subprocess.PIPE,stderr=decoder_errors,text=True)
                    try:
                        for line in inspector.stdout:
                            row=json.loads(line);decoded+=1;symbols.add(row['symbol'])
                            if row['type']=='snapshot':
                                snapshots+=1
                                stamp=row['received_at']
                                snapshot_batches[stamp]+=1
                                millis=ordinal_batch(ranges,starts,decoded-1) if ranges else round(datetime.datetime.fromisoformat(stamp).timestamp()*1000)
                                if millis in minutes:
                                    batch=minutes[millis];batch['decoded']+=1
                                    if batch['previous'] is not None and row['symbol']<=batch['previous']:batch['ordering_errors']+=1
                                    batch['previous']=row['symbol']
                            else:deltas+=1;zero_deletions+=int(row['quantity']==0)
                        inspector.wait(timeout=20)
                        assert inspector.returncode==0,'SBE inspection failed'
                    finally:
                        if inspector.poll() is None:inspector.terminate();inspector.wait(timeout=5)
            endings={}
            for record in records:
                if record.get('event')=='connection_end':
                    key=record.get('reason','unknown');endings[key]=endings.get(key,0)+1
            max_ready=max((r['ready'] for r in health),default=0)
            max_symbols=max((r['symbols'] for r in health),default=0)
            for batch in minutes.values():
                batch.pop('previous')
                batch['decoded_matches_emitted']=batch['decoded']==batch['emitted']
                batch['identification']='output_record_range' if batch['record_start'] is not None else 'timestamp_cohort_may_include_coincident_events'
                batch['complete_ready_universe']=batch['universe']>0 and batch['missing']==0 and batch['decoded_matches_emitted'] and batch['ordering_errors']==0
            summary.update(minute_snapshots=list(minutes.values()),ready_ratio=max_ready/max_symbols if max_symbols else 0,
                connection_ends_by_reason=endings,connection_starts=sum(r.get('event')=='connection_start' for r in records),
                warm_promotions=sum(r.get('event')=='connection_promoted' for r in records),
                scheduled_snapshot_batches=[{'received_at':stamp,'records':ids,'candidate_ratio':ids/max_symbols if max_symbols else 0} for stamp,ids in snapshot_batches.items() if ids>=max(101,max_ready//2)],
                exit_code=process.returncode,stop_reason=reason,collection_seconds=round(collection_seconds,2),decode_and_summary_seconds=round(time.monotonic()-decode_started,2),elapsed_seconds=round(time.monotonic()-start,2),peak_rss_kib=peak_rss,
                complete_sweeps=len(discovery),discovery=discovery[-1] if discovery else None,max_ready=max((r['ready'] for r in health),default=0),
                max_symbols=max((r['symbols'] for r in health),default=0),max_redundant_ready=max((r.get('coverage',{}).get('redundant_ready',0) for r in health),default=0),
                last_health=health[-1] if health else None,shutdown=shutdown[-1] if shutdown else None,
                sbe_bytes=output.stat().st_size if output.exists() else 0,decoded_records=decoded,snapshots=snapshots,deltas=deltas,zero_deletions=zero_deletions,
                symbols_in_sbe=len(symbols),snapshot_batch_count=len(snapshot_batches),largest_snapshot_batches=sorted([{'received_at':stamp,'records':ids} for stamp,ids in snapshot_batches.items()],key=lambda row:row['records'],reverse=True)[:20],process_reaped=True,temporary_output_removed=True)
    finally:
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait()
    (args.report_dir/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k not in ('last_health','discovery','largest_snapshot_batches')}),flush=True)

if __name__=='__main__':main()
