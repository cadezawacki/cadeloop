#!/usr/bin/env python3
"""Independent, bounded cadeloop review harness. Does not use bench/.
Each subprocess benchmark is capped at 15 seconds; scenario payloads target <3s.
"""
from __future__ import annotations
import asyncio, json, os, platform, socket, statistics, subprocess, sys, textwrap, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]; OUT=ROOT/'review'; TIMEOUT=15
CANDIDATES=['stdlib asyncio','cadeloop','uvloop','rloop','rsloop','winloop','tokio (via pyo3-async-runtimes)','libuv (via uvloop)','uvicorn','hypercorn','granian','daphne']
SCENARIOS=['call_soon chain','call_soon burst','timer fire','sleep(0) chain','task fan-out','queue ping-pong','TCP echo 1 KiB','ASGI app exchange']
CODE=r'''
import asyncio,json,socket,statistics,sys,time
scenario=sys.argv[1]; n=int(sys.argv[2]); samples=[]
async def run():
 loop=asyncio.get_running_loop()
 if scenario=='chain':
  fut=loop.create_future(); left=n
  def cb():
   nonlocal left; left-=1
   if left: loop.call_soon(cb)
   else: fut.set_result(None)
  t=time.perf_counter(); loop.call_soon(cb); await fut; return n/(time.perf_counter()-t)
 if scenario=='burst':
  fut=loop.create_future(); left=n
  def cb():
   nonlocal left; left-=1
   if not left: fut.set_result(None)
  t=time.perf_counter()
  for _ in range(n): loop.call_soon(cb)
  await fut; return n/(time.perf_counter()-t)
 if scenario=='timer':
  fut=loop.create_future(); left=n
  def cb():
   nonlocal left; left-=1
   if not left: fut.set_result(None)
  t=time.perf_counter()
  for _ in range(n): loop.call_later(0,cb)
  await fut; return n/(time.perf_counter()-t)
 if scenario=='sleep0':
  t=time.perf_counter()
  for _ in range(n): await asyncio.sleep(0)
  return n/(time.perf_counter()-t)
 if scenario=='tasks':
  async def one(): return 1
  t=time.perf_counter(); await asyncio.gather(*(one() for _ in range(n))); return n/(time.perf_counter()-t)
 if scenario=='queue':
  q=asyncio.Queue(); done=loop.create_future()
  async def pong():
   for _ in range(n): await q.get(); q.put_nowait(1)
   done.set_result(None)
  asyncio.create_task(pong()); t=time.perf_counter(); q.put_nowait(1)
  for _ in range(n): await q.get(); q.put_nowait(1)
  await done; return n/(time.perf_counter()-t)
 if scenario=='asgi':
  async def app(scope,receive,send):
   await receive(); await send({'type':'http.response.start','status':200,'headers':[]}); await send({'type':'http.response.body','body':b'hello'})
  t=time.perf_counter()
  for _ in range(n):
   inbox=[{'type':'http.request','body':b'','more_body':False}]; out=[]
   async def recv(): return inbox.pop()
   async def send(x): out.append(x)
   await app({'type':'http','asgi':{'version':'3.0'}},recv,send)
  return n/(time.perf_counter()-t)
for _ in range(2): asyncio.run(run())
for _ in range(5): samples.append(asyncio.run(run()))
print(json.dumps({'median_ops_s':statistics.median(samples),'min':min(samples),'max':max(samples),'samples':samples}))
'''
def run_bench(name,n):
 p=subprocess.run([sys.executable,'-c',CODE,name,str(n)],text=True,capture_output=True,timeout=TIMEOUT,check=True)
 return json.loads(p.stdout)
def tcp_echo(n=3000):
 code=r'''
import asyncio,json,statistics,time,sys
n=int(sys.argv[1]); vals=[]
async def once():
 async def h(r,w):
  try:
   while d:=await r.readexactly(1024): w.write(d); await w.drain()
  except asyncio.IncompleteReadError: pass
  finally: w.close(); await w.wait_closed()
 srv=await asyncio.start_server(h,'127.0.0.1',0); port=srv.sockets[0].getsockname()[1]
 r,w=await asyncio.open_connection('127.0.0.1',port); payload=b'x'*1024; lat=[]; t=time.perf_counter()
 for _ in range(n):
  q=time.perf_counter_ns(); w.write(payload); await w.drain(); await r.readexactly(1024); lat.append(time.perf_counter_ns()-q)
 elapsed=time.perf_counter()-t; w.close(); await w.wait_closed(); srv.close(); await srv.wait_closed(); return n/elapsed,statistics.median(lat)/1000,sorted(lat)[int(.99*len(lat))-1]/1000
for _ in range(2): asyncio.run(once())
for _ in range(5): vals.append(asyncio.run(once()))
print(json.dumps({'median_ops_s':statistics.median(x[0] for x in vals),'p50_us':statistics.median(x[1] for x in vals),'p99_us':statistics.median(x[2] for x in vals),'samples':vals}))
'''
 p=subprocess.run([sys.executable,'-c',code,str(n)],text=True,capture_output=True,timeout=TIMEOUT,check=True); return json.loads(p.stdout)
def availability():
 rows={}
 for label,mod in [('cadeloop','cadeloop'),('uvloop','uvloop'),('rloop','rloop'),('rsloop','rsloop'),('uvicorn','uvicorn'),('hypercorn','hypercorn'),('granian','granian'),('daphne','daphne')]:
  p=subprocess.run([sys.executable,'-c',f'import {mod}; print(getattr({mod},"__version__","installed"))'],capture_output=True,text=True)
  rows[label]={'available':p.returncode==0,'detail':(p.stdout or p.stderr).strip()[:240]}
 return rows
def main():
 results={}
 for k,n in [('chain',200000),('burst',150000),('timer',80000),('sleep0',150000),('tasks',80000),('queue',80000),('asgi',150000)]:
  try: results[k]=run_bench(k,n)
  except Exception as e: results[k]={'error':repr(e)}
 try: results['tcp_echo']=tcp_echo()
 except Exception as e: results['tcp_echo']={'error':repr(e)}
 data={'generated_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'python':sys.version,'platform':platform.platform(),'cpu_count':os.cpu_count(),'timeout_s':TIMEOUT,'method':'fresh subprocess; 2 warmups + 5 samples; medians; no repository benchmark code','availability':availability(),'results':results}
 (OUT/'independent-results.json').write_text(json.dumps(data,indent=2)+'\n')
 print(json.dumps(data,indent=2))
if __name__=='__main__': main()
