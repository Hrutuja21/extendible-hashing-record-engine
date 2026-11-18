"""Persistent extendible-hashing record store (custom JSON snapshot + write-ahead log)."""
from __future__ import annotations
import hashlib, json, os, shutil, tempfile, threading, time, uuid, zipfile
from pathlib import Path
from datetime import datetime, timezone

class StorageError(Exception): pass
class DuplicateKeyError(StorageError): pass
class MissingKeyError(StorageError): pass

class ExtendibleHashStore:
    def __init__(self, data_dir='data', bucket_capacity=4, max_depth=24):
        self.data_dir=Path(data_dir); self.data_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_path=self.data_dir/'storage.json'; self.wal_path=self.data_dir/'operations.jsonl'
        self.backup_dir=self.data_dir/'backups'; self.backup_dir.mkdir(exist_ok=True)
        self.capacity=int(bucket_capacity); self.max_depth=int(max_depth); self.lock=threading.RLock()
        self._new_state(); self.recover()

    def _new_state(self):
        self.global_depth=0; self.directory=['B00']; self.buckets={'B00':{'local_depth':0,'prefix':0,'records':[],'overflow':[]}}
        self.next_bucket=1; self.records_total=0
        self.stats={'splits':0,'directory_expansions':0,'merges':0,'search_success':0,'search_failure':0,'insert_success':0,'insert_failure':0,'update_success':0,'delete_success':0}
        self.sequence=0; self.created_at=self._now(); self.last_recovery='Fresh store initialized'

    @staticmethod
    def _now(): return datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds')
    @staticmethod
    def _hash(key): return int.from_bytes(hashlib.sha256(str(key).encode('utf-8')).digest(),'big')
    def _index(self,key,depth=None):
        d=self.global_depth if depth is None else depth
        return 0 if d==0 else self._hash(key) >> (256-d)
    def _bucket_id(self,key): return self.directory[self._index(key)]
    def _new_bucket(self,depth,prefix):
        bid=f'B{self.next_bucket:02d}'; self.next_bucket+=1
        self.buckets[bid]={'local_depth':depth,'prefix':prefix,'records':[],'overflow':[]}; return bid
    def _all_records(self,b): return b['records']+b.get('overflow',[])

    def _split(self,bid):
        b=self.buckets[bid]; old_depth=b['local_depth']
        if old_depth >= self.max_depth:
            return False
        old_global=self.global_depth
        if old_depth == self.global_depth:
            self.directory=[x for x in self.directory for _ in (0,1)]
            self.global_depth+=1; self.stats['directory_expansions']+=1
        new_depth=old_depth+1
        old_prefix=b['prefix']; new_id=self._new_bucket(new_depth,old_prefix | (1 << (new_depth-1)))
        b['local_depth']=new_depth
        for i,pointer in enumerate(self.directory):
            if pointer==bid and ((i >> (self.global_depth-new_depth)) & 1): self.directory[i]=new_id
        all_records=self._all_records(b); b['records']=[]; b['overflow']=[]
        for rec in all_records:
            target=self._bucket_id(rec['key']); dest=self.buckets[target]
            if len(dest['records']) < self.capacity: dest['records'].append(rec)
            else: dest['overflow'].append(rec)
        self.stats['splits']+=1
        return True

    def _place(self,rec):
        while True:
            bid=self._bucket_id(rec['key']); b=self.buckets[bid]
            if len(b['records']) < self.capacity and not b['overflow']:
                b['records'].append(rec); return
            if not self._split(bid):
                b['overflow'].append(rec); return
            # If a pathological hash prefix remains full, keep splitting until max depth.

    def _state(self):
        return {'format_version':1,'bucket_capacity':self.capacity,'max_depth':self.max_depth,'global_depth':self.global_depth,
          'directory':self.directory,'buckets':self.buckets,'next_bucket':self.next_bucket,'records_total':self.records_total,
          'stats':self.stats,'sequence':self.sequence,'created_at':self.created_at,'saved_at':self._now()}
    def _load_state(self,s):
        self.capacity=s['bucket_capacity']; self.max_depth=s.get('max_depth',24); self.global_depth=s['global_depth']; self.directory=s['directory']
        self.buckets=s['buckets']; self.next_bucket=s['next_bucket']; self.records_total=s['records_total']; self.stats=s['stats']; self.sequence=s.get('sequence',0); self.created_at=s.get('created_at',self._now())
    def _commit(self,operation,detail):
        self.sequence+=1; state=self._state(); event={'sequence':self.sequence,'timestamp':self._now(),'operation':operation,'detail':detail,'state':state}
        with self.wal_path.open('a',encoding='utf-8') as f: f.write(json.dumps(event,ensure_ascii=False)+'\n'); f.flush(); os.fsync(f.fileno())
        fd,tmp=tempfile.mkstemp(prefix='storage-',suffix='.tmp',dir=self.data_dir)
        try:
            with os.fdopen(fd,'w',encoding='utf-8') as f: json.dump(state,f,ensure_ascii=False,indent=2); f.flush(); os.fsync(f.fileno())
            os.replace(tmp,self.snapshot_path)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)

    def recover(self):
        with self.lock:
            snapshot=None
            if self.snapshot_path.exists():
                try: snapshot=json.loads(self.snapshot_path.read_text(encoding='utf-8'))
                except Exception: snapshot=None
            if snapshot: self._load_state(snapshot)
            recovered=False
            if self.wal_path.exists():
                for line in self.wal_path.read_text(encoding='utf-8').splitlines():
                    try: event=json.loads(line)
                    except Exception: continue
                    if event.get('sequence',0)>self.sequence and event.get('state'):
                        self._load_state(event['state']); recovered=True
            if recovered or not self.snapshot_path.exists() or snapshot is None:
                self._write_snapshot_only()
            self.last_recovery='Recovered latest committed state from operation log' if recovered else 'Snapshot loaded successfully' if snapshot else 'Initialized new persistent store'
            return {'recovered_from_log':recovered,'message':self.last_recovery,'sequence':self.sequence}
    def _write_snapshot_only(self):
        tmp=self.snapshot_path.with_suffix('.tmp'); tmp.write_text(json.dumps(self._state(),ensure_ascii=False,indent=2),encoding='utf-8'); os.replace(tmp,self.snapshot_path)

    def insert(self,record):
        with self.lock:
            key=str(record.get('key','')).strip()
            if not key: raise StorageError('Key is required')
            if self.search(key,track=False) is not None:
                self.stats['insert_failure']+=1; self._commit('INSERT_REJECTED',{'key':key,'reason':'duplicate key'}); raise DuplicateKeyError(f'Duplicate key: {key}')
            now=self._now(); rec={'key':key,'name':record.get('name',''),'category':record.get('category',''),'description':record.get('description',''),'value':record.get('value',''),'created_date':record.get('created_date') or now,'updated_date':now}
            initial_bucket=self._bucket_id(key); before_d=self.global_depth; before_s=self.stats['splits']; before_e=self.stats['directory_expansions']; self._place(rec); self.records_total+=1; self.stats['insert_success']+=1
            self._commit('INSERT',{'key':key,'bucket':self._bucket_id(key),'global_depth_before':before_d,'global_depth_after':self.global_depth})
            return {'record':rec,'bucket':self._bucket_id(key),'split':self.stats['splits']>before_s,'split_bucket':initial_bucket,'directory_expanded':self.stats['directory_expansions']>before_e,'depth_before':before_d,'depth_after':self.global_depth}

    def search(self,key,track=True):
        with self.lock:
            key=str(key); bid=self._bucket_id(key); found=next((r for r in self._all_records(self.buckets[bid]) if r['key']==key),None)
            if track:
                self.stats['search_success' if found else 'search_failure']+=1; self._commit('SEARCH',{'key':key,'found':bool(found),'bucket':bid})
            return dict(found) if found else None
    def update(self,key,changes):
        with self.lock:
            key=str(key); bid=self._bucket_id(key); b=self.buckets[bid]
            for r in self._all_records(b):
                if r['key']==key:
                    for field in ('name','category','description','value'):
                        if field in changes: r[field]=changes[field]
                    r['updated_date']=self._now(); self.stats['update_success']+=1; self._commit('UPDATE',{'key':key,'bucket':bid}); return dict(r)
            raise MissingKeyError(f'Record not found: {key}')

    def delete(self,key):
        with self.lock:
            key=str(key); bid=self._bucket_id(key); b=self.buckets[bid]; removed=None
            for field in ('records','overflow'):
                for i,r in enumerate(b[field]):
                    if r['key']==key: removed=b[field].pop(i); break
                if removed: break
            if not removed: raise MissingKeyError(f'Record not found: {key}')
            self.records_total-=1; self.stats['delete_success']+=1; self._try_merge(bid); self._commit('DELETE',{'key':key,'bucket':bid}); return removed

    def _try_merge(self,bid):
        while bid in self.buckets:
            b=self.buckets[bid]; ld=b['local_depth']
            if ld<=0 or b['overflow']: break
            buddy_prefix=b['prefix'] ^ 1; buddy_id=None
            for x,v in self.buckets.items():
                if x!=bid and v['local_depth']==ld and v['prefix']==buddy_prefix: buddy_id=x; break
            if not buddy_id: break
            buddy=self.buckets[buddy_id]
            if buddy.get('overflow') or len(b['records'])+len(buddy['records'])>self.capacity: break
            keep_id,drop_id=(bid,buddy_id) if b['prefix']%2==0 else (buddy_id,bid)
            keep=self.buckets[keep_id]; drop=self.buckets[drop_id]
            keep['records'].extend(drop['records']); keep['local_depth']=ld-1; keep['prefix'] >>= 1
            self.directory=[keep_id if x==drop_id else x for x in self.directory]; del self.buckets[drop_id]
            self.stats['merges']+=1; bid=keep_id
        while self.global_depth>0 and all(v['local_depth']<self.global_depth for v in self.buckets.values()):
            self.directory=self.directory[::2]; self.global_depth-=1

    def integrity_check(self):
        issues=[]; seen={}; expected=1<<self.global_depth
        if len(self.directory)!=expected: issues.append(f'Directory length {len(self.directory)} does not equal 2^{self.global_depth}')
        for i,bid in enumerate(self.directory):
            if bid not in self.buckets: issues.append(f'Invalid directory pointer at entry {i}: {bid}')
        for bid,b in self.buckets.items():
            ld=b['local_depth']; prefix=b['prefix']
            if ld>self.global_depth: issues.append(f'{bid}: local depth exceeds global depth')
            refs=[i for i,x in enumerate(self.directory) if x==bid]
            if len(refs)!=(1<<(self.global_depth-ld)): issues.append(f'{bid}: invalid pointer count {len(refs)} for local depth {ld}')
            for r in self._all_records(b):
                if r['key'] in seen: issues.append(f'Duplicate key detected: {r["key"]}')
                seen[r['key']]=bid
                if self._bucket_id(r['key'])!=bid: issues.append(f'Misplaced record {r["key"]}: stored in {bid}, routes to {self._bucket_id(r["key"])}')
            if len(b['records'])>self.capacity: issues.append(f'{bid}: primary bucket exceeds capacity')
        if len(seen)!=self.records_total: issues.append(f'Record count mismatch: metadata={self.records_total}, actual={len(seen)}')
        return {'ok':not issues,'issues':issues,'checked_records':len(seen),'checked_buckets':len(self.buckets),'checked_directory_entries':len(self.directory),'checked_at':self._now()}

    def stats_view(self):
        return {'global_depth':self.global_depth,'directory_entries':len(self.directory),'bucket_count':len(self.buckets),'total_records':self.records_total,'bucket_capacity':self.capacity,'bucket_splits':self.stats['splits'],'directory_expansions':self.stats['directory_expansions'],'merges':self.stats['merges'],'successful_searches':self.stats['search_success'],'unsuccessful_searches':self.stats['search_failure'],'storage_status':'Healthy' if self.integrity_check()['ok'] else 'Integrity warning','utilization':round(self.records_total/(len(self.buckets)*self.capacity),4) if self.buckets else 0,'sequence':self.sequence,'last_recovery':self.last_recovery}
    def directory_view(self):
        return [{'entry':format(i,f'0{self.global_depth}b') if self.global_depth else 'ε','index':i,'bucket_id':bid,'local_depth':self.buckets[bid]['local_depth']} for i,bid in enumerate(self.directory)]
    def bucket_view(self):
        return [{'bucket_id':bid,'local_depth':b['local_depth'],'prefix':format(b['prefix'],f'0{b["local_depth"]}b') if b['local_depth'] else 'ε','record_count':len(self._all_records(b)),'primary_count':len(b['records']),'overflow_count':len(b['overflow']),'records':self._all_records(b)} for bid,b in sorted(self.buckets.items())]
    def logs(self,limit=200):
        if not self.wal_path.exists(): return []
        out=[]
        for line in self.wal_path.read_text(encoding='utf-8').splitlines()[-limit:]:
            try:
                e=json.loads(line); out.append({k:e.get(k) for k in ('sequence','timestamp','operation','detail')})
            except Exception: pass
        return list(reversed(out))
    def create_backup(self):
        stamp=datetime.now().strftime('%Y%m%d-%H%M%S'); path=self.backup_dir/f'backup-{stamp}.zip'
        with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
            if self.snapshot_path.exists(): z.write(self.snapshot_path,'storage.json')
            if self.wal_path.exists(): z.write(self.wal_path,'operations.jsonl')
        return {'filename':path.name,'path':str(path),'size_bytes':path.stat().st_size}
    def restore_backup(self,filename):
        if Path(filename).name!=filename: raise StorageError('Invalid backup filename')
        path=self.backup_dir/filename
        if not path.exists(): raise StorageError('Backup not found')
        with zipfile.ZipFile(path) as z:
            if 'storage.json' not in z.namelist(): raise StorageError('Backup has no storage snapshot')
            state=json.loads(z.read('storage.json'))
            self._load_state(state); self._write_snapshot_only()
            if 'operations.jsonl' in z.namelist(): self.wal_path.write_bytes(z.read('operations.jsonl'))
        self._commit('RESTORE',{'filename':filename}); return self.stats_view()
    def backup_list(self):
        return [{'filename':p.name,'size_bytes':p.stat().st_size,'modified':datetime.fromtimestamp(p.stat().st_mtime).astimezone().isoformat(timespec='seconds')} for p in sorted(self.backup_dir.glob('*.zip'),reverse=True)]
    def load_demo(self):
        # Sequential-looking keys still hash through SHA-256; bucket capacity 4 makes splits observable.
        names=['Aarav Sharma','Anaya Patil','Vihaan Kulkarni','Ira Deshmukh','Aditya Joshi','Sara Khan','Reyansh More','Myra Shah','Arjun Pawar','Kiara Nair','Atharv Jadhav','Riya Mehta','Kabir Chavan','Aadhya Rao','Ishaan Sane','Tara Gokhale','Omkar Bhosale','Saanvi Iyer','Rudra Naik','Diya Kapoor','Neil Shinde','Mira Kulkarni','Dev Malhotra','Avni Patil']
        inserted=[]; skipped=[]
        for i,name in enumerate(names,1):
            key=f'STU-{i:03d}'
            try: inserted.append(self.insert({'key':key,'name':name,'category':['Computer','AI & ML','Data Science','Networks'][i%4],'description':f'Fictional demonstration student record {i}.','value':f'CGPA: {6.5+(i%35)/10:.1f}'}))
            except DuplicateKeyError: skipped.append(key)
        # Demonstrate successful search, miss, update and deletion, retaining 23 records.
        self.search('STU-003'); self.search('STU-999')
        try: self.update('STU-004',{'description':'Updated during the demo workflow.'})
        except MissingKeyError: pass
        try: self.delete('STU-024')
        except MissingKeyError: pass
        return {'inserted':len(inserted),'skipped':skipped,'final_records':self.records_total,'depth':self.global_depth,'splits':self.stats['splits'],'expansions':self.stats['directory_expansions'],'demo_searches':2,'demo_update':'STU-004','demo_delete':'STU-024'}
