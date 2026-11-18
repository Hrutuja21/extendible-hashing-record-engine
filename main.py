from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pathlib import Path
from engine import ExtendibleHashStore, DuplicateKeyError, MissingKeyError, StorageError

BASE=Path(__file__).resolve().parent
store=ExtendibleHashStore(data_dir=BASE/'data',bucket_capacity=4)
app=FastAPI(title='Persistent Extendible Hashing Record Engine',version='1.0.0')
app.mount('/static',StaticFiles(directory=BASE/'static'),name='static')
class Record(BaseModel):
    key:str; name:str=''; category:str=''; description:str=''; value:str=''; created_date:str|None=None
class Changes(BaseModel): name:str|None=None; category:str|None=None; description:str|None=None; value:str|None=None
class KeyRequest(BaseModel): key:str
class RestoreRequest(BaseModel): filename:str
@app.get('/',response_class=HTMLResponse)
def home(): return (BASE/'templates'/'index.html').read_text(encoding='utf-8')
@app.get('/api/health')
def health(): return {'ok':True,'storage':store.stats_view()}
@app.get('/api/dashboard')
def dashboard(): return {'stats':store.stats_view(),'integrity':store.integrity_check()}
@app.post('/api/records')
def insert(r:Record):
    try:
        result=store.insert(r.model_dump(exclude_none=True)); messages=[]
        if result['split'] and result['directory_expanded']: messages.append(f"Bucket {result['split_bucket']} was full. Directory expanded from depth {result['depth_before']} to depth {result['depth_after']}. Bucket {result['split_bucket']} was split and records were redistributed.")
        elif result['split']: messages.append(f"Bucket {result['split_bucket']} was full. It was split and its records were redistributed without directory expansion.")
        return {'ok':True,'message':f"Record {r.key} inserted successfully. "+' '.join(messages),'result':result}
    except DuplicateKeyError as e: raise HTTPException(409,str(e))
    except StorageError as e: raise HTTPException(400,str(e))
@app.get('/api/records/{key}')
def search(key:str):
    r=store.search(key)
    if not r: raise HTTPException(404,'Record not found')
    return {'ok':True,'record':r,'bucket':store._bucket_id(key)}
@app.put('/api/records/{key}')
def update(key:str,c:Changes):
    try: return {'ok':True,'record':store.update(key,c.model_dump(exclude_none=True)),'message':f'Record {key} updated.'}
    except MissingKeyError as e: raise HTTPException(404,str(e))
@app.delete('/api/records/{key}')
def delete(key:str):
    try: return {'ok':True,'record':store.delete(key),'message':f'Record {key} deleted.'}
    except MissingKeyError as e: raise HTTPException(404,str(e))
@app.get('/api/records')
def records(): return {'records':[r for b in store.bucket_view() for r in b['records']]}
@app.get('/api/directory')
def directory(): return {'global_depth':store.global_depth,'entries':store.directory_view()}
@app.get('/api/buckets')
def buckets(): return {'buckets':store.bucket_view()}
@app.get('/api/statistics')
def statistics(): return store.stats_view()
@app.get('/api/integrity')
def integrity(): return store.integrity_check()
@app.post('/api/demo')
def demo(): return {'ok':True,'message':'Demo workflow completed: inserts, searches, update and delete were executed.','result':store.load_demo()}
@app.post('/api/backup')
def backup(): return {'ok':True,'backup':store.create_backup()}
@app.get('/api/backups')
def backups(): return {'backups':store.backup_list()}
@app.post('/api/restore')
def restore(r:RestoreRequest):
    try: return {'ok':True,'message':f"Restored {r.filename}",'stats':store.restore_backup(r.filename)}
    except StorageError as e: raise HTTPException(400,str(e))
@app.get('/api/logs')
def logs(): return {'logs':store.logs()}
@app.post('/api/recover')
def recover(): return {'ok':True,'result':store.recover()}
@app.get('/api/export')
def export(): return FileResponse(store.snapshot_path,filename='extendible-hashing-storage.json',media_type='application/json')
