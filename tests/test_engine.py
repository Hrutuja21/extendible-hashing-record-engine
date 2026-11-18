import tempfile, unittest, json
from pathlib import Path
from engine import ExtendibleHashStore, DuplicateKeyError, MissingKeyError

class ExtendibleHashStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.store=ExtendibleHashStore(self.root,bucket_capacity=2,max_depth=20)
    def tearDown(self): self.tmp.cleanup()
    def rec(self,k,**kw): return {'key':k,'name':kw.get('name',f'Name {k}'),'category':'Demo','description':'test','value':'v'}
    def test_insert_and_search(self):
        self.store.insert(self.rec('A')); self.assertEqual(self.store.search('A')['name'],'Name A')
    def test_missing_search(self):
        self.assertIsNone(self.store.search('missing')); self.assertEqual(self.store.stats['search_failure'],1)
    def test_duplicate_rejected(self):
        self.store.insert(self.rec('A'))
        with self.assertRaises(DuplicateKeyError): self.store.insert(self.rec('A'))
    def test_update(self):
        self.store.insert(self.rec('A')); r=self.store.update('A',{'name':'Updated'}); self.assertEqual(r['name'],'Updated')
    def test_delete(self):
        self.store.insert(self.rec('A')); self.store.delete('A'); self.assertIsNone(self.store.search('A',track=False))
    def test_bucket_split(self):
        for k in ['a','b','c']: self.store.insert(self.rec(k))
        self.assertGreater(self.store.stats['splits'],0); self.assertGreater(len(self.store.buckets),1)
    def test_directory_doubling(self):
        for i in range(50): self.store.insert(self.rec(f'key-{i}'))
        self.assertGreater(self.store.stats['directory_expansions'],0); self.assertGreater(self.store.global_depth,0)
    def test_record_redistribution(self):
        for i in range(50): self.store.insert(self.rec(f'key-{i}'))
        self.assertTrue(self.store.integrity_check()['ok'],self.store.integrity_check()['issues'])
    def test_persistence_after_restart(self):
        self.store.insert(self.rec('persist')); reopened=ExtendibleHashStore(self.root)
        self.assertIsNotNone(reopened.search('persist',track=False))
    def test_backup_restore(self):
        self.store.insert(self.rec('backup')); b=self.store.create_backup(); self.store.insert(self.rec('later'))
        self.store.restore_backup(Path(b['filename']).name)
        self.assertIsNotNone(self.store.search('backup',track=False)); self.assertIsNone(self.store.search('later',track=False))
    def test_integrity(self):
        for i in range(20): self.store.insert(self.rec(f'k{i}'))
        self.assertTrue(self.store.integrity_check()['ok'])
    def test_recovery_from_log(self):
        self.store.insert(self.rec('wal-only')); self.store.snapshot_path.unlink()
        reopened=ExtendibleHashStore(self.root)
        self.assertIsNotNone(reopened.search('wal-only',track=False)); self.assertIn('Recovered',reopened.last_recovery)

if __name__=='__main__': unittest.main(verbosity=2)
