import tempfile
import unittest
from pathlib import Path
from tme.database import Database
from tme.executor_lease import ExecutorLease

class ExecutorLeaseTests(unittest.TestCase):
    def test_single_owner_and_release(self):
        tmp=tempfile.TemporaryDirectory()
        db=Database(str(Path(tmp.name)/"tme.db"))
        lease=ExecutorLease(db)
        self.assertTrue(lease.acquire("primary","0xabc",45000)["acquired"])
        self.assertFalse(lease.acquire("clone","0xabc",45000)["acquired"])
        self.assertTrue(lease.renew("primary","0xabc",45000)["acquired"])
        self.assertFalse(lease.renew("clone","0xabc",45000)["acquired"])
        self.assertTrue(lease.release("primary"))
        self.assertTrue(lease.acquire("clone","0xabc",45000)["acquired"])
        db.close();tmp.cleanup()

if __name__=="__main__":
    unittest.main()
