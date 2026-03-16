import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
import pandas as pd
from validation.data_validator import DataValidator, ValidationRule, ReconciliationFramework

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        "amount": [100.0, 250.5, 50.0, 999.9, 300.0],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "dave@example.com", "eve@example.com"],
    })

@pytest.fixture
def df_with_nulls():
    return pd.DataFrame({"id": [1, 2, None, 4, 5], "name": ["Alice", None, "Charlie", "Dave", "Eve"], "amount": [100.0, 250.5, 50.0, None, 300.0]})

@pytest.fixture
def df_with_duplicates():
    return pd.DataFrame({"id": [1, 1, 2, 3, 3], "name": ["Alice", "Alice", "Bob", "Charlie", "Charlie"], "amount": [100.0, 100.0, 250.5, 50.0, 50.0]})

class TestDataValidator:
    def test_not_null_passes(self, sample_df):
        r = DataValidator(sample_df, "t").check_not_null("id")
        assert r.passed and r.failed_count == 0

    def test_not_null_fails(self, df_with_nulls):
        r = DataValidator(df_with_nulls, "t").check_not_null("id")
        assert not r.passed and r.failed_count == 1

    def test_unique_passes(self, sample_df):
        r = DataValidator(sample_df, "t").check_unique("id")
        assert r.passed and r.failed_count == 0

    def test_unique_fails(self, df_with_duplicates):
        r = DataValidator(df_with_duplicates, "t").check_unique("id")
        assert not r.passed

    def test_range_passes(self, sample_df):
        r = DataValidator(sample_df, "t").check_range("amount", 0, 1000)
        assert r.passed

    def test_range_fails(self, sample_df):
        r = DataValidator(sample_df, "t").check_range("amount", 0, 100)
        assert not r.passed

    def test_regex_passes(self, sample_df):
        r = DataValidator(sample_df, "t").check_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
        assert r.passed

    def test_run_all(self, sample_df):
        rules = [ValidationRule("r1","id","not_null"), ValidationRule("r2","id","unique"), ValidationRule("r3","amount","range",{"min":0,"max":9999})]
        assert len(DataValidator(sample_df, "t").run_all(rules)) == 3

    def test_summary(self, sample_df):
        v = DataValidator(sample_df, "t")
        v.run_all([ValidationRule("r1","id","not_null"), ValidationRule("r2","id","unique")])
        s = v.summary()
        assert s["passed"] == 2 and s["failed"] == 0

class TestReconciliation:
    def test_count_match(self, sample_df):
        assert ReconciliationFramework(sample_df, sample_df.copy()).record_count_check()["match"]

    def test_count_mismatch(self, sample_df):
        assert not ReconciliationFramework(sample_df, sample_df.head(3)).record_count_check()["match"]

    def test_aggregate_passes(self, sample_df):
        assert ReconciliationFramework(sample_df, sample_df.copy()).aggregate_check("amount")["match"]

    def test_full_recon(self, sample_df):
        assert ReconciliationFramework(sample_df, sample_df.copy()).run_reconciliation(["amount"])["overall_status"] == "PASS"
