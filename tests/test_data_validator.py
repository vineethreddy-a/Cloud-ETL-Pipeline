"""
Unit tests for data validation module.
"""

import pytest
import pandas as pd
from src.validation.data_validator import DataValidator, ValidationRule, ReconciliationFramework


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        "amount": [100.0, 250.5, 50.0, 999.9, 300.0],
        "email": [
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
            "dave@example.com",
            "eve@example.com",
        ],
    })


@pytest.fixture
def df_with_nulls():
    return pd.DataFrame({
        "id": [1, 2, None, 4, 5],
        "name": ["Alice", None, "Charlie", "Dave", "Eve"],
        "amount": [100.0, 250.5, 50.0, None, 300.0],
    })


@pytest.fixture
def df_with_duplicates():
    return pd.DataFrame({
        "id": [1, 1, 2, 3, 3],
        "name": ["Alice", "Alice", "Bob", "Charlie", "Charlie"],
        "amount": [100.0, 100.0, 250.5, 50.0, 50.0],
    })


class TestDataValidator:

    def test_not_null_passes_on_clean_data(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        result = validator.check_not_null("id")
        assert result.passed is True
        assert result.failed_count == 0
        assert result.pass_rate == 1.0

    def test_not_null_fails_with_nulls(self, df_with_nulls):
        validator = DataValidator(df_with_nulls, "test_dataset")
        result = validator.check_not_null("id")
        assert result.passed is False
        assert result.failed_count == 1

    def test_unique_passes_on_clean_data(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        result = validator.check_unique("id")
        assert result.passed is True
        assert result.failed_count == 0

    def test_unique_fails_with_duplicates(self, df_with_duplicates):
        validator = DataValidator(df_with_duplicates, "test_dataset")
        result = validator.check_unique("id")
        assert result.passed is False
        assert result.failed_count > 0

    def test_range_passes_within_bounds(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        result = validator.check_range("amount", min_val=0, max_val=1000)
        assert result.passed is True

    def test_range_fails_outside_bounds(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        result = validator.check_range("amount", min_val=0, max_val=100)
        assert result.passed is False
        assert result.failed_count > 0

    def test_regex_passes_valid_emails(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        result = validator.check_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
        assert result.passed is True

    def test_run_all_executes_all_rules(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        rules = [
            ValidationRule("r1", "id", "not_null"),
            ValidationRule("r2", "id", "unique"),
            ValidationRule("r3", "amount", "range", {"min": 0, "max": 9999}),
        ]
        results = validator.run_all(rules)
        assert len(results) == 3

    def test_summary_reports_correct_counts(self, sample_df):
        validator = DataValidator(sample_df, "test_dataset")
        rules = [
            ValidationRule("r1", "id", "not_null"),
            ValidationRule("r2", "id", "unique"),
        ]
        validator.run_all(rules)
        summary = validator.summary()
        assert summary["total_rules"] == 2
        assert summary["passed"] == 2
        assert summary["failed"] == 0
        assert summary["pass_rate"] == 1.0


class TestReconciliationFramework:

    def test_record_count_match(self, sample_df):
        recon = ReconciliationFramework(sample_df, sample_df.copy())
        result = recon.record_count_check()
        assert result["match"] is True
        assert result["difference"] == 0

    def test_record_count_mismatch(self, sample_df):
        target = sample_df.head(3)
        recon = ReconciliationFramework(sample_df, target)
        result = recon.record_count_check()
        assert result["match"] is False
        assert result["difference"] == 2

    def test_aggregate_check_passes(self, sample_df):
        recon = ReconciliationFramework(sample_df, sample_df.copy())
        result = recon.aggregate_check("amount")
        assert result["match"] is True

    def test_full_reconciliation(self, sample_df):
        recon = ReconciliationFramework(sample_df, sample_df.copy())
        result = recon.run_reconciliation(["amount"])
        assert result["overall_status"] == "PASS"
