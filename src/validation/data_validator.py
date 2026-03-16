"""
Data Validation & Reconciliation Module
Achieves 99%+ data accuracy through automated quality checks,
source-to-target reconciliation, and alerting.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import boto3

from utils.logger import get_logger
from utils.config import load_config

logger = get_logger(__name__)


@dataclass
class ValidationRule:
    """Defines a single validation rule."""
    name: str
    column: Optional[str]
    rule_type: str  # not_null, unique, range, regex, referential, custom
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = "ERROR"  # ERROR, WARNING


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    pass_rate: float
    severity: str
    details: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class DataValidator:
    """
    Runs a suite of data quality validations on a DataFrame.
    Supports null checks, uniqueness, range validation, regex, and referential integrity.
    """

    def __init__(self, df: pd.DataFrame, dataset_name: str):
        self.df = df
        self.dataset_name = dataset_name
        self.results: List[ValidationResult] = []

    def check_not_null(self, column: str, severity: str = "ERROR") -> ValidationResult:
        """Validate that a column has no null values."""
        total = len(self.df)
        null_count = self.df[column].isnull().sum()
        passed = null_count == 0
        pass_rate = (total - null_count) / total if total > 0 else 1.0

        result = ValidationResult(
            rule_name=f"not_null_{column}",
            passed=passed,
            failed_count=int(null_count),
            total_count=total,
            pass_rate=round(pass_rate, 4),
            severity=severity,
            details=f"{null_count} null values found in '{column}'"
        )
        self._log_result(result)
        return result

    def check_unique(self, column: str, severity: str = "ERROR") -> ValidationResult:
        """Validate that a column has no duplicate values."""
        total = len(self.df)
        dup_count = total - self.df[column].nunique()
        passed = dup_count == 0
        pass_rate = (total - dup_count) / total if total > 0 else 1.0

        result = ValidationResult(
            rule_name=f"unique_{column}",
            passed=passed,
            failed_count=int(dup_count),
            total_count=total,
            pass_rate=round(pass_rate, 4),
            severity=severity,
            details=f"{dup_count} duplicate values found in '{column}'"
        )
        self._log_result(result)
        return result

    def check_range(
        self, column: str, min_val: float, max_val: float, severity: str = "ERROR"
    ) -> ValidationResult:
        """Validate that numeric values fall within [min_val, max_val]."""
        total = len(self.df)
        out_of_range = self.df[
            (self.df[column] < min_val) | (self.df[column] > max_val)
        ].shape[0]
        passed = out_of_range == 0
        pass_rate = (total - out_of_range) / total if total > 0 else 1.0

        result = ValidationResult(
            rule_name=f"range_{column}_{min_val}_{max_val}",
            passed=passed,
            failed_count=int(out_of_range),
            total_count=total,
            pass_rate=round(pass_rate, 4),
            severity=severity,
            details=f"{out_of_range} values outside [{min_val}, {max_val}] in '{column}'"
        )
        self._log_result(result)
        return result

    def check_regex(self, column: str, pattern: str, severity: str = "WARNING") -> ValidationResult:
        """Validate that string values match a regex pattern."""
        total = len(self.df)
        non_matching = (~self.df[column].astype(str).str.match(pattern)).sum()
        passed = non_matching == 0
        pass_rate = (total - non_matching) / total if total > 0 else 1.0

        result = ValidationResult(
            rule_name=f"regex_{column}",
            passed=passed,
            failed_count=int(non_matching),
            total_count=total,
            pass_rate=round(pass_rate, 4),
            severity=severity,
            details=f"{non_matching} values in '{column}' don't match pattern '{pattern}'"
        )
        self._log_result(result)
        return result

    def check_referential_integrity(
        self, column: str, ref_df: pd.DataFrame, ref_column: str, severity: str = "ERROR"
    ) -> ValidationResult:
        """Validate foreign key relationships between datasets."""
        total = len(self.df)
        valid_ids = set(ref_df[ref_column].dropna().unique())
        invalid_count = (~self.df[column].isin(valid_ids)).sum()
        passed = invalid_count == 0
        pass_rate = (total - invalid_count) / total if total > 0 else 1.0

        result = ValidationResult(
            rule_name=f"referential_{column}",
            passed=passed,
            failed_count=int(invalid_count),
            total_count=total,
            pass_rate=round(pass_rate, 4),
            severity=severity,
            details=f"{invalid_count} values in '{column}' not found in reference dataset"
        )
        self._log_result(result)
        return result

    def run_all(self, rules: List[ValidationRule]) -> List[ValidationResult]:
        """Execute all validation rules and collect results."""
        for rule in rules:
            if rule.rule_type == "not_null":
                self.results.append(self.check_not_null(rule.column, rule.severity))
            elif rule.rule_type == "unique":
                self.results.append(self.check_unique(rule.column, rule.severity))
            elif rule.rule_type == "range":
                self.results.append(
                    self.check_range(rule.column, rule.params["min"], rule.params["max"], rule.severity)
                )
            elif rule.rule_type == "regex":
                self.results.append(
                    self.check_regex(rule.column, rule.params["pattern"], rule.severity)
                )
        return self.results

    def summary(self) -> dict:
        """Return a summary of all validation results."""
        total_rules = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        errors = [r for r in self.results if not r.passed and r.severity == "ERROR"]

        return {
            "dataset": self.dataset_name,
            "timestamp": datetime.utcnow().isoformat(),
            "total_rules": total_rules,
            "passed": passed,
            "failed": total_rules - passed,
            "pass_rate": round(passed / total_rules, 4) if total_rules > 0 else 1.0,
            "critical_errors": len(errors),
            "results": [vars(r) for r in self.results],
        }

    def _log_result(self, result: ValidationResult):
        if result.passed:
            logger.info(f"✅ PASS | {result.rule_name} | {result.pass_rate:.1%}")
        else:
            level = logger.error if result.severity == "ERROR" else logger.warning
            level(f"{'❌' if result.severity == 'ERROR' else '⚠️'} {result.severity} | {result.rule_name} | {result.details}")


class ReconciliationFramework:
    """
    Source-to-target reconciliation to ensure data completeness and accuracy.
    Compares record counts, checksums, and aggregate totals.
    """

    def __init__(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        self.source = source_df
        self.target = target_df

    def record_count_check(self) -> dict:
        src_count = len(self.source)
        tgt_count = len(self.target)
        match = src_count == tgt_count
        return {
            "check": "record_count",
            "source_count": src_count,
            "target_count": tgt_count,
            "match": match,
            "difference": abs(src_count - tgt_count),
        }

    def aggregate_check(self, column: str) -> dict:
        src_sum = self.source[column].sum()
        tgt_sum = self.target[column].sum()
        diff_pct = abs(src_sum - tgt_sum) / src_sum if src_sum != 0 else 0
        return {
            "check": f"aggregate_sum_{column}",
            "source_sum": float(src_sum),
            "target_sum": float(tgt_sum),
            "difference_pct": round(diff_pct * 100, 4),
            "match": diff_pct < 0.001,  # 0.1% tolerance
        }

    def run_reconciliation(self, numeric_columns: List[str]) -> dict:
        results = [self.record_count_check()]
        for col in numeric_columns:
            results.append(self.aggregate_check(col))

        all_passed = all(r["match"] for r in results)
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "PASS" if all_passed else "FAIL",
            "checks": results,
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Data Validation")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--path", required=True, help="Path to parquet file")
    args = parser.parse_args()

    config = load_config(args.env)
    df = pd.read_parquet(args.path)

    # Example rules — customize per dataset
    rules = [
        ValidationRule("check_id_not_null", "id", "not_null"),
        ValidationRule("check_id_unique", "id", "unique"),
        ValidationRule("check_amount_range", "amount", "range", {"min": 0, "max": 1_000_000}),
    ]

    validator = DataValidator(df, args.dataset)
    validator.run_all(rules)
    summary = validator.summary()

    print(json.dumps(summary, indent=2))

    if summary["critical_errors"] > 0:
        logger.error("Validation failed with critical errors. Halting pipeline.")
        exit(1)


if __name__ == "__main__":
    main()
