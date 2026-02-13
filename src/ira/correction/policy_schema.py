from __future__ import annotations

from typing import Any, Literal, Optional, Dict, List, Tuple
from pathlib import Path
import hashlib
import json
import yaml

from pydantic import BaseModel, Field, ValidationError, model_validator


def _dedupe_list(xs: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in xs:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _normalize_colname(name: str) -> str:
    return " ".join(str(name).strip().split())


def _hash_policy_dict(d: Dict[str, Any]) -> str:
    payload = json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


PercentScale = Literal["auto", "0_1", "0_100"]
OnFailure = Literal["null", "drop_row", "keep_raw"]
Casefold = Literal["none", "lower", "upper", "title"]
DedupeStrategy = Literal["keep_first", "keep_last", "keep_max", "keep_min"]
ExactDupes = Literal["drop", "keep"]
ImputeNumeric = Literal["none", "mean", "median", "constant"]
ImputeCat = Literal["none", "mode", "constant"]
ImputeDatetime = Literal["none", "constant"]
ValidityAction = Literal["flag", "null", "drop_row"]
OutlierMethod = Literal["iqr", "zscore"]
OutlierAction = Literal["flag", "cap", "drop_row"]
OutputFormat = Literal["parquet", "csv"]
AuditDetail = Literal["summary", "detailed"]

# Valid column types for parsing.column_types
ColumnType = Literal[
    "boolean",
    "numeric",
    "integer",
    "float",
    "datetime",
    "date",
    "timestamp",
]
VALID_COLUMN_TYPES = {"boolean", "numeric", "integer", "float", "datetime", "date", "timestamp"}


class DatasetSection(BaseModel):
    name: str = "unknown"
    description: str = ""
    expected_granularity: str = ""

    primary_key: Dict[str, Any] = Field(
        default_factory=lambda: {"columns": [], "mode": "infer_or_use"}
    )
    time_column: Dict[str, Any] = Field(
        default_factory=lambda: {"column": None, "mode": "infer_or_use"}
    )

    @model_validator(mode="after")
    def _validate_dataset(self) -> "DatasetSection":
        pk = self.primary_key or {}
        tc = self.time_column or {}

        pk_cols = pk.get("columns", [])
        pk_mode = pk.get("mode", "infer_or_use")
        if pk_mode not in ("infer_or_use", "use_only"):
            raise ValueError("dataset.primary_key.mode must be infer_or_use or use_only")
        if not isinstance(pk_cols, list):
            raise ValueError("dataset.primary_key.columns must be a list")

        time_col = tc.get("column", None)
        time_mode = tc.get("mode", "infer_or_use")
        if time_mode not in ("infer_or_use", "use_only", "none"):
            raise ValueError("dataset.time_column.mode must be infer_or_use/use_only/none")
        if time_mode == "use_only" and not time_col:
            raise ValueError("dataset.time_column.column required when mode=use_only")

        return self


class RolesSection(BaseModel):
    critical_columns: List[str] = Field(default_factory=list)
    protected_columns: List[str] = Field(default_factory=list)
    standardize_columns: List[str] = Field(default_factory=list)
    fillable_columns: List[str] = Field(default_factory=list)
    droppable_columns: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize_and_validate_roles(self) -> "RolesSection":
        self.critical_columns = _dedupe_list([_normalize_colname(c) for c in self.critical_columns])
        self.protected_columns = _dedupe_list([_normalize_colname(c) for c in self.protected_columns])
        self.standardize_columns = _dedupe_list([_normalize_colname(c) for c in self.standardize_columns])
        self.fillable_columns = _dedupe_list([_normalize_colname(c) for c in self.fillable_columns])
        self.droppable_columns = _dedupe_list([_normalize_colname(c) for c in self.droppable_columns])

        overlap_std = set(self.protected_columns) & set(self.standardize_columns)
        if overlap_std:
            raise ValueError(f"Protected columns cannot be standardized: {sorted(overlap_std)}")

        overlap_fill = set(self.protected_columns) & set(self.fillable_columns)
        if overlap_fill:
            raise ValueError(f"Protected columns cannot be imputed: {sorted(overlap_fill)}")

        return self


class ParsingDatetime(BaseModel):
    dayfirst: bool = False
    yearfirst: bool = False
    allowed_formats: List[str] = Field(default_factory=list)
    on_failure: OnFailure = "null"


class ParsingNumeric(BaseModel):
    allow_commas: bool = True
    allow_currency_symbols: bool = True
    allow_percent_symbol: bool = True
    percent_scale: PercentScale = "auto"
    on_failure: OnFailure = "null"


class ParsingBoolean(BaseModel):
    true_values: List[str] = Field(default_factory=lambda: ["true", "t", "yes", "y", "1"])
    false_values: List[str] = Field(default_factory=lambda: ["false", "f", "no", "n", "0"])
    on_failure: OnFailure = "null"


class ParsingSection(BaseModel):
    infer_types: bool = True
    column_types: Dict[str, str] = Field(default_factory=dict)

    datetime: ParsingDatetime = Field(default_factory=ParsingDatetime)
    numeric: ParsingNumeric = Field(default_factory=ParsingNumeric)
    boolean: ParsingBoolean = Field(default_factory=ParsingBoolean)

    # Allow custom currency symbols that are not standard
    currency_symbols: List[str] = Field(default_factory=list)
    
    # Per-column boolean configuration
    boolean_columns: Dict[str, ParsingBoolean] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize_and_validate_column_types(self) -> "ParsingSection":
        normalized = {}
        for k, v in self.column_types.items():
            norm_key = _normalize_colname(k)
            if v not in VALID_COLUMN_TYPES:
                raise ValueError(
                    f"Invalid column type '{v}' for column '{k}'. "
                    f"Must be one of: {sorted(VALID_COLUMN_TYPES)}"
                )
            normalized[norm_key] = v
        self.column_types = normalized
        return self


class StandardizationSection(BaseModel):
    global_trim_whitespace: bool = True
    global_collapse_whitespace: bool = True
    casefold: Casefold = "none"
    mappings: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    strip_nonprinting: bool = True

    @model_validator(mode="after")
    def _normalize_mappings(self) -> "StandardizationSection":
        norm: Dict[str, Dict[str, str]] = {}
        for col, mp in (self.mappings or {}).items():
            norm[_normalize_colname(col)] = dict(mp)
        self.mappings = norm
        return self


class DeduplicationSection(BaseModel):
    enabled: bool = True
    keys: List[str] = Field(default_factory=list)
    strategy: DedupeStrategy = "keep_first"
    order_by: Optional[str] = None
    exact_row_duplicates: ExactDupes = "drop"

    @model_validator(mode="after")
    def _normalize(self) -> "DeduplicationSection":
        self.keys = _dedupe_list([_normalize_colname(c) for c in self.keys])
        self.order_by = _normalize_colname(self.order_by) if self.order_by else None

        if self.strategy in ("keep_max", "keep_min") and not self.order_by:
            raise ValueError("deduplication.order_by is required when strategy is keep_max/keep_min")

        return self


class MissingImputationNumeric(BaseModel):
    default: ImputeNumeric = "none"
    constants: Dict[str, float] = Field(default_factory=dict)
    allow_if_missing_pct_leq: float = 0.2


class MissingImputationCategorical(BaseModel):
    default: ImputeCat = "none"
    constants: Dict[str, str] = Field(default_factory=dict)
    allow_if_missing_pct_leq: float = 0.2


class MissingImputationDatetime(BaseModel):
    default: ImputeDatetime = "none"
    constants: Dict[str, str] = Field(default_factory=dict)
    allow_if_missing_pct_leq: float = 0.05


class MissingImputation(BaseModel):
    numeric: MissingImputationNumeric = Field(default_factory=MissingImputationNumeric)
    categorical: MissingImputationCategorical = Field(default_factory=MissingImputationCategorical)
    datetime: MissingImputationDatetime = Field(default_factory=MissingImputationDatetime)

    @model_validator(mode="after")
    def _normalize_constants(self) -> "MissingImputation":
        self.numeric.constants = {_normalize_colname(k): v for k, v in self.numeric.constants.items()}
        self.categorical.constants = {_normalize_colname(k): v for k, v in self.categorical.constants.items()}
        self.datetime.constants = {_normalize_colname(k): v for k, v in self.datetime.constants.items()}
        return self


class MissingDataSection(BaseModel):
    enabled: bool = True
    drop_if_missing_critical: bool = True
    drop_thresholds: Dict[str, Any] = Field(default_factory=lambda: {
        "row_missing_pct_gt": None,
        "col_missing_pct_gt": None,
    })
    imputation: MissingImputation = Field(default_factory=MissingImputation)

    @model_validator(mode="after")
    def _validate_thresholds(self) -> "MissingDataSection":
        r = self.drop_thresholds.get("row_missing_pct_gt", None)
        c = self.drop_thresholds.get("col_missing_pct_gt", None)

        for name, val in (("row_missing_pct_gt", r), ("col_missing_pct_gt", c)):
            if val is not None and not (0.0 <= float(val) <= 1.0):
                raise ValueError(f"missing_data.drop_thresholds.{name} must be between 0 and 1")
        return self


class ValidityRulesSection(BaseModel):
    enabled: bool = True
    ranges: Dict[str, Dict[str, float]] = Field(default_factory=dict)
    allowed_values: Dict[str, List[Any]] = Field(default_factory=dict)
    regex: Dict[str, str] = Field(default_factory=dict)
    non_negative_columns: List[str] = Field(default_factory=list)
    on_violation: ValidityAction = "flag"

    @model_validator(mode="after")
    def _normalize(self) -> "ValidityRulesSection":
        self.ranges = {_normalize_colname(k): v for k, v in self.ranges.items()}
        self.allowed_values = {_normalize_colname(k): v for k, v in self.allowed_values.items()}
        self.regex = {_normalize_colname(k): v for k, v in self.regex.items()}
        self.non_negative_columns = _dedupe_list([_normalize_colname(c) for c in self.non_negative_columns])
        return self


class OutliersSection(BaseModel):
    enabled: bool = True
    method: OutlierMethod = "iqr"
    action: OutlierAction = "flag"
    cap_quantiles: Tuple[float, float] = (0.01, 0.99)
    apply_to_columns: List[str] = Field(default_factory=list)
    exclude_protected: bool = True

    @model_validator(mode="after")
    def _validate(self) -> "OutliersSection":
        lo, hi = self.cap_quantiles
        if not (0.0 <= lo < hi <= 1.0):
            raise ValueError("outliers.cap_quantiles must be (lo, hi) with 0<=lo<hi<=1")

        self.apply_to_columns = _dedupe_list([_normalize_colname(c) for c in self.apply_to_columns])
        return self


class OutputSection(BaseModel):
    format: OutputFormat = "parquet"
    include_audit_log: bool = True
    include_before_after_scores: bool = True
    audit_detail: AuditDetail = "summary"
    save_intermediate: bool = False


class ReproSection(BaseModel):
    random_seed: int = 42
    policy_hash: str = "auto"


class Policy(BaseModel):
    version: str = "1.0"
    dataset: DatasetSection = Field(default_factory=DatasetSection)
    roles: RolesSection = Field(default_factory=RolesSection)
    parsing: ParsingSection = Field(default_factory=ParsingSection)
    standardization: StandardizationSection = Field(default_factory=StandardizationSection)
    deduplication: DeduplicationSection = Field(default_factory=DeduplicationSection)
    missing_data: MissingDataSection = Field(default_factory=MissingDataSection)
    validity_rules: ValidityRulesSection = Field(default_factory=ValidityRulesSection)
    outliers: OutliersSection = Field(default_factory=OutliersSection)
    output: OutputSection = Field(default_factory=OutputSection)
    reproducibility: ReproSection = Field(default_factory=ReproSection)

    @model_validator(mode="after")
    def _cross_section_safety(self) -> "Policy":
        protected = set(self.roles.protected_columns)

        bad_map = protected & set(self.standardization.mappings.keys())
        if bad_map:
            raise ValueError(f"standardization.mappings cannot include protected columns: {sorted(bad_map)}")

        bad_num = protected & set(self.missing_data.imputation.numeric.constants.keys())
        bad_cat = protected & set(self.missing_data.imputation.categorical.constants.keys())
        bad_dt = protected & set(self.missing_data.imputation.datetime.constants.keys())
        bad_any = bad_num | bad_cat | bad_dt
        if bad_any:
            raise ValueError(f"imputation.constants cannot include protected columns: {sorted(bad_any)}")

        if any(not k for k in self.deduplication.keys):
            raise ValueError("deduplication.keys contains an empty column name")

        return self


class PolicyLoadError(Exception):
    pass


def load_policy(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise PolicyLoadError(f"Policy file not found: {p}")

    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise PolicyLoadError("Policy file must parse to a YAML mapping (dictionary).")
    except Exception as e:
        raise PolicyLoadError(f"Failed to read policy YAML: {e}") from e

    try:
        policy_obj = Policy.model_validate(raw)
    except ValidationError as e:
        raise PolicyLoadError(f"Policy validation error:\n{e}") from e
    except Exception as e:
        raise PolicyLoadError(f"Unexpected policy error: {e}") from e

    policy_dict = policy_obj.model_dump()

    if policy_dict.get("reproducibility", {}).get("policy_hash") == "auto":
        temp = json.loads(json.dumps(policy_dict))
        temp["reproducibility"]["policy_hash"] = ""
        policy_dict["reproducibility"]["policy_hash"] = _hash_policy_dict(temp)

    return policy_dict


def validate_against_dataset(
    policy_dict: Dict[str, Any],
    dataset_columns: List[str],
) -> List[str]:
    """
    Validate that policy references only columns present in the dataset.
    Returns a list of warning messages for missing columns.
    
    This prevents silent failures where policy rules are ignored because
    the referenced columns don't exist.
    """
    warnings = []
    norm_dataset_cols = {_normalize_colname(c) for c in dataset_columns}
    
    # Check roles
    roles = policy_dict.get("roles", {})
    for role_name in ["critical_columns", "protected_columns", "standardize_columns", 
                      "fillable_columns", "droppable_columns"]:
        for col in roles.get(role_name, []):
            if col not in norm_dataset_cols:
                warnings.append(f"roles.{role_name}: column '{col}' not found in dataset")
    
    # Check parsing.column_types
    parsing = policy_dict.get("parsing", {})
    for col in parsing.get("column_types", {}).keys():
        if col not in norm_dataset_cols:
            warnings.append(f"parsing.column_types: column '{col}' not found in dataset")
    
    # Check standardization.mappings
    std = policy_dict.get("standardization", {})
    for col in std.get("mappings", {}).keys():
        if col not in norm_dataset_cols:
            warnings.append(f"standardization.mappings: column '{col}' not found in dataset")
    
    # Check deduplication.keys
    dedupe = policy_dict.get("deduplication", {})
    for col in dedupe.get("keys", []):
        if col not in norm_dataset_cols:
            warnings.append(f"deduplication.keys: column '{col}' not found in dataset")
    
    # Check deduplication.order_by
    order_by = dedupe.get("order_by")
    if order_by and order_by not in norm_dataset_cols:
        warnings.append(f"deduplication.order_by: column '{order_by}' not found in dataset")
    
    # Check missing_data.imputation constants
    missing = policy_dict.get("missing_data", {})
    imputation = missing.get("imputation", {})
    for imp_type in ["numeric", "categorical", "datetime"]:
        for col in imputation.get(imp_type, {}).get("constants", {}).keys():
            if col not in norm_dataset_cols:
                warnings.append(f"missing_data.imputation.{imp_type}.constants: column '{col}' not found in dataset")
    
    # Check validity_rules
    validity = policy_dict.get("validity_rules", {})
    for col in validity.get("ranges", {}).keys():
        if col not in norm_dataset_cols:
            warnings.append(f"validity_rules.ranges: column '{col}' not found in dataset")
    for col in validity.get("allowed_values", {}).keys():
        if col not in norm_dataset_cols:
            warnings.append(f"validity_rules.allowed_values: column '{col}' not found in dataset")
    for col in validity.get("regex", {}).keys():
        if col not in norm_dataset_cols:
            warnings.append(f"validity_rules.regex: column '{col}' not found in dataset")
    for col in validity.get("non_negative_columns", []):
        if col not in norm_dataset_cols:
            warnings.append(f"validity_rules.non_negative_columns: column '{col}' not found in dataset")
    
    # Check outliers.apply_to_columns
    outliers = policy_dict.get("outliers", {})
    for col in outliers.get("apply_to_columns", []):
        if col not in norm_dataset_cols:
            warnings.append(f"outliers.apply_to_columns: column '{col}' not found in dataset")
    
    # Check dataset.primary_key.columns
    dataset_section = policy_dict.get("dataset", {})
    pk = dataset_section.get("primary_key", {})
    for col in pk.get("columns", []):
        if col not in norm_dataset_cols:
            warnings.append(f"dataset.primary_key.columns: column '{col}' not found in dataset")
    
    # Check dataset.time_column.column
    tc = dataset_section.get("time_column", {})
    time_col = tc.get("column")
    if time_col and time_col not in norm_dataset_cols:
        warnings.append(f"dataset.time_column.column: column '{time_col}' not found in dataset")
    
    return warnings
