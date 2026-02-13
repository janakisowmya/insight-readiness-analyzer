# Insight Readiness Analyzer (IRA) 🛡️

**A Data Quality Firewall for Modern Analytics.**

IRA is an automated, policy-driven data cleaning engine designed to transform messy, raw datasets into analytics-ready assets with zero manual effort. Unlike traditional tools that just *validate* data, IRA actively *corrects* it based on your rules, while maintaining a forensic audit trail of every change.

## 🚀 Key Features

- **Policy-As-Code**: Define your cleaning logic in simple YAML (e.g., "Standardize dates to ISO-8601", "Impute missing values with mean").
- **Auto-Correction**: Automatically fixes common issues like:
  - Mixed date formats (`Feb 20th 2024` → `2024-02-20`)
  - Currency symbols & text noise (`$1,234.56` → `1234.56`)
  - Unicode/Encoding artifacts (`\u200b`, NBSP, BOM)
- **Forensic Audit Log**: Generates a line-by-line JSONL audit log of *every single change* made to the data. Prove compliance effortlessly.
- **Validity Rules Engine**: Enforce business logic constraints:
  - Numeric ranges (`salary > 0`)
  - Regex patterns (`email` validation)
  - Allowed sets (`status in [Active, Inactive]`)
- **Readiness Score**: Get a discrete score (0-100) representing how ready your data is for machine learning or BI.

## 📦 Installation

```bash
git clone https://github.com/janakisowmya/insight-readiness-analyzer.git
cd insight-readiness-analyzer
pip install -e .
```

## ⚡ Quick Start

### 1. Auto-Infer a Policy
Don't know where to start? Let IRA look at your data and guess the rules.
```bash
ira infer --input data/raw_sales.csv --out policy.yaml
```

### 2. Run the Correction Pipeline
Apply the policy to clean the data and generate an audit log.
```bash
ira correct \
  --input data/raw_sales.csv \
  --policy policy.yaml \
  --out data/clean_sales.csv \
  --audit audit.jsonl
```

## 🛠️ Architecture

IRA is built as a modular pipeline:
1.  **Standardization**: Global text cleanup (encoding, whitespace).
2.  **Parsing**: Robust type inference and conversion (handling edge cases like `1,234.00-`).
3.  **Validity**: Business rule enforcement (Flag/Null/Drop invalid rows).
4.  **Imputation**: Intelligent filling of missing values based on column type.

## License

MIT License.
