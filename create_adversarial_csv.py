#!/usr/bin/env python3
"""Generate adversarial_logic.csv — a dataset that attacks pipeline LOGIC, not encoding.

MINDSET: Every value is valid UTF-8, no special bytes. Instead, we attack:
1. Temporal paradoxes — impossible dates, causal violations
2. Cross-column contradictions — values that don't make sense together
3. Statistical landmines — distributions that break imputation math
4. Boundary values — extremes that overflow, underflow, or confuse scoring
5. Schema mimicry — values that look like other data formats
6. Relational chaos — keys that violate assumptions
7. Type ambiguity — values that could be parsed as multiple types
"""
import csv
import math

HEADER = [
    "_row_id", "customer_id", "amount", "signup_date", "is_active",
    "segment", "notes", "discount_pct", "last_login", "region",
    "account_balance", "flag", "email", "phone", "zip", "meta_json"
]

rows = [
    # ──────────────────────────────────────────────────────────
    # GROUP A: TEMPORAL PARADOXES (rows 1-10)
    # ──────────────────────────────────────────────────────────

    # Row 1: last_login BEFORE signup_date — causal violation
    [1, "TEMP-001", "500.00", "2026-06-15", "true", "enterprise",
     "last login is before signup", "0.05", "2025-01-01", "eu",
     "1000.00", "true", "temp1@test.com", "555-0001", "10001", '{"tier":"gold"}'],

    # Row 2: signup_date in the far future — year 9999
    [2, "TEMP-002", "750.00", "9999-12-31", "true", "smb",
     "signup in year 9999", "0.03", "9999-12-31 23:59:59", "apac",
     "2000.00", "true", "temp2@test.com", "555-0002", "10002", '{"tier":"silver"}'],

    # Row 3: Feb 29 in a non-leap year — 2025 is NOT a leap year
    [3, "TEMP-003", "300.00", "2025-02-29", "false", "enterprise",
     "feb 29 in non-leap year 2025", "0.04", "2025-02-28", "eu",
     "500.00", "false", "temp3@test.com", "555-0003", "10003", '{"tier":"bronze"}'],

    # Row 4: Feb 29 in a VALID leap year — 2024 IS a leap year  
    [4, "TEMP-004", "400.00", "2024-02-29", "true", "smb",
     "feb 29 in valid leap year 2024", "0.05", "2024-03-01", "apac",
     "600.00", "true", "temp4@test.com", "555-0004", "10004", '{"tier":"gold"}'],

    # Row 5: Date with month=13
    [5, "TEMP-005", "200.00", "2026-13-01", "false", "enterprise",
     "month 13 does not exist", "0.02", "2026-12-31", "eu",
     "300.00", "false", "temp5@test.com", "555-0005", "10005", '{"tier":"silver"}'],

    # Row 6: Date with day=32
    [6, "TEMP-006", "600.00", "2026-01-32", "true", "smb",
     "day 32 does not exist", "0.06", "2026-01-31", "apac",
     "700.00", "true", "temp6@test.com", "555-0006", "10006", '{"tier":"bronze"}'],

    # Row 7: Epoch 0 — Unix epoch 1970-01-01
    [7, "TEMP-007", "100.00", "1970-01-01", "true", "enterprise",
     "unix epoch zero date", "0.01", "1970-01-01 00:00:00", "eu",
     "150.00", "true", "temp7@test.com", "555-0007", "10007", '{"tier":"gold"}'],

    # Row 8: Date before Unix epoch — 1899
    [8, "TEMP-008", "50.00", "1899-12-31", "false", "smb",
     "date before unix epoch", "0.01", "1899-12-30", "apac",
     "75.00", "false", "temp8@test.com", "555-0008", "10008", '{"tier":"silver"}'],

    # Row 9: Midnight vs end-of-day ambiguity
    [9, "TEMP-009", "900.00", "2026-03-15 00:00:00", "true", "enterprise",
     "midnight exactly", "0.05", "2026-03-15 23:59:59", "eu",
     "1100.00", "true", "temp9@test.com", "555-0009", "10009", '{"tier":"bronze"}'],

    # Row 10: ISO date with T but no time — incomplete ISO
    [10, "TEMP-010", "350.00", "2026-04-01T", "false", "smb",
     "ISO date with trailing T", "0.03", "2026-04-01T12:00", "apac",
     "450.00", "false", "temp10@test.com", "555-0010", "10010", '{"tier":"gold"}'],

    # ──────────────────────────────────────────────────────────
    # GROUP B: CROSS-COLUMN CONTRADICTIONS (rows 11-20)
    # ──────────────────────────────────────────────────────────

    # Row 11: is_active=false but last_login is TODAY
    [11, "LOGIC-011", "1200.00", "2026-01-01", "false", "enterprise",
     "inactive but logged in today", "0.05", "2026-02-13 14:30:00", "eu",
     "3000.00", "false", "logic11@test.com", "555-0011", "10011", '{"tier":"gold","active":true}'],

    # Row 12: discount_pct > 100% — impossible discount
    [12, "LOGIC-012", "500.00", "2026-01-15", "true", "smb",
     "discount over 100 percent", "2.50", "2026-02-01", "apac",
     "800.00", "true", "logic12@test.com", "555-0012", "10012", '{"tier":"silver"}'],

    # Row 13: Negative amount with positive balance 
    [13, "LOGIC-013", "-9999.99", "2026-01-20", "true", "enterprise",
     "negative amount but positive balance", "0.05", "2026-02-10", "eu",
     "50000.00", "true", "logic13@test.com", "555-0013", "10013", '{"tier":"gold"}'],

    # Row 14: Zero amount, zero balance, zero discount  
    [14, "LOGIC-014", "0", "2026-02-01", "true", "smb",
     "all zeros", "0", "2026-02-01", "apac",
     "0", "true", "logic14@test.com", "555-0014", "10014", '{"tier":"bronze"}'],

    # Row 15: Same value in every string column 
    [15, "LOGIC-015", "1.00", "2026-02-05", "true", "SAME",
     "SAME", "0.01", "2026-02-05", "SAME",
     "1.00", "true", "SAME", "SAME", "SAME", "SAME"],

    # Row 16: Email in amount field, amount in email field
    [16, "LOGIC-016", "user@example.com", "2026-02-06", "false", "enterprise",
     "fields are swapped", "0.05", "2026-02-06", "eu",
     "not_a_number", "false", "999.99", "555-0016", "10016", '{"tier":"silver"}'],

    # Row 17: Discount exactly 1.0 (100%) 
    [17, "LOGIC-017", "1000.00", "2026-02-07", "true", "smb",
     "100 percent discount", "1.0", "2026-02-07", "apac",
     "0.00", "true", "logic17@test.com", "555-0017", "10017", '{"tier":"gold"}'],

    # Row 18: Customer ID that looks like a number
    [18, "12345", "800.00", "2026-02-08", "false", "enterprise",
     "numeric customer_id", "0.04", "2026-02-08", "eu",
     "1200.00", "false", "logic18@test.com", "555-0018", "10018", '{"tier":"bronze"}'],

    # Row 19: Customer ID that looks like a boolean
    [19, "true", "650.00", "2026-02-09", "true", "smb",
     "boolean customer_id", "0.03", "2026-02-09", "apac",
     "900.00", "true", "logic19@test.com", "555-0019", "10019", '{"tier":"silver"}'],

    # Row 20: Customer ID that is empty string
    [20, "", "550.00", "2026-02-10", "false", "enterprise",
     "empty customer_id", "0.02", "2026-02-10", "eu",
     "700.00", "false", "logic20@test.com", "555-0020", "10020", '{"tier":"gold"}'],

    # ──────────────────────────────────────────────────────────
    # GROUP C: STATISTICAL LANDMINES (rows 21-30)
    # ──────────────────────────────────────────────────────────

    # Rows 21-25: All have identical amounts — zero variance
    [21, "STAT-021", "42.00", "2026-02-11", "true", "enterprise",
     "identical amount block", "0.05", "2026-02-11", "eu",
     "42.00", "true", "stat21@test.com", "555-0021", "10021", '{"tier":"gold"}'],
    [22, "STAT-022", "42.00", "2026-02-11", "false", "smb",
     "identical amount block", "0.05", "2026-02-11", "apac",
     "42.00", "false", "stat22@test.com", "555-0022", "10022", '{"tier":"silver"}'],
    [23, "STAT-023", "42.00", "2026-02-11", "true", "enterprise",
     "identical amount block", "0.05", "2026-02-11", "eu",
     "42.00", "true", "stat23@test.com", "555-0023", "10023", '{"tier":"bronze"}'],
    [24, "STAT-024", "42.00", "2026-02-11", "false", "smb",
     "identical amount block", "0.05", "2026-02-11", "apac",
     "42.00", "false", "stat24@test.com", "555-0024", "10024", '{"tier":"gold"}'],
    [25, "STAT-025", "42.00", "2026-02-11", "true", "enterprise",
     "identical amount block", "0.05", "2026-02-11", "eu",
     "42.00", "true", "stat25@test.com", "555-0025", "10025", '{"tier":"silver"}'],

    # Row 26: Extreme outlier — completely dominates mean
    [26, "STAT-026", "999999999.99", "2026-02-12", "true", "smb",
     "extreme outlier amount", "0.99", "2026-02-12", "apac",
     "999999999.99", "true", "stat26@test.com", "555-0026", "10026", '{"tier":"bronze"}'],

    # Row 27: Tiny positive amount — near machine epsilon
    [27, "STAT-027", "0.000000001", "2026-02-12", "false", "enterprise",
     "near machine epsilon", "0.000001", "2026-02-12", "eu",
     "0.000000001", "false", "stat27@test.com", "555-0027", "10027", '{"tier":"gold"}'],

    # Row 28: Negative near-zero
    [28, "STAT-028", "-0.000000001", "2026-02-12", "true", "smb",
     "negative near zero", "0.000001", "2026-02-12", "apac",
     "-0.000000001", "true", "stat28@test.com", "555-0028", "10028", '{"tier":"silver"}'],

    # Row 29: MAX safe integer for JavaScript
    [29, "STAT-029", "9007199254740991", "2026-02-13", "false", "enterprise",
     "JS MAX_SAFE_INTEGER", "0.05", "2026-02-13", "eu",
     "9007199254740991", "false", "stat29@test.com", "555-0029", "10029", '{"tier":"bronze"}'],

    # Row 30: Amount = -0.0 (negative zero)
    [30, "STAT-030", "-0.0", "2026-02-13", "true", "smb",
     "negative zero", "0.05", "2026-02-13", "apac",
     "-0.0", "true", "stat30@test.com", "555-0030", "10030", '{"tier":"gold"}'],

    # ──────────────────────────────────────────────────────────
    # GROUP D: SCHEMA MIMICRY & INJECTION (rows 31-40)
    # ──────────────────────────────────────────────────────────

    # Row 31: Notes that contain valid CSV rows
    [31, "INJECT-031", "100.00", "2026-02-14", "true", "enterprise",
     '32,"INJECT-032","200.00","2026-02-14","true","enterprise","fake row"', "0.05",
     "2026-02-14", "eu", "100.00", "true", "inj31@test.com", "555-0031", "10031", '{"tier":"silver"}'],

    # Row 32: Notes that contain JSON
    [32, "INJECT-032", "200.00", "2026-02-14", "false", "smb",
     '{"_row_id": 99, "amount": "HACKED", "exploit": true}', "0.03",
     "2026-02-14", "apac", "200.00", "false", "inj32@test.com", "555-0032", "10032", '{"tier":"bronze"}'],

    # Row 33: Notes that contain YAML
    [33, "INJECT-033", "300.00", "2026-02-15", "true", "enterprise",
     "dataset:\n  name: hacked\nroles:\n  critical: all", "0.05",
     "2026-02-15", "eu", "300.00", "true", "inj33@test.com", "555-0033", "10033", '{"tier":"gold"}'],

    # Row 34: Notes that contain SQL injection
    [34, "INJECT-034", "400.00", "2026-02-15", "false", "smb",
     "Robert'); DROP TABLE students;--", "0.04",
     "2026-02-15", "apac", "400.00", "false", "inj34@test.com", "555-0034", "10034", '{"tier":"silver"}'],

    # Row 35: meta_json is completely invalid JSON
    [35, "INJECT-035", "500.00", "2026-02-16", "true", "enterprise",
     "meta_json is broken", "0.05", "2026-02-16", "eu",
     "500.00", "true", "inj35@test.com", "555-0035", "10035", '{broken json!!!'],

    # Row 36: meta_json is a JSON array, not object
    [36, "INJECT-036", "600.00", "2026-02-16", "false", "smb",
     "meta_json is array", "0.03", "2026-02-16", "apac",
     "600.00", "false", "inj36@test.com", "555-0036", "10036", '[1, 2, 3, "not_an_object"]'],

    # Row 37: Amount field contains a date, date field contains an amount
    [37, "INJECT-037", "2026-03-15", "750.00", "true", "enterprise",
     "amount and date are swapped", "0.05", "2026-02-17", "eu",
     "850.00", "true", "inj37@test.com", "555-0037", "10037", '{"tier":"bronze"}'],

    # Row 38: Region value so long it could cause memory issues in categorization
    [38, "INJECT-038", "700.00", "2026-02-17", "false", "smb",
     "very long region value", "0.04", "2026-02-17", "x" * 500,
     "700.00", "false", "inj38@test.com", "555-0038", "10038", '{"tier":"gold"}'],

    # Row 39: Path traversal attempt in notes
    [39, "INJECT-039", "800.00", "2026-02-18", "true", "enterprise",
     "../../../etc/passwd", "0.05", "2026-02-18", "eu",
     "800.00", "true", "inj39@test.com", "555-0039", "10039", '{"tier":"silver"}'],

    # Row 40: Newlines embedded in every field via literal \n
    [40, "INJECT-040", "900.00", "2026-02-18", "false", "smb",
     "line1\nline2\nline3", "0.03", "2026-02-18", "apac",
     "900.00", "false", "inj40@test.com", "555-0040", "10040", '{"tier":"bronze"}'],

    # ──────────────────────────────────────────────────────────
    # GROUP E: TYPE AMBIGUITY & COERCION TRAPS (rows 41-50)
    # ──────────────────────────────────────────────────────────

    # Row 41: Amount that looks like a phone number
    [41, "TYPE-041", "555-0100", "2026-02-19", "true", "enterprise",
     "amount looks like phone", "0.05", "2026-02-19", "eu",
     "1-800-555-0199", "true", "type41@test.com", "555-0041", "10041", '{"tier":"gold"}'],

    # Row 42: Amount that looks like a date
    [42, "TYPE-042", "01/15/2026", "2026-02-19", "false", "smb",
     "amount looks like date", "0.04", "2026-02-19", "apac",
     "12-31-2025", "false", "type42@test.com", "555-0042", "10042", '{"tier":"silver"}'],

    # Row 43: Boolean-like amount values  
    [43, "TYPE-043", "true", "2026-02-20", "true", "enterprise",
     "boolean in amount field", "0.05", "2026-02-20", "eu",
     "false", "true", "type43@test.com", "555-0043", "10043", '{"tier":"bronze"}'],

    # Row 44: Amount with leading zeros — octal trap
    [44, "TYPE-044", "007", "2026-02-20", "false", "smb",
     "leading zeros octal trap", "0.03", "2026-02-20", "apac",
     "010", "false", "type44@test.com", "555-0044", "10044", '{"tier":"gold"}'],

    # Row 45: ZIP code that looks like octal/numeric
    [45, "TYPE-045", "500.00", "2026-02-21", "true", "enterprise",
     "zip with leading zero", "0.05", "2026-02-21", "eu",
     "500.00", "true", "type45@test.com", "555-0045", "02134", '{"tier":"silver"}'],

    # Row 46: Hexadecimal-looking amount  
    [46, "TYPE-046", "0xFF", "2026-02-21", "false", "smb",
     "hex amount", "0.04", "2026-02-21", "apac",
     "0xDEAD", "false", "type46@test.com", "555-0046", "10046", '{"tier":"bronze"}'],

    # Row 47: Binary-looking amount
    [47, "TYPE-047", "0b1010", "2026-02-22", "true", "enterprise",
     "binary amount", "0.05", "2026-02-22", "eu",
     "0b11111111", "true", "type47@test.com", "555-0047", "10047", '{"tier":"gold"}'],

    # Row 48: Roman numeral amount  
    [48, "TYPE-048", "XIV", "2026-02-22", "false", "smb",
     "roman numeral amount", "0.03", "2026-02-22", "apac",
     "MCMXCIX", "false", "type48@test.com", "555-0048", "10048", '{"tier":"silver"}'],

    # Row 49: Amount with written number words
    [49, "TYPE-049", "one thousand", "2026-02-23", "true", "enterprise",
     "written number words", "0.05", "2026-02-23", "eu",
     "five hundred", "true", "type49@test.com", "555-0049", "10049", '{"tier":"bronze"}'],

    # Row 50: Mixed number+text amount
    [50, "TYPE-050", "about 500", "2026-02-23", "false", "smb",
     "text mixed with number", "roughly 5%", "2026-02-23", "apac",
     "approx 1000", "false", "type50@test.com", "555-0050", "10050", '{"tier":"gold"}'],

    # ──────────────────────────────────────────────────────────
    # GROUP F: RELATIONAL & KEY CHAOS (rows 51-60)
    # ──────────────────────────────────────────────────────────

    # Row 51: _row_id = 0 — edge of valid range
    [0, "KEY-051", "1500.00", "2026-02-24", "true", "enterprise",
     "row_id zero", "0.05", "2026-02-24", "eu",
     "2000.00", "true", "key51@test.com", "555-0051", "10051", '{"tier":"silver"}'],

    # Row 52: _row_id = MAX_INT
    [2147483647, "KEY-052", "1600.00", "2026-02-24", "false", "smb",
     "row_id max int", "0.04", "2026-02-24", "apac",
     "2100.00", "false", "key52@test.com", "555-0052", "10052", '{"tier":"bronze"}'],

    # Row 53: Customer ID with pipe delimiter inside
    [53, "KEY|053|PIPE", "1700.00", "2026-02-25", "true", "enterprise",
     "pipe in customer_id", "0.05", "2026-02-25", "eu",
     "2200.00", "true", "key53@test.com", "555-0053", "10053", '{"tier":"gold"}'],

    # Row 54: Customer ID that is a UUID
    [54, "550e8400-e29b-41d4-a716-446655440000", "1800.00", "2026-02-25", "false", "smb",
     "UUID customer_id", "0.03", "2026-02-25", "apac",
     "2300.00", "false", "key54@test.com", "555-0054", "10054", '{"tier":"silver"}'],

    # Row 55: Customer ID with slashes
    [55, "ORG/DEPT/USER/55", "1900.00", "2026-02-26", "true", "enterprise",
     "slashes in customer_id", "0.05", "2026-02-26", "eu",
     "2400.00", "true", "key55@test.com", "555-0055", "10055", '{"tier":"bronze"}'],

    # Row 56: All boolean columns are the string "null" (not empty)
    [56, "KEY-056", "1000.00", "2026-02-26", "null", "smb",
     "string null in boolean", "0.04", "2026-02-26", "apac",
     "1500.00", "null", "key56@test.com", "555-0056", "10056", '{"tier":"gold"}'],

    # Row 57: Extremely long customer_id
    [57, "A" * 1000, "1100.00", "2026-02-27", "true", "enterprise",
     "1000-char customer_id", "0.05", "2026-02-27", "eu",
     "1600.00", "true", "key57@test.com", "555-0057", "10057", '{"tier":"silver"}'],

    # Row 58: Customer ID is a single space
    [58, " ", "1200.00", "2026-02-27", "false", "smb",
     "single space customer_id", "0.03", "2026-02-27", "apac",
     "1700.00", "false", "key58@test.com", "555-0058", "10058", '{"tier":"bronze"}'],

    # Row 59: Completely identical to row 21 — exact duplicate
    [21, "STAT-021", "42.00", "2026-02-11", "true", "enterprise",
     "identical amount block", "0.05", "2026-02-11", "eu",
     "42.00", "true", "stat21@test.com", "555-0021", "10021", '{"tier":"gold"}'],

    # Row 60: All fields are the string "0"
    [60, "0", "0", "0", "0", "0",
     "0", "0", "0", "0",
     "0", "0", "0", "0", "0", "0"],

    # ──────────────────────────────────────────────────────────
    # GROUP G: CLEAN BASELINE (rows 61-65) — just enough clean data
    # ──────────────────────────────────────────────────────────
    [61, "CLEAN-061", "5000.00", "2026-01-05", "true", "enterprise",
     "clean row", "0.05", "2026-01-10", "eu",
     "8000.00", "true", "clean61@test.com", "555-0061", "10061", '{"tier":"gold"}'],
    [62, "CLEAN-062", "5500.00", "2026-01-06", "false", "smb",
     "clean row", "0.04", "2026-01-11", "apac",
     "8500.00", "false", "clean62@test.com", "555-0062", "10062", '{"tier":"silver"}'],
    [63, "CLEAN-063", "6000.00", "2026-01-07", "true", "enterprise",
     "clean row", "0.05", "2026-01-12", "eu",
     "9000.00", "true", "clean63@test.com", "555-0063", "10063", '{"tier":"bronze"}'],
    [64, "CLEAN-064", "6500.00", "2026-01-08", "false", "smb",
     "clean row", "0.03", "2026-01-13", "apac",
     "9500.00", "false", "clean64@test.com", "555-0064", "10064", '{"tier":"gold"}'],
    [65, "CLEAN-065", "7000.00", "2026-01-09", "true", "enterprise",
     "clean row", "0.05", "2026-01-14", "eu",
     "10000.00", "true", "clean65@test.com", "555-0065", "10065", '{"tier":"silver"}'],
]

out_path = "adversarial_logic.csv"
with open(out_path, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(HEADER)
    for row in rows:
        writer.writerow(row)

print(f"Wrote {len(rows)} rows to {out_path}")
print(f"File size: {__import__('os').path.getsize(out_path)} bytes")
