#!/usr/bin/env python3
"""Generate robustness_challenge.csv — a killer dataset for pipeline stress testing."""
import csv
import io

HEADER = [
    "_row_id", "customer_id", "amount", "signup_date", "is_active",
    "segment", "notes", "discount_pct", "last_login", "region",
    "account_balance", "flag", "email", "phone", "zip", "meta_json"
]

rows = [
    # --- Row 1: BOM will be prepended to file; this row is "clean baseline" ---
    [1, "C001", "1500.00", "2026-01-10", "true", "enterprise", "clean baseline row", "0.05", "2026-01-10T10:00:00Z", "eu", "5000.00", "true", "user1@test.com", "+1-555-0001", "32608", '{"tier":"gold"}'],

    # --- Row 2: Full-width Unicode digits in amount ---
    [2, "C002", "\uff04\uff11\uff0c\uff12\uff13\uff14\uff0e\uff15\uff16", "2026-01-11", "false", "smb", "full-width amount ＄１，２３４．５６", "0.03", "2026-01-11 09:00:00", "apac", "2000.00", "false", "user2@test.com", "+1-555-0002", "32608", '{"tier":"silver"}'],

    # --- Row 3: Parenthesized negative (accounting format) ---
    [3, "C003", "(1,234.00)", "2026-01-12", "true", "enterprise", "accounting negative parentheses", "0.07", "2026-01-12 10:30:00", "eu", "(5,678.90)", "true", "user3@test.com", "+1-555-0003", "32608", '{"tier":"gold"}'],

    # --- Row 4: En-dash and em-dash as minus signs ---
    [4, "C004", "\u20131234.56", "2026-01-13", "false", "smb", "en-dash minus \u2013 in amount", "0.04", "2026-01-13 11:00:00", "eu", "\u20145678.90", "false", "user4@test.com", "+1-555-0004", "32608", '{"tier":"bronze"}'],

    # --- Row 5: Epoch timestamp (seconds) ---
    [5, "C005", "999.99", "1737936000", "true", "enterprise", "epoch seconds as signup date", "0.05", "1737936000", "apac", "1200.00", "true", "user5@test.com", "+1-555-0005", "32608", '{"tier":"gold"}'],

    # --- Row 6: Epoch timestamp (milliseconds) ---
    [6, "C006", "750.50", "1737936000000", "false", "smb", "epoch milliseconds as signup date", "0.06", "1737936000000", "eu", "800.00", "false", "user6@test.com", "+1-555-0006", "32608", '{"tier":"silver"}'],

    # --- Row 7: RTL mark and LTR mark in amount and date ---
    [7, "C007", "\u200F$2,500.00\u200F", "2026-01-15", "true", "enterprise", "RTL marks in amount", "0.05", "\u200E2026-01-15 12:00:00\u200E", "eu", "\u200F3000.00\u200F", "true", "user7@test.com", "+1-555-0007", "32608", '{"tier":"gold"}'],

    # --- Row 8: Zero-width space and zero-width non-joiner in amount ---
    [8, "C008", "1\u200B,\u200B234\u200B.\u200B56", "2026-01-16", "false", "smb", "zero-width spaces in amount", "0.03", "2026-01-16 08:00:00", "apac", "7\u200C8\u200C9\u200C0", "false", "user8@test.com", "+1-555-0008", "32608", '{"tier":"bronze"}'],

    # --- Row 9: NULL byte in notes and email ---
    [9, "C009", "500.00", "2026-01-17", "true", "enterprise", "has\x00null\x00bytes", "0.05", "2026-01-17 14:00:00", "eu", "1500.00", "true", "user9\x00@test.com", "+1-555-0009", "32608", '{"tier":"gold"}'],

    # --- Row 10: Completely empty _row_id (will be pd.NA after coercion) ---
    ["", "C010", "300.00", "2026-01-18", "false", "smb", "empty row_id", "0.02", "2026-01-18 09:00:00", "apac", "400.00", "false", "user10@test.com", "+1-555-0010", "32608", '{"tier":"silver"}'],

    # --- Row 11: Negative _row_id ---
    [-1, "C011", "1000.00", "2026-01-19", "true", "enterprise", "negative row_id", "0.05", "2026-01-19 10:00:00", "eu", "2000.00", "true", "user11@test.com", "+1-555-0011", "32608", '{"tier":"gold"}'],

    # --- Row 12: _row_id as float with many decimals ---
    [12.999, "C012", "600.00", "2026-01-20", "false", "smb", "float row_id 12.999", "0.04", "2026-01-20 11:00:00", "apac", "700.00", "false", "user12@test.com", "+1-555-0012", "32608", '{"tier":"bronze"}'],

    # --- Row 13: Boolean edge cases ---
    [13, "C013", "800.00", "2026-01-21", "TRUE\t", "enterprise", "bool with trailing tab", "0.05", "2026-01-21 09:30:00", "eu", "900.00", "  YES  ", "user13@test.com", "+1-555-0013", "32608", '{"tier":"gold"}'],

    # --- Row 14: Boolean as float "1.0" ---
    [14, "C014", "450.00", "2026-01-22", "1.0", "smb", "boolean as 1.0 float", "0.03", "2026-01-22 10:00:00", "apac", "550.00", "0.0", "user14@test.com", "+1-555-0014", "32608", '{"tier":"silver"}'],

    # --- Row 15: Boolean non-standard values ---
    [15, "C015", "200.00", "2026-01-23", "oui", "enterprise", "french boolean", "0.05", "2026-01-23 11:00:00", "eu", "250.00", "nein", "user15@test.com", "+1-555-0015", "32608", '{"tier":"bronze"}'],

    # --- Row 16: Leading dot amount ".5" and trailing dot "5." ---
    [16, "C016", ".5", "2026-01-24", "true", "smb", "leading dot amount", "0.04", "2026-01-24 12:00:00", "apac", "5.", "true", "user16@test.com", "+1-555-0016", "32608", '{"tier":"gold"}'],

    # --- Row 17: Scientific notation ---
    [17, "C017", "1.5e-10", "2026-01-25", "false", "enterprise", "scientific notation small", "0.05", "2026-01-25 13:00:00", "eu", "3E+8", "false", "user17@test.com", "+1-555-0017", "32608", '{"tier":"silver"}'],

    # --- Row 18: NaN and Infinity as amount string ---
    [18, "C018", "NaN", "2026-01-26", "true", "smb", "NaN as amount string", "0.03", "2026-01-26 14:00:00", "apac", "Infinity", "true", "user18@test.com", "+1-555-0018", "32608", '{"tier":"bronze"}'],

    # --- Row 19: -Infinity and -NaN ---
    [19, "C019", "-Infinity", "2026-01-27", "false", "enterprise", "negative infinity amount", "0.05", "2026-01-27 15:00:00", "eu", "-Infinity", "false", "user19@test.com", "+1-555-0019", "32608", '{"tier":"gold"}'],

    # --- Row 20: Whitespace-only values in multiple columns ---
    [20, "C020", "   ", "   ", "   ", "   ", "   ", "   ", "   ", "   ", "   ", "   ", "user20@test.com", "+1-555-0020", "32608", '{"tier":"silver"}'],

    # --- Row 21: Tab-only values ---
    [21, "C021", "\t\t", "\t", "\t", "\t", "\t\t\ttabs only\t\t", "\t", "\t", "\t", "\t\t", "\t", "user21@test.com", "+1-555-0021", "32608", '{"tier":"bronze"}'],

    # --- Row 22: Mixed timezone datetimes ---
    [22, "C022", "1100.00", "2026-01-15T10:00:00+05:30", "true", "smb", "IST timezone", "0.04", "2026-01-14T23:30:00-05:00", "apac", "1100.00", "true", "user22@test.com", "+1-555-0022", "32608", '{"tier":"gold"}'],

    # --- Row 23: Natural language date ---
    [23, "C023", "900.00", "January 15, 2026", "false", "enterprise", "natural language date", "0.05", "Feb 1, 2026 3:00 PM", "eu", "900.00", "false", "user23@test.com", "+1-555-0023", "32608", '{"tier":"silver"}'],

    # --- Row 24: Date with ordinal suffix ---
    [24, "C024", "700.00", "15th Jan 2026", "true", "smb", "ordinal date suffix", "0.03", "1st Feb 2026", "apac", "700.00", "true", "user24@test.com", "+1-555-0024", "32608", '{"tier":"bronze"}'],

    # --- Row 25: HTML entities in notes ---
    [25, "C025", "1300.00", "2026-01-30", "false", "enterprise", "notes with &amp; and &lt;script&gt; and &#36;100", "0.05", "2026-01-30 16:00:00", "eu", "1300.00", "false", "user25@test.com", "+1-555-0025", "32608", '{"tier":"gold"}'],

    # --- Row 26: Formula injection attempts ---
    [26, "C026", "=1+1", "2026-01-31", "true", "smb", "=SUM(A1:A10)", "0.04", "2026-01-31 09:00:00", "apac", "+cmd|'/C calc'!Z0", "true", "user26@test.com", "+1-555-0026", "32608", '{"tier":"silver"}'],

    # --- Row 27: Very long string in notes (5000 chars) ---
    [27, "C027", "400.00", "2026-02-01", "false", "enterprise", "X" * 5000, "0.05", "2026-02-01 10:00:00", "eu", "400.00", "false", "user27@test.com", "+1-555-0027", "32608", '{"tier":"bronze"}'],

    # --- Row 28: Duplicate _row_id (same as row 1) ---
    [1, "C028", "1500.00", "2026-02-02", "true", "smb", "duplicate row_id=1", "0.03", "2026-02-02 11:00:00", "apac", "1500.00", "true", "user28@test.com", "+1-555-0028", "32608", '{"tier":"gold"}'],

    # --- Row 29: Amount with multiple currency symbols ---
    [29, "C029", "US$1,234.56USD", "2026-02-03", "false", "enterprise", "multiple currency markers", "0.05", "2026-02-03 12:00:00", "eu", "€£100.00", "false", "user29@test.com", "+1-555-0029", "32608", '{"tier":"silver"}'],

    # --- Row 30: Amount with spaces as thousands separator ---
    [30, "C030", "1 234 567.89", "2026-02-04", "true", "smb", "space thousands separator", "0.04", "2026-02-04 13:00:00", "apac", "9 876.54", "true", "user30@test.com", "+1-555-0030", "32608", '{"tier":"bronze"}'],

    # --- Row 31: Percent amounts ---
    [31, "C031", "15%", "2026-02-05", "false", "enterprise", "percent in amount", "50%", "2026-02-05 14:00:00", "eu", "125%", "false", "user31@test.com", "+1-555-0031", "32608", '{"tier":"gold"}'],

    # --- Row 32: Negative percent ---
    [32, "C032", "-25.5%", "2026-02-06", "true", "smb", "negative percent amount", "-10%", "2026-02-06 15:00:00", "apac", "-75.3%", "true", "user32@test.com", "+1-555-0032", "32608", '{"tier":"silver"}'],

    # --- Row 33: All-missing row (except row_id and customer_id) ---
    [33, "C033", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],

    # --- Row 34: Various null tokens ---
    [34, "C034", "NULL", "null", "N/A", "n/a", "#N/A", "#NULL!", "—", "---", "?", "???", "user34@test.com", "+1-555-0034", "32608", '{"tier":"bronze"}'],

    # --- Row 35: Unicode null tokens ---
    [35, "C035", "\u2014", "2026-02-09", "true", "enterprise", "em-dash as null token", "0.05", "2026-02-09 09:00:00", "eu", "\u2013", "true", "user35@test.com", "+1-555-0035", "32608", '{"tier":"gold"}'],

    # --- Row 36: Emoji everywhere ---
    [36, "C036", "💰1,500.00", "2026-02-10", "👍", "smb", "🎉 emoji party! 🎊🥳🎈", "0.04", "2026-02-10 10:00:00", "apac", "💸2000", "👎", "user36@test.com", "+1-555-0036", "32608", '{"tier":"silver"}'],

    # --- Row 37: Mixed encodings in segment/region ---
    [37, "C037", "1000.00", "2026-02-11", "true", "ENTERPRISE", "segment casing mix", "0.05", "2026-02-11 11:00:00", "Eu", "1000.00", "true", "user37@test.com", "+1-555-0037", "32608", '{"tier":"bronze"}'],

    # --- Row 38: Surrogate-like characters ---
    [38, "C038", "850.00", "2026-02-12", "false", "smb", "accented: caf\u00e9 na\u00efve r\u00e9sum\u00e9", "0.03", "2026-02-12 12:00:00", "apac", "850.00", "false", "user38@test.com", "+1-555-0038", "32608", '{"tier":"gold"}'],

    # --- Row 39: NBSP as thousands separator ---
    [39, "C039", "1\u00a0234\u00a0567,89", "2026-02-13", "true", "enterprise", "NBSP thousands separator", "0.05", "2026-02-13 13:00:00", "eu", "9\u00a0876,54", "true", "user39@test.com", "+1-555-0039", "32608", '{"tier":"silver"}'],

    # --- Row 40: Thin space as thousands separator ---
    [40, "C040", "1\u2009234\u2009567.89", "2026-02-14", "false", "smb", "thin space thousands", "0.04", "2026-02-14 14:00:00", "apac", "9\u2009876.54", "false", "user40@test.com", "+1-555-0040", "32608", '{"tier":"bronze"}'],

    # --- Rows 41-50: Clean baseline rows for ratio stability ---
    [41, "C041", "2000.00", "2026-02-15", "true", "enterprise", "baseline", "0.05", "2026-02-15 10:00:00", "eu", "3000.00", "true", "user41@test.com", "+1-555-0041", "32608", '{"tier":"gold"}'],
    [42, "C042", "2100.00", "2026-02-16", "false", "smb", "baseline", "0.04", "2026-02-16 11:00:00", "apac", "3100.00", "false", "user42@test.com", "+1-555-0042", "32608", '{"tier":"silver"}'],
    [43, "C043", "2200.00", "2026-02-17", "true", "enterprise", "baseline", "0.05", "2026-02-17 12:00:00", "eu", "3200.00", "true", "user43@test.com", "+1-555-0043", "32608", '{"tier":"gold"}'],
    [44, "C044", "2300.00", "2026-02-18", "false", "smb", "baseline", "0.03", "2026-02-18 13:00:00", "apac", "3300.00", "false", "user44@test.com", "+1-555-0044", "32608", '{"tier":"bronze"}'],
    [45, "C045", "2400.00", "2026-02-19", "true", "enterprise", "baseline", "0.05", "2026-02-19 14:00:00", "eu", "3400.00", "true", "user45@test.com", "+1-555-0045", "32608", '{"tier":"gold"}'],
    [46, "C046", "2500.00", "2026-02-20", "false", "smb", "baseline", "0.04", "2026-02-20 15:00:00", "apac", "3500.00", "false", "user46@test.com", "+1-555-0046", "32608", '{"tier":"silver"}'],
    [47, "C047", "2600.00", "2026-02-21", "true", "enterprise", "baseline", "0.05", "2026-02-21 16:00:00", "eu", "3600.00", "true", "user47@test.com", "+1-555-0047", "32608", '{"tier":"gold"}'],
    [48, "C048", "2700.00", "2026-02-22", "false", "smb", "baseline", "0.03", "2026-02-22 09:00:00", "apac", "3700.00", "false", "user48@test.com", "+1-555-0048", "32608", '{"tier":"bronze"}'],
    [49, "C049", "2800.00", "2026-02-23", "true", "enterprise", "baseline", "0.05", "2026-02-23 10:00:00", "eu", "3800.00", "true", "user49@test.com", "+1-555-0049", "32608", '{"tier":"gold"}'],
    [50, "C050", "2900.00", "2026-02-24", "false", "smb", "baseline", "0.04", "2026-02-24 11:00:00", "apac", "3900.00", "false", "user50@test.com", "+1-555-0050", "32608", '{"tier":"silver"}'],

    # --- Row 51: Double-dot amount "1.2.3" ---
    [51, "C051", "1.2.3", "2026-02-25", "true", "enterprise", "double dot amount", "0.05", "2026-02-25 12:00:00", "eu", "4.5.6", "true", "user51@test.com", "+1-555-0051", "32608", '{"tier":"gold"}'],

    # --- Row 52: Amount with plus sign ---
    [52, "C052", "+1234.56", "2026-02-26", "false", "smb", "plus sign amount", "0.04", "2026-02-26 13:00:00", "apac", "+0.001", "false", "user52@test.com", "+1-555-0052", "32608", '{"tier":"silver"}'],

    # --- Row 53: Extremely large number ---
    [53, "C053", "99999999999999999999.99", "2026-02-27", "true", "enterprise", "huge number", "0.05", "2026-02-27 14:00:00", "eu", "1e308", "true", "user53@test.com", "+1-555-0053", "32608", '{"tier":"gold"}'],

    # --- Row 54: Row with all columns as "—" (em-dash) ---
    [54, "C054", "—", "—", "—", "—", "—", "—", "—", "—", "—", "—", "—", "—", "—", "—"],

    # --- Row 55: Unicode fraction characters ---
    [55, "C055", "½", "2026-03-01", "false", "smb", "unicode fraction", "¼", "2026-03-01 15:00:00", "apac", "¾", "false", "user55@test.com", "+1-555-0055", "32608", '{"tier":"bronze"}'],
]

# Write with BOM prefix to test BOM handling
out_path = "robustness_challenge.csv"
with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(HEADER)
    for row in rows:
        writer.writerow(row)

print(f"Wrote {len(rows)} rows to {out_path}")
print(f"File size: {__import__('os').path.getsize(out_path)} bytes")
