#!/usr/bin/env python3
"""Generate three domain-specific datasets that simulate real-world data quality
nightmares from retail (Walmart-style), hospital billing, and insurance claims.

Mindset: These are NOT encoding tricks or semantic paradoxes. These are the
ACTUAL data quality issues that data engineers face when processing reports
from these industries.
"""
import csv
import random
import os

random.seed(42)  # Reproducible

# ═══════════════════════════════════════════════════════════════
# DATASET 1: WALMART-STYLE RETAIL SALES
# ═══════════════════════════════════════════════════════════════

RETAIL_HEADER = [
    "_row_id", "store_id", "transaction_id", "transaction_date", "sku",
    "product_name", "quantity", "unit_price", "total_amount", "payment_method",
    "discount_applied", "tax_amount", "is_return", "cashier_id", "register_num",
    "notes"
]

retail_rows = [
    # --- Normal transactions (baseline) ---
    [1, "WM-0042", "TXN-2026-00001", "2026-01-15 09:23:11", "00-49000-00126",
     "Coca-Cola 12pk", 2, "5.98", "11.96", "credit", "0.00", "0.84",
     "false", "EMP-1234", "R07", ""],
    [2, "WM-0042", "TXN-2026-00002", "2026-01-15 09:25:33", "00-41570-00215",
     "Tide Pods 42ct", 1, "12.97", "12.97", "debit", "0.00", "0.91",
     "false", "EMP-1234", "R07", ""],

    # --- Row 3: Return — negative quantity AND negative total ---
    [3, "WM-0042", "TXN-2026-00003", "2026-01-15 10:02:45", "00-49000-00126",
     "Coca-Cola 12pk", -1, "5.98", "-5.98", "credit", "0.00", "-0.42",
     "true", "EMP-1234", "R07", "Customer return - opened package"],

    # --- Row 4: VOID transaction — $0.00 total, was voided mid-scan ---
    [4, "WM-0042", "TXN-2026-00004", "2026-01-15 10:15:00", "00-88888-VOID",
     "*** VOID ***", 0, "0.00", "0.00", "void", "0.00", "0.00",
     "false", "EMP-1234", "R07", "VOIDED - wrong item scanned"],

    # --- Row 5: Gift card sale — looks like a huge sale but isn't revenue ---
    [5, "WM-0042", "TXN-2026-00005", "2026-01-15 10:30:22", "GC-500-VISA",
     "Visa Gift Card $500", 1, "500.00", "505.95", "cash", "0.00", "5.95",
     "false", "EMP-5678", "R03", "Gift card - activation fee $5.95"],

    # --- Row 6: SKU with leading zeros that get stripped —- UPC barcode ---
    [6, "WM-0042", "TXN-2026-00006", "2026-01-15 11:00:00", "0012345678905",
     "Great Value Milk 1gal", 1, "3.24", "3.24", "ebt", "0.00", "0.00",
     "false", "EMP-5678", "R03", "EBT - tax exempt food"],

    # --- Row 7: Tax-exempt purchase (EBT/food stamps) ---
    [7, "WM-0042", "TXN-2026-00007", "2026-01-15 11:15:00", "00-41570-00112",
     "Great Value Bread", 3, "1.28", "3.84", "ebt", "0.00", "0.00",
     "false", "EMP-5678", "R03", ""],

    # --- Row 8: Employee discount — discount > 10% ---
    [8, "WM-0042", "TXN-2026-00008", "2026-01-15 12:00:00", "00-68113-00455",
     "Samsung 55in TV", 1, "397.00", "357.30", "credit", "39.70", "25.01",
     "false", "EMP-9999", "R01", "EMPLOYEE DISCOUNT 10%"],

    # --- Row 9: POS glitch — duplicate transaction ID ---
    [9, "WM-0042", "TXN-2026-00002", "2026-01-15 09:25:33", "00-41570-00215",
     "Tide Pods 42ct", 1, "12.97", "12.97", "debit", "0.00", "0.91",
     "false", "EMP-1234", "R07", "POS DUPLICATE - system retry"],

    # --- Row 10: Price mismatch — unit_price * quantity != total_amount ---
    [10, "WM-0042", "TXN-2026-00010", "2026-01-15 13:00:00", "00-78000-00333",
     "Bounty Paper Towels 6pk", 2, "11.97", "25.94", "credit", "0.00", "1.82",
     "false", "EMP-3456", "R05", ""],

    # --- Row 11: Multi-store — different store, different date format ---
    [11, "WM-1337", "TXN-2026-00011", "01/15/2026 14:30:00", "00-49000-00126",
     "Coca-Cola 12pk", 1, "6.48", "6.48", "cash", "0.00", "0.45",
     "false", "EMP-0001", "R01", "Store 1337 uses MM/DD/YYYY format"],

    # --- Row 12: Pricing error — negative unit price ---
    [12, "WM-0042", "TXN-2026-00012", "2026-01-15 15:00:00", "00-30000-00100",
     "Price Correction", 1, "-15.00", "-15.00", "credit", "0.00", "0.00",
     "false", "EMP-MGR1", "R07", "MANAGER OVERRIDE - price adjustment"],

    # --- Row 13: Layaway payment — partial payment, not a sale ---
    [13, "WM-0042", "TXN-2026-00013", "2026-01-15 16:00:00", "LAYAWAY-PMT",
     "Layaway Payment - Order #LY-78234", 1, "50.00", "50.00", "cash", "0.00", "0.00",
     "false", "EMP-3456", "R05", "LAYAWAY - not revenue until pickup"],

    # --- Row 14: Self-checkout with weight discrepancy note ---
    [14, "WM-0042", "TXN-2026-00014", "2026-01-15 17:00:00", "PLU-4011",
     "Bananas", 2.37, "0.58", "1.37", "credit", "0.00", "0.00",
     "false", "SELF-SCO4", "SCO4", "Weight: 2.37 lbs @ $0.58/lb"],

    # --- Row 15: Walmart+ membership — subscription not product sale ---
    [15, "WM-0042", "TXN-2026-00015", "2026-01-15 18:00:00", "WM-PLUS-ANN",
     "Walmart+ Annual Membership", 1, "98.00", "98.00", "credit", "0.00", "0.00",
     "false", "EMP-5678", "R03", "Subscription renewal"],

    # --- Row 16: Pharmacy transaction — HIPAA restricted ---
    [16, "WM-0042", "TXN-2026-00016", "2026-01-16 09:00:00", "RX-REDACTED",
     "*** PHARMACY ***", 1, "REDACTED", "REDACTED", "insurance", "REDACTED", "0.00",
     "false", "PHARM-01", "RX1", "PHI REDACTED PER HIPAA"],

    # --- Row 17: International store — different currency ---
    [17, "WM-MX-0501", "TXN-MX-00017", "2026-01-16 10:00:00", "00-49000-00126",
     "Coca-Cola 12pk", 1, "MXN 109.00", "MXN 109.00", "credit", "0.00", "MXN 17.44",
     "false", "EMP-MX01", "R01", "Mexico store - amounts in MXN pesos"],

    # --- Row 18: Markdown/clearance — original price in notes ---
    [18, "WM-0042", "TXN-2026-00018", "2026-01-16 11:00:00", "00-88700-04521",
     "Christmas Lights 100ct CLEARANCE", 1, "2.50", "2.50", "cash", "0.00", "0.18",
     "false", "EMP-1234", "R07", "Was $14.97 - 83% markdown"],

    # --- Row 19: Cash back on debit — inflates total ---
    [19, "WM-0042", "TXN-2026-00019", "2026-01-16 12:00:00", "00-41570-00215",
     "Tide Pods 42ct", 1, "12.97", "52.88", "debit", "0.00", "0.91",
     "false", "EMP-5678", "R03", "CASHBACK $40.00 included in total"],

    # --- Row 20: Online pickup order — different transaction flow ---
    [20, "WM-0042", "OGP-2026-00020", "2026-01-16 14:00:00", "MULTI-OGP",
     "Online Grocery Pickup - 23 items", 23, "varies", "187.34", "credit", "12.50", "11.89",
     "false", "OGP-TEAM", "OGP", "Order #WM-OGP-789456 - curbside pickup"],

    # --- Rows 21-30: Clean baseline sales ---
    [21, "WM-0042", "TXN-2026-00021", "2026-01-16 15:00:00", "00-12345-67890",
     "Cheerios 20oz", 1, "4.98", "4.98", "credit", "0.00", "0.35",
     "false", "EMP-1234", "R07", ""],
    [22, "WM-0042", "TXN-2026-00022", "2026-01-16 15:15:00", "00-54321-09876",
     "Dawn Dish Soap", 2, "3.47", "6.94", "debit", "0.00", "0.49",
     "false", "EMP-1234", "R07", ""],
    [23, "WM-0042", "TXN-2026-00023", "2026-01-16 15:30:00", "00-11111-22222",
     "Crest Toothpaste", 1, "3.98", "3.98", "cash", "0.00", "0.28",
     "false", "EMP-5678", "R03", ""],
    [24, "WM-0042", "TXN-2026-00024", "2026-01-16 15:45:00", "00-33333-44444",
     "Doritos Party Size", 1, "5.48", "5.48", "credit", "0.00", "0.38",
     "false", "EMP-5678", "R03", ""],
    [25, "WM-0042", "TXN-2026-00025", "2026-01-16 16:00:00", "00-55555-66666",
     "Charmin Ultra Soft 12pk", 1, "12.94", "12.94", "debit", "0.00", "0.91",
     "false", "EMP-3456", "R05", ""],
    [26, "WM-0042", "TXN-2026-00026", "2026-01-17 09:00:00", "00-77777-88888",
     "Folgers Coffee 30oz", 1, "9.96", "9.96", "credit", "0.00", "0.70",
     "false", "EMP-1234", "R07", ""],
    [27, "WM-0042", "TXN-2026-00027", "2026-01-17 09:30:00", "00-99999-00000",
     "Huggies Diapers Sz3", 1, "24.94", "24.94", "credit", "0.00", "1.75",
     "false", "EMP-5678", "R03", ""],
    [28, "WM-0042", "TXN-2026-00028", "2026-01-17 10:00:00", "00-10101-20202",
     "Gatorade 8pk", 2, "6.98", "13.96", "cash", "0.00", "0.98",
     "false", "EMP-3456", "R05", ""],
    [29, "WM-0042", "TXN-2026-00029", "2026-01-17 10:30:00", "00-30303-40404",
     "Lysol Spray", 1, "5.97", "5.97", "debit", "0.00", "0.42",
     "false", "EMP-1234", "R07", ""],
    [30, "WM-0042", "TXN-2026-00030", "2026-01-17 11:00:00", "00-50505-60606",
     "Swiffer WetJet Refills", 1, "8.97", "8.97", "credit", "0.00", "0.63",
     "false", "EMP-5678", "R03", ""],
]

# ═══════════════════════════════════════════════════════════════
# DATASET 2: HOSPITAL BILLING REPORTS
# ═══════════════════════════════════════════════════════════════

HOSPITAL_HEADER = [
    "_row_id", "patient_id", "encounter_id", "service_date", "billing_date",
    "cpt_code", "icd10_code", "procedure_desc", "billed_amount", "insurance_plan",
    "allowed_amount", "patient_responsibility", "payment_status", "provider_id",
    "department", "notes"
]

hospital_rows = [
    # --- Row 1: Normal outpatient visit ---
    [1, "PAT-00001", "ENC-2026-0001", "2026-01-10", "2026-01-15",
     "99213", "J06.9", "Office Visit - Level 3", "185.00", "BlueCross PPO",
     "142.50", "42.50", "paid", "NPI-1234567890", "Family Medicine", ""],

    # --- Row 2: CPT code with modifier — "99213-25" is NOT just a number ---
    [2, "PAT-00002", "ENC-2026-0002", "2026-01-10", "2026-01-15",
     "99213-25", "E11.65", "Office Visit w/ Significant Eval", "220.00", "Aetna HMO",
     "168.00", "52.00", "paid", "NPI-1234567890", "Internal Medicine",
     "Modifier 25 - separate E/M same day as procedure"],

    # --- Row 3: Denied claim — $0 allowed, full patient responsibility ---
    [3, "PAT-00003", "ENC-2026-0003", "2026-01-11", "2026-01-20",
     "99215", "M54.5", "Office Visit - Level 5", "350.00", "UHC Choice Plus",
     "0.00", "350.00", "denied", "NPI-9876543210", "Orthopedics",
     "DENIED: Prior authorization required - not obtained"],

    # --- Row 4: Date of service 4 months before billing — common in hospital ---
    [4, "PAT-00004", "ENC-2025-9912", "2025-09-15", "2026-01-12",
     "43239", "K21.0", "Upper GI Endoscopy w/ Biopsy", "4250.00", "Medicare Part B",
     "1847.32", "369.46", "partial", "NPI-5555555555", "Gastroenterology",
     "Delayed billing - coding review completed 01/10/2026"],

    # --- Row 5: ICD-10 code that looks numeric — Z00.00 has leading Z ---
    [5, "PAT-00005", "ENC-2026-0005", "2026-01-12", "2026-01-12",
     "99395", "Z00.00", "Annual Wellness Visit", "275.00", "BlueCross PPO",
     "275.00", "0.00", "paid", "NPI-1234567890", "Family Medicine",
     "Preventive - covered at 100%"],

    # --- Row 6: Emergency room — massive amount, multiple ICD codes ---
    [6, "PAT-00006", "ENC-2026-0006", "2026-01-13 02:34:00", "2026-01-30",
     "99285", "I21.09;I50.9;R07.9", "ER Visit - Level 5 Critical", "18750.00", "Medicaid",
     "3200.00", "0.00", "paid", "NPI-ER-TEAM-01", "Emergency Dept",
     "STEMI - transferred to cath lab - multiple DX"],

    # --- Row 7: Self-pay / uninsured — no insurance plan ---
    [7, "PAT-00007", "ENC-2026-0007", "2026-01-14", "2026-01-14",
     "99203", "J02.9", "New Patient Visit", "250.00", "SELF-PAY",
     "250.00", "250.00", "pending", "NPI-1234567890", "Urgent Care",
     "Uninsured - sliding fee scale applied"],

    # --- Row 8: Bundled procedure — $0 because included in surgical package ---
    [8, "PAT-00008", "ENC-2026-0008", "2026-01-15", "2026-01-20",
     "99024", "M23.611", "Post-Op Follow-Up", "0.00", "Cigna Open Access",
     "0.00", "0.00", "bundled", "NPI-9876543210", "Orthopedics",
     "BUNDLED: Included in 90-day global surgical package #27447"],

    # --- Row 9: Lab work — CPT range 80000-89999 ---
    [9, "PAT-00009", "ENC-2026-0009", "2026-01-15", "2026-01-16",
     "80053", "R73.09", "Comprehensive Metabolic Panel", "45.00", "BlueCross PPO",
     "18.72", "3.74", "paid", "NPI-LAB-001", "Laboratory",
     "Quest Diagnostics reference lab"],

    # --- Row 10: Charity care / write-off — negative adjustment ---
    [10, "PAT-00010", "ENC-2025-8800", "2025-11-20", "2026-01-10",
     "ADJ-CHARITY", "N/A", "Charity Care Write-Off", "-12500.00", "UNINSURED",
     "0.00", "0.00", "adjusted", "ADMIN-FIN", "Finance",
     "Board-approved charity write-off - original balance $12,500"],

    # --- Row 11: HIPAA redacted — PHI removed ---
    [11, "REDACTED", "REDACTED", "2026-01-16", "2026-01-16",
     "REDACTED", "REDACTED", "*** PHI REMOVED ***", "REDACTED", "REDACTED",
     "REDACTED", "REDACTED", "REDACTED", "REDACTED", "REDACTED",
     "Record exists for audit trail only - PHI purged per retention policy"],

    # --- Row 12: Telehealth visit with place-of-service modifier ---
    [12, "PAT-00012", "ENC-2026-0012", "2026-01-16", "2026-01-16",
     "99214-95", "F32.1", "Telehealth Visit - Established", "175.00", "Aetna HMO",
     "140.00", "35.00", "paid", "NPI-TELE-001", "Psychiatry",
     "POS 10 - Telehealth in patient home. Modifier 95"],

    # --- Row 13: Duplicate charge — same encounter, same CPT ---
    [13, "PAT-00001", "ENC-2026-0001", "2026-01-10", "2026-01-18",
     "99213", "J06.9", "Office Visit - Level 3", "185.00", "BlueCross PPO",
     "142.50", "42.50", "paid", "NPI-1234567890", "Family Medicine",
     "DUPLICATE CHARGE - rebilled in error"],

    # --- Row 14: Workers comp — different payer rules ---
    [14, "PAT-00014", "ENC-2026-0014", "2026-01-17", "2026-01-17",
     "99213", "S62.001A", "Office Visit - Hand Fracture F/U", "185.00", "WC-EMPLOYERS-FIRST",
     "185.00", "0.00", "paid", "NPI-9876543210", "Orthopedics",
     "WC Claim #WC-2025-78456 - employer liable"],

    # --- Row 15: Anesthesia with time units — billed differently ---
    [15, "PAT-00015", "ENC-2026-0015", "2026-01-17", "2026-01-25",
     "00630-AA", "K35.80", "Anesthesia - Appendectomy", "2400.00", "UHC Choice Plus",
     "1680.00", "336.00", "paid", "NPI-ANES-001", "Anesthesiology",
     "150 min anesthesia time. AA modifier = anesthesiologist personally performed"],

    # --- Row 16: Coordination of benefits — two insurance plans ---
    [16, "PAT-00016", "ENC-2026-0016", "2026-01-18", "2026-01-25",
     "27447", "M17.11", "Total Knee Replacement", "45000.00", "Medicare Part A + Medigap",
     "12500.00", "1250.00", "paid", "NPI-9876543210", "Orthopedics",
     "Primary: Medicare $11,250. Secondary: Medigap $1,250. Patient: $1,250"],

    # --- Rows 17-25: Clean baseline visits ---
    [17, "PAT-00017", "ENC-2026-0017", "2026-01-19", "2026-01-22",
     "99214", "J45.20", "Asthma Follow-Up", "225.00", "BlueCross PPO",
     "178.50", "46.50", "paid", "NPI-1234567890", "Pulmonology", ""],
    [18, "PAT-00018", "ENC-2026-0018", "2026-01-19", "2026-01-22",
     "99213", "E78.5", "Cholesterol Check", "185.00", "Aetna HMO",
     "142.50", "42.50", "paid", "NPI-1234567890", "Internal Medicine", ""],
    [19, "PAT-00019", "ENC-2026-0019", "2026-01-20", "2026-01-23",
     "99214", "I10", "Hypertension Management", "225.00", "UHC Choice Plus",
     "178.50", "46.50", "paid", "NPI-1234567890", "Cardiology", ""],
    [20, "PAT-00020", "ENC-2026-0020", "2026-01-20", "2026-01-23",
     "99213", "M79.3", "Knee Pain Eval", "185.00", "Cigna Open Access",
     "142.50", "42.50", "paid", "NPI-9876543210", "Orthopedics", ""],
    [21, "PAT-00021", "ENC-2026-0021", "2026-01-21", "2026-01-24",
     "99214", "G43.909", "Migraine Follow-Up", "225.00", "BlueCross PPO",
     "178.50", "46.50", "paid", "NPI-NEURO-01", "Neurology", ""],
    [22, "PAT-00022", "ENC-2026-0022", "2026-01-21", "2026-01-24",
     "99213", "L70.0", "Acne Treatment", "185.00", "Aetna HMO",
     "142.50", "42.50", "paid", "NPI-DERM-01", "Dermatology", ""],
    [23, "PAT-00023", "ENC-2026-0023", "2026-01-22", "2026-01-25",
     "99395", "Z00.00", "Annual Physical", "275.00", "UHC Choice Plus",
     "275.00", "0.00", "paid", "NPI-1234567890", "Family Medicine", ""],
    [24, "PAT-00024", "ENC-2026-0024", "2026-01-22", "2026-01-25",
     "99214", "N39.0", "UTI Follow-Up", "225.00", "Medicaid",
     "98.00", "0.00", "paid", "NPI-1234567890", "Internal Medicine", ""],
    [25, "PAT-00025", "ENC-2026-0025", "2026-01-23", "2026-01-26",
     "99213", "H10.10", "Conjunctivitis", "185.00", "BlueCross PPO",
     "142.50", "42.50", "paid", "NPI-EYE-001", "Ophthalmology", ""],
]

# ═══════════════════════════════════════════════════════════════
# DATASET 3: INSURANCE CLAIMS REPORT
# ═══════════════════════════════════════════════════════════════

INSURANCE_HEADER = [
    "_row_id", "policy_number", "claim_id", "claim_date", "incident_date",
    "claimant_name", "claim_type", "claim_amount", "deductible", "payout_amount",
    "claim_status", "adjuster_id", "loss_ratio", "premium_paid", "coverage_start",
    "notes"
]

insurance_rows = [
    # --- Row 1: Normal auto claim ---
    [1, "POL-AUTO-2024-00001", "CLM-2026-00001", "2026-01-10", "2026-01-08",
     "John Smith", "auto_collision", "4500.00", "500.00", "4000.00",
     "closed", "ADJ-101", "0.85", "1200.00", "2025-07-01",
     "Rear-end collision at intersection - other driver at fault"],

    # --- Row 2: Claim amount exceeds policy limit ---
    [2, "POL-HOME-2023-00042", "CLM-2026-00002", "2026-01-12", "2026-01-11",
     "Maria Garcia", "homeowner_fire", "850000.00", "2500.00", "500000.00",
     "partial", "ADJ-102", "4.25", "2400.00", "2023-03-15",
     "Payout capped at $500K policy limit. Actual loss $850K"],

    # --- Row 3: Fraudulent claim — flagged but not yet denied ---
    [3, "POL-AUTO-2025-00099", "CLM-2026-00003", "2026-01-13", "2026-01-12",
     "Robert Johnson", "auto_theft", "65000.00", "1000.00", "0.00",
     "under_investigation", "ADJ-SIU-01", "0.00", "3600.00", "2025-01-01",
     "SIU FLAG: Vehicle found 2 miles from claimant home. Inconsistent statements"],

    # --- Row 4: Very old claim — incident 2 years before claim ---
    [4, "POL-HOME-2022-00007", "CLM-2026-00004", "2026-01-14", "2024-03-15",
     "Sarah Williams", "homeowner_water", "22000.00", "1000.00", "0.00",
     "denied", "ADJ-103", "0.00", "1800.00", "2022-06-01",
     "DENIED: Claim filed beyond 1-year statute of limitations"],

    # --- Row 5: $0 claim — informational only ---
    [5, "POL-AUTO-2025-00200", "CLM-2026-00005", "2026-01-15", "2026-01-14",
     "David Brown", "auto_glass", "0.00", "0.00", "0.00",
     "info_only", "ADJ-101", "0.00", "900.00", "2025-06-01",
     "Glass claim filed for tracking - no deductible, handled by Safelite direct"],

    # --- Row 6: Subrogation — we're recovering money FROM the other party ---
    [6, "POL-AUTO-2024-00001", "CLM-2026-00006", "2026-01-16", "2026-01-08",
     "John Smith", "subrogation_recovery", "-4000.00", "0.00", "-4000.00",
     "closed", "ADJ-SUBRO-01", "-0.85", "1200.00", "2025-07-01",
     "Recovery from at-fault driver's insurer - Liberty Mutual"],

    # --- Row 7: Workers comp claim — different regulatory framework ---
    [7, "WC-2025-STATE-FL-00042", "WC-CLM-2026-001", "2026-01-17", "2025-12-20",
     "REDACTED PER WC REGS", "workers_comp_injury", "15000.00", "0.00", "8500.00",
     "open", "ADJ-WC-01", "N/A", "N/A", "2025-01-01",
     "L5-S1 disc herniation - ongoing treatment. Premium = employer-paid"],

    # --- Row 8: Life insurance claim — very different from P&C ---
    [8, "LIFE-2020-TERM-500K", "LIFE-CLM-2026-001", "2026-01-18", "2026-01-15",
     "Estate of James Wilson", "life_death_benefit", "500000.00", "0.00", "500000.00",
     "paid", "ADJ-LIFE-01", "N/A", "125.00", "2020-01-01",
     "Term life - 20yr term. Monthly premium $125. Death cert verified"],

    # --- Row 9: Claim with multiple claimants and split payout ---
    [9, "POL-AUTO-2025-00150", "CLM-2026-00009", "2026-01-19", "2026-01-18",
     "Jennifer Davis & Michael Davis", "auto_collision", "12000.00", "500.00", "11500.00",
     "closed", "ADJ-104", "1.28", "2100.00", "2025-03-01",
     "Both named insureds injured. Split payout: J=$7K, M=$4.5K"],

    # --- Row 10: Catastrophe claim — hurricane-related ---
    [10, "POL-HOME-2024-00500", "CLM-2026-CAT-001", "2026-01-20", "2025-10-15",
     "Thomas Martinez", "homeowner_wind", "175000.00", "10000.00", "165000.00",
     "closed", "ADJ-CAT-TEAM", "2.75", "5000.00", "2024-01-01",
     "CAT EVENT: Hurricane Milton. Separate wind deductible 2% of dwelling coverage"],

    # --- Row 11: Premium refund — negative premium ---
    [11, "POL-AUTO-2025-00300", "ADM-2026-00011", "2026-01-20", "N/A",
     "Lisa Anderson", "premium_refund", "0.00", "0.00", "-450.00",
     "processed", "ADJ-ADMIN", "N/A", "-450.00", "2025-09-01",
     "Policy cancelled mid-term. Pro-rata refund of unearned premium"],

    # --- Row 12: Reinsurance recovery — claim from reinsurer ---
    [12, "TREATY-2025-XOL-001", "RI-2026-00001", "2026-01-21", "2025-10-15",
     "Swiss Re Treaty Recovery", "reinsurance_recovery", "-2500000.00", "0.00", "-2500000.00",
     "closed", "ADJ-RI-TEAM", "N/A", "N/A", "2025-01-01",
     "XOL treaty recovery for Hurricane Milton losses exceeding $5M retention"],

    # --- Row 13: Claim reopened — was closed, now additional damages found ---
    [13, "POL-HOME-2024-00500", "CLM-2026-CAT-001-SUPP", "2026-01-22", "2025-10-15",
     "Thomas Martinez", "homeowner_water", "35000.00", "0.00", "35000.00",
     "open", "ADJ-CAT-TEAM", "0.58", "5000.00", "2024-01-01",
     "SUPPLEMENTAL: Additional water damage found during roof repair. No addl deductible"],

    # --- Row 14: Claim with exact duplicate claim_id (system glitch) ---
    [14, "POL-AUTO-2025-00400", "CLM-2026-00001", "2026-01-22", "2026-01-21",
     "Chris Taylor", "auto_comprehensive", "3500.00", "250.00", "3250.00",
     "closed", "ADJ-105", "0.72", "1500.00", "2025-04-01",
     "Claim ID collision with row 1 - system migration error"],

    # --- Row 15: Very small claim — below adjuster threshold ---
    [15, "POL-AUTO-2025-00450", "CLM-2026-00015", "2026-01-23", "2026-01-22",
     "Amanda White", "auto_glass", "89.99", "0.00", "89.99",
     "auto_approved", "SYSTEM-AUTO", "0.10", "1100.00", "2025-05-01",
     "Auto-approved: Below $100 threshold. No adjuster review needed"],

    # --- Rows 16-25: Clean baseline claims ---
    [16, "POL-AUTO-2025-00500", "CLM-2026-00016", "2026-01-24", "2026-01-23",
     "Kevin Moore", "auto_collision", "3200.00", "500.00", "2700.00",
     "closed", "ADJ-101", "0.56", "1400.00", "2025-07-01", ""],
    [17, "POL-HOME-2024-00600", "CLM-2026-00017", "2026-01-24", "2026-01-22",
     "Nancy Lee", "homeowner_theft", "8500.00", "1000.00", "7500.00",
     "closed", "ADJ-102", "1.50", "2200.00", "2024-08-01", ""],
    [18, "POL-AUTO-2025-00550", "CLM-2026-00018", "2026-01-25", "2026-01-24",
     "Brian Clark", "auto_collision", "5800.00", "500.00", "5300.00",
     "closed", "ADJ-103", "0.88", "1350.00", "2025-02-01", ""],
    [19, "POL-HOME-2024-00700", "CLM-2026-00019", "2026-01-25", "2026-01-20",
     "Dorothy Hall", "homeowner_water", "4200.00", "1000.00", "3200.00",
     "closed", "ADJ-104", "0.64", "2800.00", "2024-05-01", ""],
    [20, "POL-AUTO-2025-00600", "CLM-2026-00020", "2026-01-26", "2026-01-25",
     "Ronald King", "auto_comprehensive", "2100.00", "250.00", "1850.00",
     "closed", "ADJ-105", "0.41", "1500.00", "2025-08-01", ""],
    [21, "POL-HOME-2024-00800", "CLM-2026-00021", "2026-01-26", "2026-01-18",
     "Karen Wright", "homeowner_liability", "15000.00", "0.00", "15000.00",
     "closed", "ADJ-101", "1.88", "3200.00", "2024-09-01", ""],
    [22, "POL-AUTO-2025-00650", "CLM-2026-00022", "2026-01-27", "2026-01-26",
     "Steven Hill", "auto_collision", "7200.00", "1000.00", "6200.00",
     "closed", "ADJ-102", "1.03", "1600.00", "2025-03-01", ""],
    [23, "POL-AUTO-2025-00700", "CLM-2026-00023", "2026-01-27", "2026-01-26",
     "Michelle Green", "auto_collision", "4100.00", "500.00", "3600.00",
     "closed", "ADJ-103", "0.72", "1300.00", "2025-06-01", ""],
    [24, "POL-HOME-2024-00900", "CLM-2026-00024", "2026-01-28", "2026-01-25",
     "Edward Baker", "homeowner_fire", "55000.00", "2500.00", "52500.00",
     "closed", "ADJ-104", "2.19", "4800.00", "2024-04-01", ""],
    [25, "POL-AUTO-2025-00750", "CLM-2026-00025", "2026-01-28", "2026-01-27",
     "Susan Nelson", "auto_glass", "450.00", "0.00", "450.00",
     "closed", "ADJ-105", "0.30", "1200.00", "2025-09-01", ""],
]


def write_dataset(filename, header, rows):
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    size = os.path.getsize(filename)
    print(f"  {filename}: {len(rows)} rows, {size:,} bytes")


print("Generating domain-specific datasets...")
write_dataset("walmart_retail_sales.csv", RETAIL_HEADER, retail_rows)
write_dataset("hospital_billing.csv", HOSPITAL_HEADER, hospital_rows)
write_dataset("insurance_claims.csv", INSURANCE_HEADER, insurance_rows)
print("Done!")
