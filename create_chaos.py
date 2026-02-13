#!/usr/bin/env python3
"""Generate complete_chaos.csv — THE ULTIMATE stress test.

Every single data quality issue we've ever seen, all in one dataset.
200 rows of pure mayhem across 15 columns.
"""
import csv, random, io, os

random.seed(42)

HEADERS = [
    "_row_id", "customer_id", "full_name", "email", "phone",
    "signup_date", "last_login", "country", "is_active",
    "account_balance", "monthly_spend", "discount_rate",
    "satisfaction_score", "notes", "referral_code"
]

rows = []

def R(rid): return rid  # row id helper

# ============================================================
# GROUP 1: UNICODE & ENCODING NIGHTMARES (rows 1-25)
# ============================================================

# 1. Full-width numbers in amounts
rows.append([1, "C001", "Tanaka Yūki", "tanaka@test.jp", "+81-90-1234-5678",
    "2024-01-15", "2024-06-01", "Japan", "Yes",
    "＄１，２３４．５６", "＄５６７．８９", "１５％", "４．５", "Full-width everything", "REF001"])

# 2. RTL marks wrapping numbers
rows.append([2, "C002", "أحمد محمد", "ahmed@test.sa", "+966-50-123-4567",
    "2024-02-20", "2024-07-15", "Saudi Arabia", "TRUE",
    "\u200f$3,500.00\u200f", "\u200f$150.00\u200f", "\u200f10%\u200f", "\u200f4.8\u200f", "RTL marks everywhere", "REF002"])

# 3. Zero-width chars in numbers
rows.append([3, "C003", "Jan Müller", "jan@test.de", "+49-170-1234567",
    "2024-03-10", "2024-08-22", "Germany", "true",
    "€1\u200b,\u200b234\u200b.\u200b56", "€4\u200b5\u200b0", "2\u200b0\u200b%", "3\u200b.\u200b9", "Zero-width joiners", "REF003"])

# 4. NBSP instead of regular spaces
rows.append([4, "C004", "Pierre\u00a0Dupont", "pierre@test.fr", "+33\u00a01\u00a023\u00a045\u00a067",
    "2024-04-05", "2024-09-30", "France", "Oui",
    "EUR\u00a01,500.00", "EUR\u00a0200", "12\u00a0%", "4.2", "NBSP\u00a0everywhere", "REF004"])

# 5. Parenthesized negative (accounting format)
rows.append([5, "C005", "Sarah O'Brien", "sarah@test.ie", "+353-87-123-4567",
    "2024-05-12", "2024-10-15", "Ireland", "yes",
    "(€2,345.67)", "(€150.00)", "0%", "2.1", "Accounting format negatives", "REF005"])

# 6. En-dash and em-dash as minus
rows.append([6, "C006", "Erik Lindström", "erik@test.se", "+46-70-123-4567",
    "2024-06-18", "2024-11-20", "Sweden", "1",
    "–1,000.50", "—75.25", "5%", "–1.5", "Various dashes as minus", "REF006"])

# 7. Mixed currency symbols
rows.append([7, "C007", "Priya Sharma", "priya@test.in", "+91-98765-43210",
    "2024-07-22", "2024-12-01", "India", "Active",
    "₹85,000", "Rs. 5,000", "INR 8%", "4.7", "Multiple Indian currency formats", "REF007"])

# 8. BOM character at start of value
rows.append([8, "C008", "\ufeffJohn Smith", "\ufeffjohn@test.com", "\ufeff+1-555-0100",
    "\ufeff2024-08-01", "2024-12-15", "USA", "\ufeffyes",
    "\ufeff$5,000.00", "\ufeff$250.00", "\ufeff15%", "\ufeff4.0", "BOM in every field", "REF008"])

# 9. NULL bytes embedded
rows.append([9, "C009", "Li\x00Wei", "li@test\x00.cn", "+86-138-0000-0000",
    "2024-09-05", "2025-01-10", "China", "是",
    "¥\x0010,000", "¥\x00500", "3%", "4\x00.5", "NULL bytes scattered", "REF009"])

# 10. Emoji in everything
rows.append([10, "C010", "Maria 🌟 Santos", "maria🎉@test.br", "📱+55-11-99999-0000",
    "2024-10-10", "2025-02-01", "Brazil 🇧🇷", "✅",
    "R$💰2,500.00", "R$💸300.00", "🔥20%", "⭐5.0", "Emoji overload 🚀🔥💯", "REF010"])

# 11. Vertical tab, form feed, carriage return
rows.append([11, "C011", "Test\vUser\f11", "test11@test.com", "+1-555-0111",
    "2024-11-11", "2025-02-10", "USA", "yes\r",
    "$1,111.11\v", "$111.11\f", "11%\r", "3.3\v", "Control chars", "REF011"])

# 12. Combining diacritics (decomposed Unicode)
rows.append([12, "C012", "Jose\u0301 Garci\u0301a", "jose@test.mx", "+52-55-1234-5678",
    "2024-12-01", "2025-02-12", "Me\u0301xico", "si",
    "MXN 15,000", "MXN 800", "10%", "4.1", "Decomposed Unicode", "REF012"])

# 13-15. More encoding edge cases
rows.append([13, "C013", "Ölaf Björk", "olaf@test.is", "+354-555-1234",
    "2023-01-15", "2024-06-30", "Iceland", "já",
    "USD 2,222.22", "USD 333.33", "7.5%", "3.8", "Icelandic chars", "REF013"])

rows.append([14, "C014", "Ωmega Τest", "omega@test.gr", "+30-21-0123-4567",
    "2023-02-28", "2024-07-15", "Greece", "ναι",
    "€3,333.33", "€444.44", "8%", "4.4", "Greek chars", "REF014"])

rows.append([15, "C015", "Владимир Тест", "vlad@test.ru", "+7-495-123-4567",
    "2023-03-31", "2024-08-20", "Russia", "да",
    "USD 4,444.44", "USD 555.55", "6%", "3.6", "Cyrillic chars", "REF015"])

# 16-20. More full-width/halfwidth variants
rows.append([16, "C016", "ＦｕｌｌＷｉｄｔｈ Ｎａｍｅ", "fw@test.com", "＋１－５５５－０１１６",
    "２０２３－０４－１５", "2024-09-01", "ＵＳＡ", "ｙｅｓ",
    "＄９，９９９．９９", "＄１，１１１", "２５％", "４．９", "Full-width EVERYTHING", "REF016"])

rows.append([17, "C017", "Test Seventeen", "t17@test.com", "+1-555-0117",
    "2023-05-20", "2024-09-15", "USA", "YES",
    "AUD 1,750.50", "AUD 200", "15%", "3.5", "Australian dollars", "REF017"])

rows.append([18, "C018", "Test Eighteen", "t18@test.com", "+1-555-0118",
    "2023-06-25", "2024-10-01", "Canada", "Y",
    "CAD 2,100.00", "CAD 350", "12%", "4.0", "Canadian dollars", "REF018"])

rows.append([19, "C019", "Test GBP", "t19@test.co.uk", "+44-20-7946-0958",
    "2023-07-30", "2024-10-15", "UK", "True",
    "GBP 3,200.50", "GBP 425.75", "18%", "4.6", "British pounds word", "REF019"])

rows.append([20, "C020", "Test CHF", "t20@test.ch", "+41-44-123-4567",
    "2023-08-15", "2024-11-01", "Switzerland", "Ja",
    "CHF 5,500.00", "CHF 600", "9%", "4.3", "Swiss francs", "REF020"])

# 21-25. Currency suffix (number first)
rows.append([21, "C021", "Test Suffix1", "t21@test.com", "+1-555-0121",
    "2023-09-10", "2024-11-15", "Japan", "はい",
    "50000 JPY", "5000 JPY", "5%", "3.7", "Currency suffix", "REF021"])

rows.append([22, "C022", "Test Suffix2", "t22@test.com", "+82-10-1234-5678",
    "2023-10-05", "2024-12-01", "South Korea", "예",
    "1500000 KRW", "150000 KRW", "3%", "4.1", "Korean won suffix", "REF022"])

rows.append([23, "C023", "Test SGD", "t23@test.sg", "+65-9123-4567",
    "2023-11-20", "2024-12-15", "Singapore", "yes",
    "SGD 4,200.00", "SGD 500", "10%", "4.5", "Singapore dollars", "REF023"])

rows.append([24, "C024", "Test NZD", "t24@test.nz", "+64-21-123-4567",
    "2023-12-25", "2025-01-01", "New Zealand", "yes",
    "NZD 2,800.00", "NZD 350", "8%", "4.2", "NZ dollars", "REF024"])

rows.append([25, "C025", "Test HKD", "t25@test.hk", "+852-9123-4567",
    "2024-01-01", "2025-01-15", "Hong Kong", "yes",
    "HKD 25,000.00", "HKD 3,000", "6%", "3.9", "HK dollars", "REF025"])

# ============================================================
# GROUP 2: DATE CHAOS (rows 26-50)
# ============================================================

# 26. ISO format (normal baseline)
rows.append([26, "C026", "Date Normal", "dn@test.com", "+1-555-0126",
    "2024-01-15", "2024-06-01", "USA", "yes",
    "$1,000", "$100", "5%", "4.0", "Normal dates", "REF026"])

# 27. US format MM/DD/YYYY
rows.append([27, "C027", "Date US", "dus@test.com", "+1-555-0127",
    "01/15/2024", "06/01/2024", "USA", "yes",
    "$1,100", "$110", "6%", "4.1", "US date format", "REF027"])

# 28. European DD/MM/YYYY
rows.append([28, "C028", "Date EU", "deu@test.com", "+44-20-7946-0128",
    "15/01/2024", "01/06/2024", "UK", "yes",
    "£1,200", "£120", "7%", "4.2", "European dates", "REF028"])

# 29. Abbreviated month
rows.append([29, "C029", "Date Abbrev", "da@test.com", "+1-555-0129",
    "Jan 15, 2024", "Jun 1, 2024", "USA", "yes",
    "$1,300", "$130", "8%", "4.3", "Abbreviated month names", "REF029"])

# 30. Full month name
rows.append([30, "C030", "Date Full", "df@test.com", "+1-555-0130",
    "January 15, 2024", "June 1, 2024", "USA", "yes",
    "$1,400", "$140", "9%", "4.4", "Full month names", "REF030"])

# 31. Unix epoch timestamp
rows.append([31, "C031", "Date Epoch", "de@test.com", "+1-555-0131",
    "1705276800", "1717200000", "USA", "yes",
    "$1,500", "$150", "10%", "4.5", "Unix timestamps", "REF031"])

# 32. ISO with timezone
rows.append([32, "C032", "Date TZ", "dtz@test.com", "+1-555-0132",
    "2024-01-15T10:30:00+05:30", "2024-06-01T08:00:00-07:00", "USA", "yes",
    "$1,600", "$160", "11%", "4.6", "ISO with timezone", "REF032"])

# 33. Year 9999 (overflow)
rows.append([33, "C033", "Date Overflow", "do@test.com", "+1-555-0133",
    "9999-12-31", "9999-12-31", "USA", "yes",
    "$1,700", "$170", "12%", "4.7", "Year 9999 overflow", "REF033"])

# 34. Feb 29 non-leap year
rows.append([34, "C034", "Date BadLeap", "dbl@test.com", "+1-555-0134",
    "2023-02-29", "2024-02-29", "USA", "yes",
    "$1,800", "$180", "13%", "4.8", "Feb 29 in non-leap year", "REF034"])

# 35. Month 13
rows.append([35, "C035", "Date Month13", "dm@test.com", "+1-555-0135",
    "2024-13-01", "2024-00-15", "USA", "yes",
    "$1,900", "$190", "14%", "4.9", "Impossible months", "REF035"])

# 36. Day 32
rows.append([36, "C036", "Date Day32", "dd@test.com", "+1-555-0136",
    "2024-01-32", "2024-06-31", "USA", "yes",
    "$2,000", "$200", "15%", "5.0", "Impossible days", "REF036"])

# 37. Just year
rows.append([37, "C037", "Date YearOnly", "dy@test.com", "+1-555-0137",
    "2024", "2025", "USA", "yes",
    "$2,100", "$210", "16%", "3.0", "Year only", "REF037"])

# 38. Relative date strings
rows.append([38, "C038", "Date Relative", "dr@test.com", "+1-555-0138",
    "yesterday", "last week", "USA", "yes",
    "$2,200", "$220", "17%", "3.1", "Relative date strings", "REF038"])

# 39. DD-Mon-YY format
rows.append([39, "C039", "Date DDMonYY", "dmyy@test.com", "+1-555-0139",
    "15-Jan-24", "01-Jun-24", "USA", "yes",
    "$2,300", "$230", "18%", "3.2", "DD-Mon-YY format", "REF039"])

# 40. Slash-separated no century
rows.append([40, "C040", "Date NoYYYY", "dny@test.com", "+1-555-0140",
    "1/15/24", "6/1/24", "USA", "yes",
    "$2,400", "$240", "19%", "3.3", "Two-digit year", "REF040"])

# 41. Date with dots
rows.append([41, "C041", "Date Dots", "ddots@test.com", "+49-170-0000141",
    "15.01.2024", "01.06.2024", "Germany", "yes",
    "€2,500", "€250", "20%", "3.4", "Dot-separated dates", "REF041"])

# 42-45: More date edge cases
rows.append([42, "C042", "Date Empty", "demp@test.com", "+1-555-0142",
    "", "", "USA", "yes",
    "$2,600", "$260", "5%", "3.5", "Empty dates", "REF042"])

rows.append([43, "C043", "Date NULL", "dnl@test.com", "+1-555-0143",
    "NULL", "N/A", "USA", "yes",
    "$2,700", "$270", "6%", "3.6", "NULL/NA dates", "REF043"])

rows.append([44, "C044", "Date Spaces", "dsp@test.com", "+1-555-0144",
    "   2024-03-15   ", "  2024-08-01  ", "USA", "yes",
    "$2,800", "$280", "7%", "3.7", "Extra whitespace dates", "REF044"])

rows.append([45, "C045", "Date Mixed", "dmx@test.com", "+1-555-0145",
    "2024/03/15", "2024.08.01", "USA", "yes",
    "$2,900", "$290", "8%", "3.8", "Mixed separators", "REF045"])

# 46-50: Temporal paradoxes
rows.append([46, "C046", "Paradox Future", "pf@test.com", "+1-555-0146",
    "2030-01-01", "2020-01-01", "USA", "yes",
    "$3,000", "$300", "9%", "4.0", "Signup in future, login in past", "REF046"])

rows.append([47, "C047", "Paradox Same", "ps@test.com", "+1-555-0147",
    "2024-06-15", "2024-06-15", "USA", "yes",
    "$3,100", "$310", "10%", "4.1", "Same day signup and login", "REF047"])

rows.append([48, "C048", "Paradox Ancient", "pa@test.com", "+1-555-0148",
    "1900-01-01", "1899-12-31", "USA", "yes",
    "$3,200", "$320", "11%", "4.2", "Ancient dates", "REF048"])

rows.append([49, "C049", "Paradox Swapped", "psw@test.com", "+1-555-0149",
    "2024-12-31", "2024-01-01", "USA", "yes",
    "$3,300", "$330", "12%", "4.3", "Login before signup", "REF049"])

rows.append([50, "C050", "Date SQL", "dsql@test.com", "+1-555-0150",
    "0000-00-00", "0000-00-00 00:00:00", "USA", "yes",
    "$3,400", "$340", "13%", "4.4", "SQL zero dates", "REF050"])

# ============================================================
# GROUP 3: NUMERIC NIGHTMARES (rows 51-80)
# ============================================================

# 51. NaN string
rows.append([51, "C051", "Num NaN", "nn@test.com", "+1-555-0151",
    "2024-01-01", "2024-06-01", "USA", "yes",
    "NaN", "NaN", "NaN", "NaN", "NaN everywhere", "REF051"])

# 52. Infinity
rows.append([52, "C052", "Num Inf", "ni@test.com", "+1-555-0152",
    "2024-01-02", "2024-06-02", "USA", "yes",
    "Infinity", "-Infinity", "inf", "-inf", "Infinity values", "REF052"])

# 53. Scientific notation
rows.append([53, "C053", "Num SciNot", "ns@test.com", "+1-555-0153",
    "2024-01-03", "2024-06-03", "USA", "yes",
    "1.5e4", "2.5e2", "1.5e1", "4.0e0", "Scientific notation", "REF053"])

# 54. Leading/trailing zeros
rows.append([54, "C054", "Num Zeros", "nz@test.com", "+1-555-0154",
    "2024-01-04", "2024-06-04", "USA", "yes",
    "007500.00", "00250.00", "005%", "04.5", "Leading zeros", "REF054"])

# 55. Negative zero
rows.append([55, "C055", "Num NegZero", "nzo@test.com", "+1-555-0155",
    "2024-01-05", "2024-06-05", "USA", "yes",
    "-0", "-0.00", "-0%", "-0.0", "Negative zeros", "REF055"])

# 56. MAX values
rows.append([56, "C056", "Num MAX", "nmax@test.com", "+1-555-0156",
    "2024-01-06", "2024-06-06", "USA", "yes",
    "99999999999999999999.99", "99999999999.99", "999%", "999.99", "Extreme maximums", "REF056"])

# 57. Tiny values
rows.append([57, "C057", "Num Tiny", "ntiny@test.com", "+1-555-0157",
    "2024-01-07", "2024-06-07", "USA", "yes",
    "0.000000001", "0.000000001", "0.000001%", "0.001", "Near-epsilon values", "REF057"])

# 58. Comma as decimal (European)
rows.append([58, "C058", "Num EurDecimal", "ned@test.com", "+33-1-23-45-67-58",
    "2024-01-08", "2024-06-08", "France", "yes",
    "1.234,56", "567,89", "15,5%", "4,2", "European decimal commas", "REF058"])

# 59. Space as thousands separator
rows.append([59, "C059", "Num SpaceThous", "nst@test.com", "+33-1-23-45-67-59",
    "2024-01-09", "2024-06-09", "France", "yes",
    "1 234 567.89", "12 345.67", "15%", "4.0", "Space thousands", "REF059"])

# 60. Percentage > 100
rows.append([60, "C060", "Num PctOver", "npo@test.com", "+1-555-0160",
    "2024-01-10", "2024-06-10", "USA", "yes",
    "$5,000", "$500", "250%", "4.5", "Discount > 100%", "REF060"])

# 61. Formula injection
rows.append([61, "C061", "Num Formula", "nf@test.com", "+1-555-0161",
    "2024-01-11", "2024-06-11", "USA", "yes",
    "=1+1", "=SUM(A1:A5)", "=100*2%", "=4+0.5", "Excel formula injection", "REF061"])

# 62. SQL injection in numeric
rows.append([62, "C062", "Num SQLInj", "nsql@test.com", "+1-555-0162",
    "2024-01-12", "2024-06-12", "USA", "yes",
    "1000; DROP TABLE users", "500 OR 1=1", "10%", "4.0", "SQL injection attempts", "REF062"])

# 63. Currency with no number
rows.append([63, "C063", "Num NoAmount", "nna@test.com", "+1-555-0163",
    "2024-01-13", "2024-06-13", "USA", "yes",
    "$", "€", "%", "", "Currency symbol with no number", "REF063"])

# 64. Multiple decimal points
rows.append([64, "C064", "Num MultiDot", "nmd@test.com", "+1-555-0164",
    "2024-01-14", "2024-06-14", "USA", "yes",
    "1.234.567", "12.34.56", "1.5.0%", "4.5.0", "Multiple decimals", "REF064"])

# 65. Fractions
rows.append([65, "C065", "Num Frac", "nfr@test.com", "+1-555-0165",
    "2024-01-15", "2024-06-15", "USA", "yes",
    "1/2", "3/4", "50%", "4 1/2", "Fraction notation", "REF065"])

# 66. Words as numbers
rows.append([66, "C066", "Num Words", "nw@test.com", "+1-555-0166",
    "2024-01-16", "2024-06-16", "USA", "yes",
    "one thousand", "five hundred", "ten percent", "four point five", "Written out numbers", "REF066"])

# 67. Mixed text and numbers
rows.append([67, "C067", "Num Mixed", "nmx@test.com", "+1-555-0167",
    "2024-01-17", "2024-06-17", "USA", "yes",
    "about $1,000", "~$500", "approx 10%", "~4.5", "Approximate values", "REF067"])

# 68. Pipe-delimited within CSV
rows.append([68, "C068", "Num Pipe", "np@test.com", "+1-555-0168",
    "2024-01-18", "2024-06-18", "USA", "yes",
    "1000|2000|3000", "500|600", "10%|20%", "4.5|3.5", "Pipe-delimited values", "REF068"])

# 69-75. More numeric chaos
rows.append([69, "C069", "Num Hex", "nh@test.com", "+1-555-0169",
    "2024-01-19", "2024-06-19", "USA", "yes",
    "0xFF", "0x1A", "0x0A%", "0x04", "Hex values", "REF069"])

rows.append([70, "C070", "Num Binary", "nb@test.com", "+1-555-0170",
    "2024-01-20", "2024-06-20", "USA", "yes",
    "0b1111101000", "0b111110100", "0b1010%", "0b100", "Binary values", "REF070"])

rows.append([71, "C071", "Num Roman", "nr@test.com", "+1-555-0171",
    "2024-01-21", "2024-06-21", "USA", "yes",
    "M", "D", "X%", "IV", "Roman numerals", "REF071"])

rows.append([72, "C072", "Num Negative$", "nnd@test.com", "+1-555-0172",
    "2024-01-22", "2024-06-22", "USA", "yes",
    "-$1,234.56", "-$567.89", "-15%", "-3.5", "Negative with $ prefix", "REF072"])

rows.append([73, "C073", "Num ParenUSD", "npu@test.com", "+1-555-0173",
    "2024-01-23", "2024-06-23", "USA", "yes",
    "($1,234.56)", "(USD 567.89)", "(15%)", "(3.5)", "Parenthesized with currency", "REF073"])

rows.append([74, "C074", "Num Lakh", "nlk@test.com", "+91-98765-00074",
    "2024-01-24", "2024-06-24", "India", "yes",
    "₹12,34,567.89", "Rs 1,23,456", "10%", "4.5", "Indian lakh format", "REF074"])

rows.append([75, "C075", "Num WhiteNoise", "nwn@test.com", "+1-555-0175",
    "2024-01-25", "2024-06-25", "USA", "yes",
    "  $ 1 , 2 3 4 . 5 6  ", "  $ 5 6 7  ", "  1 5 %  ", "  4 . 5  ", "Excessive spacing", "REF075"])

# 76-80. Boundary values
rows.append([76, "C076", "Num ZeroAll", "nza@test.com", "+1-555-0176",
    "2024-01-26", "2024-06-26", "USA", "yes",
    "$0.00", "$0.00", "0%", "0.0", "All zeros", "REF076"])

rows.append([77, "C077", "Num OneCent", "noc@test.com", "+1-555-0177",
    "2024-01-27", "2024-06-27", "USA", "yes",
    "$0.01", "$0.01", "0.01%", "0.1", "Tiny but valid", "REF077"])

rows.append([78, "C078", "Num PlusSign", "nps@test.com", "+1-555-0178",
    "2024-01-28", "2024-06-28", "USA", "yes",
    "+$1,000.00", "+500", "+10%", "+4.5", "Explicit plus signs", "REF078"])

rows.append([79, "C079", "Num TrailDot", "ntd@test.com", "+1-555-0179",
    "2024-01-29", "2024-06-29", "USA", "yes",
    "1000.", "500.", "10.%", "4.", "Trailing decimals no digits", "REF079"])

rows.append([80, "C080", "Num LeadDot", "nld@test.com", "+1-555-0180",
    "2024-01-30", "2024-06-30", "USA", "yes",
    ".99", ".50", ".10%", ".5", "Leading dot no zero", "REF080"])

# ============================================================
# GROUP 4: BOOLEAN CHAOS (rows 81-100)
# ============================================================
bool_values = [
    "yes", "no", "YES", "NO", "Yes", "No", "Y", "N", "y", "n",
    "true", "false", "TRUE", "FALSE", "True", "False", "T", "F",
    "1", "0", "1.0", "0.0", "on", "off", "ON", "OFF",
    "active", "inactive", "ACTIVE", "INACTIVE", "Active", "Inactive",
    "enabled", "disabled", "si", "ja", "oui", "non",
    "да", "нет", "はい", "いいえ", "是", "否",
    "✅", "❌", "👍", "👎", "positive", "negative",
    "", "NULL", "N/A", "n/a", "None", "none", "nil",
    "maybe", "unknown", "TBD", "pending", "-", "?",
    "yep", "nope", "yeah", "nah", "affirmative", "negative"
]

for i in range(81, 101):
    bv = bool_values[(i-81) % len(bool_values)]
    rows.append([i, f"C{i:03d}", f"Bool Test {i-80}", f"bt{i-80}@test.com", f"+1-555-{i:04d}",
        "2024-02-01", "2025-01-01", "USA", bv,
        f"${(i-80)*100}", f"${(i-80)*10}", f"{(i-80)}%", f"{2.0 + (i-80)*0.1:.1f}", f"Boolean={bv}", f"REF{i:03d}"])

# ============================================================
# GROUP 5: MISSING DATA PATTERNS (rows 101-130)
# ============================================================
missing_variants = [
    "", " ", "  ", "\t", "\n", "NULL", "null", "Null",
    "N/A", "n/a", "NA", "na", "N.A.", "N.A",
    "None", "none", "NONE", "nil", "NIL",
    "-", "--", "---", ".", "..", "...",
    "missing", "MISSING", "Missing",
    "unknown", "UNKNOWN", "Unknown",
    "not available", "NOT AVAILABLE",
    "#N/A", "#REF!", "#VALUE!", "#NULL!", "#DIV/0!",
    "undefined", "UNDEFINED", "void", "VOID",
    "?", "??", "???", "TBD", "tbd",
    "0", "0.0", "0.00",
    "-999", "-9999", "99999",
    "not applicable", "redacted", "REDACTED", "[REDACTED]",
    "classified", "withheld", "restricted",
    "pending", "PENDING", "awaiting",
    "error", "ERROR", "#ERROR", "ERR",
    "nan", "NAN", "NaN", " nan ", " NaN "
]

for i in range(101, 131):
    idx = (i - 101) % len(missing_variants)
    mv = missing_variants[idx]
    # Scatter the missing value across different columns each row
    col_idx = (i - 101) % 4
    bal = "$5,000" if col_idx != 0 else mv
    spend = "$500" if col_idx != 1 else mv
    disc = "10%" if col_idx != 2 else mv
    score = "4.0" if col_idx != 3 else mv

    rows.append([i, f"C{i:03d}", f"Missing Test {i-100}", f"mt{i-100}@test.com", f"+1-555-{i:04d}",
        "2024-03-01", "2025-01-15", "USA", "yes",
        bal, spend, disc, score, f"Missing variant: [{mv}]", f"REF{i:03d}"])

# ============================================================
# GROUP 6: INJECTION & STRUCTURAL CHAOS (rows 131-160)
# ============================================================

# CSV injection
rows.append([131, "C131", 'Name","injected_col","more', "inj@test.com", "+1-555-0131",
    "2024-04-01", "2025-02-01", "USA", "yes",
    "$1,000", "$100", "5%", "4.0", "CSV column injection attempt", "REF131"])

# JSON in fields
rows.append([132, "C132", '{"name":"json_test"}', "json@test.com", "+1-555-0132",
    "2024-04-02", "2025-02-02", "USA", "yes",
    '{"amount":1000}', '{"spend":100}', "5%", "4.0", '{"note":"json"}', "REF132"])

# HTML tags
rows.append([133, "C133", "<b>Bold Name</b>", "html@test.com", "+1-555-0133",
    "2024-04-03", "2025-02-03", "USA", "yes",
    "<span>$1,000</span>", "$100", "5%", "4.0", "<script>alert('xss')</script>", "REF133"])

# SQL injection in text
rows.append([134, "C134", "Robert'; DROP TABLE customers;--", "sql@test.com", "+1-555-0134",
    "2024-04-04", "2025-02-04", "USA", "yes",
    "1000 UNION SELECT * FROM passwords", "$100", "5%", "4.0", "SQL injection", "REF134"])

# Extremely long value
rows.append([135, "C135", "A" * 500, "long@test.com", "+1-555-0135",
    "2024-04-05", "2025-02-05", "USA", "yes",
    "$1,000", "$100", "5%", "4.0", "X" * 1000, "REF135"])

# Newlines in field
rows.append([136, "C136", "Line1\nLine2\nLine3", "nl@test.com", "+1-555-0136",
    "2024-04-06", "2025-02-06", "USA", "yes",
    "$1,000", "$100", "5%", "4.0", "Multi-line\nnotes\nfield", "REF136"])

# Tab-separated values embedded
rows.append([137, "C137", "Tab\tSeparated\tName", "tab@test.com", "+1-555-0137",
    "2024-04-07", "2025-02-07", "USA", "yes",
    "1000\t2000", "$100", "5%", "4.0", "Tabs\tin\tfields", "REF137"])

# YAML in field
rows.append([138, "C138", "name: yaml_test", "yaml@test.com", "+1-555-0138",
    "2024-04-08", "2025-02-08", "USA", "yes",
    "amount: 1000", "$100", "5%", "4.0", "key: value", "REF138"])

# URL as a value
rows.append([139, "C139", "https://evil.com/steal?data=true", "url@test.com", "+1-555-0139",
    "2024-04-09", "2025-02-09", "USA", "yes",
    "$1,000", "$100", "5%", "4.0", "https://legit.com/notes", "REF139"])

# Path traversal
rows.append([140, "C140", "../../etc/passwd", "path@test.com", "+1-555-0140",
    "2024-04-10", "2025-02-10", "USA", "yes",
    "$1,000", "$100", "5%", "4.0", "../../root/.ssh/id_rsa", "REF140"])

# 141-160: More structural chaos
for i in range(141, 161):
    inject_types = [
        "CRLF\r\nInjection",
        "Back\\slash\\test",
        "Single'Quote",
        'Double"Quote',
        "Semicolon;Here",
        "Pipe|Value",
        "Ampersand&Test",
        "At@Sign",
        "Hash#Tag",
        "Dollar$Sign",
        "Percent%Age",
        "Caret^Up",
        "Star*Wild",
        "Question?Mark",
        "Exclaim!Bang",
        "Tilde~Wave",
        "Backtick`Grave",
        "Bracket[Open]",
        "Paren(Open)",
        "Brace{Open}",
    ]
    inj = inject_types[(i-141) % len(inject_types)]
    rows.append([i, f"C{i:03d}", inj, f"struct{i-140}@test.com", f"+1-555-{i:04d}",
        "2024-05-01", "2025-02-12", "USA", "yes",
        "$1,000", "$100", "5%", "4.0", f"Special char: {inj}", f"REF{i:03d}"])

# ============================================================
# GROUP 7: DUPLICATE & IDENTITY CHAOS (rows 161-180)
# ============================================================

# Exact duplicates
for i in range(161, 166):
    rows.append([i, "C161", "Exact Duplicate", "dup@test.com", "+1-555-0161",
        "2024-06-01", "2025-02-12", "USA", "yes",
        "$5,000", "$500", "10%", "4.5", "Exact duplicate row", "REF161"])

# Near-duplicates (case variations)
rows.append([166, "C166", "john doe", "JOHN@TEST.COM", "+1-555-0166",
    "2024-06-15", "2025-02-12", "USA", "yes",
    "$3,000", "$300", "8%", "4.2", "Lowercase name", "REF166"])
rows.append([167, "C167", "JOHN DOE", "john@test.com", "+1-555-0167",
    "2024-06-15", "2025-02-12", "USA", "YES",
    "$3,000", "$300", "8%", "4.2", "Uppercase name", "REF167"])
rows.append([168, "C168", "John Doe", "John@Test.Com", "+1-555-0168",
    "2024-06-15", "2025-02-12", "USA", "Yes",
    "$3,000", "$300", "8%", "4.2", "Title case name", "REF168"])

# UUID-style IDs
rows.append([169, "C169", "UUID Test", "uuid@test.com", "+1-555-0169",
    "2024-06-20", "2025-02-12", "USA", "yes",
    "$4,000", "$400", "9%", "4.3", "Normal ID", "550e8400-e29b-41d4-a716-446655440000"])

# Same customer different formats
rows.append([170, "C001", "Tanaka Yuki", "tanaka@test.jp", "+819012345678",
    "2024-01-15", "2025-01-01", "JP", "1",
    "¥150000", "¥5000", "10%", "4.5", "Same as row 1, different format", "REF170"])

# 171-180: Identity edge cases
for i in range(171, 181):
    rows.append([i, f" C{i:03d} ", f"  Whitespace  ID  {i}  ", f"ws{i}@test.com", f"+1-555-{i:04d}",
        "2024-07-01", "2025-02-12", "USA", "yes",
        "$2,000", "$200", "7%", "4.0", f"Leading/trailing whitespace ID", f"REF{i:03d}"])

# ============================================================
# GROUP 8: CLEAN BASELINE (rows 181-200)
# ============================================================
for i in range(181, 201):
    bal = round(random.uniform(500, 50000), 2)
    spend = round(random.uniform(50, 5000), 2)
    disc = random.choice([5, 10, 15, 20, 25])
    score = round(random.uniform(2.0, 5.0), 1)
    rows.append([i, f"C{i:03d}", f"Clean User {i-180}", f"clean{i-180}@example.com", f"+1-555-{i:04d}",
        f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        f"2025-{random.randint(1,2):02d}-{random.randint(1,28):02d}",
        random.choice(["USA","UK","Canada","Australia","Germany"]),
        random.choice(["yes","no"]),
        f"${bal:,.2f}", f"${spend:,.2f}", f"{disc}%", str(score),
        "Clean baseline data", f"REF{i:03d}"])


# ============== WRITE ==============
with open("complete_chaos.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    w.writerow(HEADERS)
    for row in rows:
        w.writerow(row)

print(f"Wrote {len(rows)} rows to complete_chaos.csv")
print(f"File size: {os.path.getsize('complete_chaos.csv')} bytes")
print(f"\nGroups:")
print(f"  1. Unicode & Encoding Nightmares:    rows 1-25")
print(f"  2. Date Chaos:                       rows 26-50")
print(f"  3. Numeric Nightmares:               rows 51-80")
print(f"  4. Boolean Chaos:                    rows 81-100")
print(f"  5. Missing Data Patterns:            rows 101-130")
print(f"  6. Injection & Structural Chaos:     rows 131-160")
print(f"  7. Duplicate & Identity Chaos:       rows 161-180")
print(f"  8. Clean Baseline:                   rows 181-200")
