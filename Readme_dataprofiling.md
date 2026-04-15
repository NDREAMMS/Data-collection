# Data Profiling Report

## Dataset Overview

- **Records:** 2,246
- **Columns:** 8
- **Key Issue:** 37% missing data in host response fields

---

## Column Profiles

### `id`
- **Type:** Integer | **Missing:** 0% | **Unique:** 2,246 (100%)
- Later records contain anomalously large 18-digit IDs — possible format change or concatenation errors

### `calculated_host_listings_count`
- **Type:** Integer | **Missing:** 0%
- **Range:** 1–816 | **Median:** 1 | **Mean:** ~30.5
- ~57% of hosts have exactly 1 listing; heavily right-skewed
- Value 816 appears ~45 times (single large property manager)
- Other frequent commercial operators: 370, 216, 204, 191, 184, 170, 141

### `availability_365`
- **Type:** Integer | **Missing:** 0%
- **Range:** 0–365 | **Median:** ~179
- ~21% have zero availability (inactive/blocked/fully booked)
- Bimodal: clusters at 0 and 300+

### `host_response_rate_num`
- **Type:** Float | **Missing:** ~835 (37%)
- **Range:** 0.0–100.0 | **Median:** 100.0
- ~65% of non-null values are exactly 100.0
- Missingness perfectly aligned with `host_response_time_code`

### `room_type_code`
- **Type:** Categorical | **Missing:** 0% | **Values:** {1, 2, 3}

| Code | Count | % |
|------|-------|---|
| 2 (Entire home) | ~1,740 | 77.5% |
| 1 (Private room) | ~400 | 17.8% |
| 3 (Shared/Hotel) | ~106 | 4.7% |

### `host_response_time_code`
- **Type:** Ordinal | **Missing:** ~835 (37%)
- **Values:** {0=within hour, 1=few hours, 2=within day, 3=few days}
- ~62% of non-null = 0 (fastest)
- Strong negative correlation with response rate

### `standardization_score`
- **Type:** Ordinal | **Missing:** 0% | **Values:** {-1, 0, 1}
- Distribution: -1 (37%), 0 (28%), 1 (35%)

### `neighborhood_impact_score`
- **Type:** Ordinal | **Missing:** 0% | **Values:** {-1, 0, 1}
- Distribution: -1 (37%), 0 (30%), 1 (33%)

---

## Key Cross-Column Findings

| Finding | Detail |
|---------|--------|
| **Missing data pattern** | `host_response_rate_num` and `host_response_time_code` always missing together (835 records) |
| **Score correlation** | `standardization_score` and `neighborhood_impact_score` agree ~67% of the time; strong positive correlation |
| **Response consistency** | Faster response time codes correlate with higher response rates |
| **Commercial hosts** | High listing counts (141+) tend to have non-null response data and consistent rates (87–100%) |
| **Zero availability** | ~65% of zero-availability listings also have missing response data |

---

## Data Quality Issues

| Issue | Severity | Detail |
|-------|----------|--------|
| High missingness | 🔴 HIGH | 37% missing in two response columns |
| Outlier concentration | 🟡 MEDIUM | 816-count host = ~45 records; can skew analysis |
| ID format anomalies | 🟡 MEDIUM | ~15+ IDs with 18+ digits may be malformed |
| Zero-availability ambiguity | 🟡 MEDIUM | ~480 records — unclear if inactive vs. fully booked |
| Score disagreement | 🟢 LOW | ~5% of records have opposing extreme scores (-1 vs 1) |

---

## Recommendations

1. **Missing data:** Decide on imputation strategy or separate modeling for records without response data
2. **Host segmentation:** Analyze individual (count=1) vs. commercial hosts separately
3. **ID validation:** Verify large-format IDs for correctness
4. **Zero-availability filtering:** Clarify business meaning before inclusion/exclusion
5. **Outlier handling:** Consider capping or separate treatment for 816-listing hostgi