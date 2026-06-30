# Project 4 — World Cup 2026 Data Pipeline

Pipeline dữ liệu end-to-end cho FIFA World Cup 2026 (11/6 – 19/7/2026), từ thu thập qua API đến dashboard BI, theo kiến trúc Medallion.

**Mục tiêu:** Portfolio xin việc Data Engineer

## Architecture

```
API-Football ──┐
football-data ─┼──► Ingestion (Python) ──► Bronze (raw JSON)
GitHub data ───┘         │
                         ▼
                  dbt (Silver: cleaned)
                         │
                         ▼
                  dbt (Gold: star schema + KPI)
                         │
                         ▼
                  Power BI / Superset (Dashboard)
```

**Orchestration:** Apache Airflow (scheduled daily, retry, alert)
**Deployment:** VPS (remote server) — truy cập database & Airflow từ bất kỳ đâu

## Tech Stack

| Thành phần | Công nghệ |
|---|---|
| Ingestion | Python + requests |
| Storage | MinIO / Parquet trên disk |
| Transformation | dbt + DuckDB |
| Orchestration | Apache Airflow |
| Visualization | Power BI Desktop / Superset |
| Deployment | VPS + Docker Compose |

## Data Model (Galaxy Schema)

**Fact tables (4):**
- `fact_lineup` — trước trận (grain: cầu thủ/trận)
- `fact_match_result` — kết quả trận (grain: trận)
- `fact_match_stats` — thống kê đội/trận (grain: đội/trận)
- `fact_player_stats` — thống kê cầu thủ/trận (grain: cầu thủ/trận)

**Dimension tables (8):**
dim_team, dim_player, dim_match, dim_stadium, dim_date, dim_stage, dim_referee, dim_country

## Data Sources

| Nguồn | Vai trò | Free tier |
|---|---|---|
| API-Football (API-SPORTS) | Nguồn chính | ~100 req/ngày |
| football-data.org | Đối chiếu chất lượng | Free forever |
| GitHub worldcup2026 | Cấu trúc giải | Open-source |

## Project Progress

### Phase 1 — Ingestion (Bronze)
- [ ] Setup API-Football client
- [ ] Script lấy fixtures, lineups, statistics, players
- [ ] Lưu raw JSON vào Bronze layer
- [ ] Multi-source ingestion (3 nguồn)

### Phase 2 — Incremental Loading
- [ ] Watermark control table
- [ ] Logic chỉ lấy trận mới mỗi ngày
- [ ] Xử lý trận đang diễn ra vs đã kết thúc

### Phase 3 — Transformation (Silver)
- [ ] dbt staging models (làm sạch, chuẩn hóa)
- [ ] Khử trùng lặp, ép kiểu
- [ ] Data quality tests (dbt test)

### Phase 4 — Modeling (Gold)
- [ ] Dựng galaxy schema (4 facts + 8 dims)
- [ ] Tính KPI: tỷ lệ thắng, hiệu suất dứt điểm, kiểm soát bóng...
- [ ] Marts cho dashboard

### Phase 5 — Orchestration
- [ ] Airflow DAGs chạy theo lịch
- [ ] Retry logic + alerting
- [ ] Deploy trên VPS

### Phase 6 — Visualization
- [ ] Dashboard: BXH, top scorer, team comparison
- [ ] So sánh vòng bảng vs knockout

### Phase 7 — Hoàn thiện
- [ ] README + sơ đồ kiến trúc
- [ ] Push GitHub
- [ ] CI chạy dbt test

## Deployment Notes

**VPS Setup:**
- Deploy toàn bộ pipeline trên VPS (không phụ thuộc máy local)
- Truy cập database từ bất kỳ đâu
- Airflow webserver accessible qua IP/domain
- Docker Compose để quản lý services

## Điểm nhấn Portfolio

1. **Incremental loading thật** — watermark, không full reload
2. **Data quality test tự động** — dbt test
3. **Orchestration có schedule/retry/alert** — Airflow
4. **Multi-source ingestion** — 3 nguồn dữ liệu
5. **VPS deployment** — production-like environment
