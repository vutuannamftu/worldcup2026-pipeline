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

> Cập nhật lần cuối: 2026-07-01

### ✅ Infrastructure (Hoàn thành)
- [x] Tạo cấu trúc folder project (ingestion, dbt, orchestration, docker, data/bronze/silver/gold)
- [x] Tạo GitHub repo: https://github.com/vutuannamftu/worldcup2026-pipeline
- [x] Thuê VPS Ubuntu 22.04 (IP: `103.72.57.74`, 34GB disk, deploy user với SSH key)
- [x] Cài Docker + Docker Compose trên VPS
- [x] Cấu hình firewall (ufw): SSH/8080/5432/8088
- [x] Docker Compose stack chạy ổn định: PostgreSQL (project DB + Airflow metadata) + Airflow webserver/scheduler
- [x] Airflow UI truy cập được tại `http://103.72.57.74:8080` (admin: `vutuannam2105`)
- [x] PostgreSQL project DB expose port 5432 (có thể connect từ DBeaver/Python)

### Phase 1 — Ingestion (Bronze)
- [ ] Đăng ký API key từ API-Football (api-sports.io)
- [ ] Viết API client Python (fixtures, lineups, statistics, players)
- [ ] Script lưu raw JSON vào Bronze layer
- [ ] Ingestion từ football-data.org (đối chiếu)
- [ ] Multi-source ingestion (3 nguồn)

### Phase 2 — Incremental Loading
- [ ] Watermark control table trong PostgreSQL
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
- [x] Deploy Airflow trên VPS (Docker Compose, LocalExecutor)
- [ ] Viết Airflow DAGs cho ingestion pipeline
- [ ] Retry logic + alerting (email/Slack)
- [ ] Schedule chạy daily

### Phase 6 — Visualization
- [ ] Kết nối BI tool với PostgreSQL project DB
- [ ] Dashboard: BXH, top scorer, team comparison
- [ ] So sánh vòng bảng vs knockout

### Phase 7 — Hoàn thiện
- [ ] Cập nhật README + sơ đồ kiến trúc
- [ ] CI chạy dbt test tự động (GitHub Actions)

## Deployment Notes

**VPS:**
- Ubuntu 22.04, IP: `103.72.57.74`
- User: `deploy` (sudo, SSH key auth)
- Repo clone tại: `~/worldcup2026-pipeline/`
- Stack: `cd ~/worldcup2026-pipeline/docker && docker compose up -d`
- File `.env` nằm trong `docker/` (quan trọng: không phải thư mục gốc)
- Airflow UI: `http://103.72.57.74:8080`
- PostgreSQL project DB: `103.72.57.74:5432` (DB: `worldcup2026`)

## Điểm nhấn Portfolio

1. **Incremental loading thật** — watermark, không full reload
2. **Data quality test tự động** — dbt test
3. **Orchestration có schedule/retry/alert** — Airflow
4. **Multi-source ingestion** — 3 nguồn dữ liệu
5. **VPS deployment** — production-like environment
