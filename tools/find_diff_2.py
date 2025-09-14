import os
import psycopg2
import pandas as pd
from deepdiff import DeepDiff
from pathlib import Path
from datetime import datetime

try:
    from dotenv import load_dotenv  # optional
    load_dotenv()
except Exception:
    pass

# ---------- ENV ----------
def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

PG_DSN = env_str("PG_DSN")
REPORT_CSV_PATH = os.getenv("REPORT_CSV_PATH", "./youthpolicy_diff_report.csv")

# ---------- Helpers ----------
def pick_change_dt(record: dict) -> str | None:
    """
    변경 시점으로 other 레코드의 lastMdfcnDt > ingested_at 순으로 선택.
    문자열/타임스탬프 모두 허용, 반환은 ISO 문자열.
    """
    cand = record.get("lastMdfcnDt") or record.get("ingested_at")
    if cand is None:
        return None
    # pandas Timestamp, datetime, str 모두 처리
    if isinstance(cand, pd.Timestamp):
        return cand.to_pydatetime().isoformat()
    if isinstance(cand, datetime):
        return cand.isoformat()
    # 문자열이면 그대로(가능하면 공백 제거)
    return str(cand).strip()

def path_to_field(path: str) -> str:
    """
    DeepDiff 경로(root['a']['b'])를 사람이 읽기 쉬운 점 표기(a.b)로 변환.
    raw_json 루트 접두사는 제거.
    """
    import re
    parts = re.findall(r"\['([^]]+)'\]", path)
    if parts:
        # raw_json 접두사 제거
        if parts and parts[0] == "raw_json":
            parts = parts[1:]
        return ".".join(parts) if parts else "root"
    # fallback
    return path.replace("root['raw_json']", "").replace("root.", "").replace("root", "").strip()

# ---------- Connect ----------
conn = psycopg2.connect(PG_DSN)

# ---------- Load duplicated records ----------
query = """
SELECT *
FROM youthpolicy.stg.youthpolicy_landing
WHERE policy_id IN (
    SELECT policy_id
    FROM youthpolicy.stg.youthpolicy_landing
    GROUP BY policy_id
    HAVING COUNT(*) > 1
)
ORDER BY policy_id, ingested_at;  -- 시간순으로 정렬해 비교
"""

df = pd.read_sql(query, conn)

# ---------- Compare with DeepDiff ----------
diff_rows: list[dict] = []

for policy_id, group in df.groupby("policy_id", sort=False):
    # 기록 순서 보장 (쿼리에서 정렬됨)
    records = group.to_dict(orient="records")
    if len(records) <= 1:
        continue

    base = records[0]
    base_change_dt = pick_change_dt(base)

    for idx, other in enumerate(records[1:], start=2):
        diff = DeepDiff(
            base,
            other,
            ignore_order=True,
            exclude_paths=[
                "root['lastMdfcnDt']",
                "root['frstRegDt']",
                "root['ingested_at']",
                "root['record_hash']",
                "root['raw_ingest_id']"
            ]
        )

        change_dt = pick_change_dt(other)

        if "values_changed" in diff:
            for path, change in diff["values_changed"].items():
                field = path_to_field(path)
                diff_rows.append({
                    "policy_id": policy_id,
                    "compare_seq": idx,                 # base=1, 그 다음=2,3,...
                    "field": field,
                    "old_value": change.get("old_value"),
                    "new_value": change.get("new_value"),
                    "base_change_dt": base_change_dt,   # 기준 레코드 시점
                    "change_dt": change_dt              # 변경(비교 대상) 시점
                })

        # 다음 라운드 비교를 위해 base를 최신으로 갱신 (체인지로그처럼)
        base = other
        base_change_dt = change_dt

# ---------- Print results (방법 1: 한 줄 비교 + 변경일 출력) ----------
if diff_rows:
    for row in diff_rows:
        when = row["change_dt"] or "-"
        print(f"{row['policy_id']} | {row['field']} | {row['old_value']}  -->  {row['new_value']} | 변경일: {when}")
else:
    print("중복된 policy_id 레코드 간 차이가 없습니다.")

# ---------- CSV 리포트 저장 (방법 2) ----------
if diff_rows:
    out_df = pd.DataFrame(diff_rows, columns=[
        "policy_id", "compare_seq", "field", "old_value", "new_value", "base_change_dt", "change_dt"
    ])
    # 경로 보장
    out_path = Path(REPORT_CSV_PATH).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[CSV] {len(out_df)}건 저장: {out_path}")

# ---------- Close ----------
conn.close()