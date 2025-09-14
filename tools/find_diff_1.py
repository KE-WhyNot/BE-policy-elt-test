import os
import psycopg2
import pandas as pd
from deepdiff import DeepDiff

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
ORDER BY policy_id;
"""

df = pd.read_sql(query, conn)

# ---------- Compare with DeepDiff ----------
diff_rows = []

for policy_id, group in df.groupby("policy_id"):
    records = group.to_dict(orient="records")
    if len(records) > 1:
        base = records[0]
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
            if "values_changed" in diff:
                for path, change in diff["values_changed"].items():
                    field = path.replace("root['raw_json']", "").replace("root", "").strip()
                    diff_rows.append({
                        "policy_id": policy_id,
                        "record_idx": idx,
                        "field": field,
                        "old_value": change["old_value"],
                        "new_value": change["new_value"]
                    })

# ---------- Print results (방법 1: 한 줄 비교) ----------
if diff_rows:
    for row in diff_rows:
        print(f"{row['policy_id']} | {row['field']} | {row['old_value']}  -->  {row['new_value']}")
else:
    print("중복된 policy_id 레코드 간 차이가 없습니다.")

# ---------- Close ----------
conn.close()