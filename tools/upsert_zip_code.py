import csv
from sqlalchemy import create_engine, text

# DB 연결 (환경변수나 DSN에 맞게 수정하세요)
engine = create_engine("postgresql+psycopg2://admin:adminpw@localhost:5432/youthpolicy")

def upsert_zipcodes_from_csv(csv_path: str):
    success_count = 0
    fail_count = 0
    # DB connection test
    try:
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
            
            # master.region의 full_name 리스트 출력 테스트
            # result = test_conn.execute(text("SELECT full_name FROM master.region")).fetchall()
            # full_names = [r[0] for r in result]
            # print(f"[TEST] master.region full_name 리스트 ({len(full_names)}개):")
            # print(full_names)

    except Exception as e:
        print(f"[ERROR] DB 연결 실패: {e}")
        return

    with engine.begin() as conn, open(csv_path, newline="", encoding="EUC-KR") as f:
        reader = csv.DictReader(f)

        # CSV의 법정동명 리스트 출력 테스트
        # csv_full_names = [row["법정동명"].strip() for row in reader]
        # print(f"[TEST] CSV 법정동명 리스트 ({len(csv_full_names)}개):")
        # print(csv_full_names)

        # DictReader는 한 번 순회하면 소진되므로, 다시 파일을 열어 reader를 재생성
        f.seek(0)
        reader = csv.DictReader(f)

        for row in reader:
            code10 = row["법정동코드"].strip()
            name = row["법정동명"].strip()
            status = row["폐지여부"].strip()

            # 시도 단위(끝 00000) 또는 폐지된 건 스킵
            # if code10.endswith("00000") or status != "존재":
            #     continue

            # 5자리 zip prefix 추출
            zip_prefix = code10[:5]

            # master.region.full_name과 비교
            res = conn.execute(
                text("SELECT id FROM master.region WHERE full_name = :name"),
                {"name": name},
            ).mappings().first()

            if not res:
                # print(f"[WARN] full_name 매칭 실패: {name}")
                fail_count += 1
                continue  # 매칭 안되면 skip

            region_id = res["id"]

            # master.region에 zip_code 업데이트
            conn.execute(
                text("""
                    UPDATE master.region
                    SET zip_code = :zip
                    WHERE id = :rid
                """),
                {"zip": zip_prefix, "rid": region_id},
            )
            success_count += 1

    print("CSV import & zip_code update 완료")
    print(f"성공: {success_count} / 실패: {fail_count}")

# 실행 예시
if __name__ == "__main__":
    upsert_zipcodes_from_csv("국토교통부_법정동코드_20250805.csv")