from fastapi import FastAPI
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd
import mariadb
import os
import glob

from settings import settings

app = FastAPI()
spark = None


# =========================
# 1. 표준 컬럼 정의
# =========================
STANDARD_COLS = [
    "날짜", "호선", "역번호", "역명", "구분",
    "05~06", "06~07", "07~08", "08~09", "09~10",
    "10~11", "11~12", "12~13", "13~14", "14~15",
    "15~16", "16~17", "17~18", "18~19", "19~20",
    "20~21", "21~22", "22~23", "23~24", "24~"
]

TIME_COLS = [
    "05~06", "06~07", "07~08", "08~09", "09~10",
    "10~11", "11~12", "12~13", "13~14", "14~15",
    "15~16", "16~17", "17~18", "18~19", "19~20",
    "20~21", "21~22", "22~23", "23~24", "24~"
]

METRO_SCHEMA = StructType([
    StructField("날짜", StringType(), True),
    StructField("호선", StringType(), True),
    StructField("역번호", StringType(), True),
    StructField("역명", StringType(), True),
    StructField("구분", StringType(), True),
    StructField("05~06", IntegerType(), True),
    StructField("06~07", IntegerType(), True),
    StructField("07~08", IntegerType(), True),
    StructField("08~09", IntegerType(), True),
    StructField("09~10", IntegerType(), True),
    StructField("10~11", IntegerType(), True),
    StructField("11~12", IntegerType(), True),
    StructField("12~13", IntegerType(), True),
    StructField("13~14", IntegerType(), True),
    StructField("14~15", IntegerType(), True),
    StructField("15~16", IntegerType(), True),
    StructField("16~17", IntegerType(), True),
    StructField("17~18", IntegerType(), True),
    StructField("18~19", IntegerType(), True),
    StructField("19~20", IntegerType(), True),
    StructField("20~21", IntegerType(), True),
    StructField("21~22", IntegerType(), True),
    StructField("22~23", IntegerType(), True),
    StructField("23~24", IntegerType(), True),
    StructField("24~", IntegerType(), True),
    StructField("src_year", IntegerType(), True),
])


# =========================
# 2. Spark 시작 / 종료
# =========================
@app.on_event("startup")
def startup_event():
    global spark
    try:
        jar_path = settings.jdbc_jar_path.strip().replace("\\", "/")
        jar_uri = f"file:///{jar_path}"

        print("===== Spark 시작 시도 =====")
        print("JAVA_HOME =", os.environ.get("JAVA_HOME"))
        print("spark_url =", settings.spark_url)
        print("host_ip =", settings.host_ip)
        print("jdbc_jar_path raw =", repr(settings.jdbc_jar_path))
        print("jar_path =", jar_path)
        print("jar_uri =", jar_uri)
        print("jar exists =", os.path.exists(jar_path))
        print("data_dir exists =", os.path.exists(settings.data_dir))

        spark = (
            SparkSession.builder
            .appName("Jeonbin")
            .master(settings.spark_url)
            .config("spark.jars", jar_uri)
            .config("spark.driver.host", settings.host_ip)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "10000")
            .config("spark.blockManager.port", "10001")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.cores.max", "2")
            .getOrCreate()
        )

        print("Spark 시작 성공!")
        print("Spark version:", spark.version)

    except Exception as e:
        spark = None
        print(f"Failed to create Spark session: {e}")


@app.on_event("shutdown")
def shutdown_event():
    global spark
    if spark:
        spark.stop()
        print("Spark 종료 완료")


# =========================
# 3. 파일 읽기 함수
# =========================
def read_any_file(filepath: str) -> pd.DataFrame:
    filename = os.path.basename(filepath)
    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".csv":
        if any(y in filename for y in ["2017", "2018"]):
            try:
                return pd.read_csv(filepath, encoding="utf-8", header=1, dtype=str, low_memory=False)
            except UnicodeDecodeError:
                return pd.read_csv(filepath, encoding="cp949", header=1, dtype=str, low_memory=False)

        if "2019" in filename:
            try:
                # For 2019, the header is on the second row (index 1).
                return pd.read_csv(filepath, encoding="utf-8", header=1, dtype=str, low_memory=False)
            except UnicodeDecodeError:
                return pd.read_csv(filepath, encoding="cp949", header=1, dtype=str, low_memory=False)

        try:
            return pd.read_csv(filepath, encoding="utf-8", dtype=str, low_memory=False)
        except UnicodeDecodeError:
            return pd.read_csv(filepath, encoding="cp949", dtype=str, low_memory=False)

    elif ext == ".xlsx":
        return pd.read_excel(filepath, engine="openpyxl", dtype=str)

    else:
        raise ValueError(f"지원하지 않는 파일 형식: {filepath}")


# =========================
# 4. 연도별 지하철 파일 표준화
# =========================
def normalize_metro_pdf(pdf: pd.DataFrame, year: int) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf.columns = [str(c).strip() for c in pdf.columns]

    rename_map = {
        "사용일자": "날짜",
        "역번호(호선별)": "역번호",
        "역번호(호선별 기준)": "역번호",
        "승하차구분": "구분",
        "승하차": "구분",

        "06시 이전": "05~06",
        "06 ~ 07": "06~07",
        "07 ~ 08": "07~08",
        "08 ~ 09": "08~09",
        "09 ~ 10": "09~10",
        "10 ~ 11": "10~11",
        "11 ~ 12": "11~12",
        "12 ~ 13": "12~13",
        "13 ~ 14": "13~14",
        "14 ~ 15": "14~15",
        "15 ~ 16": "15~16",
        "16 ~ 17": "16~17",
        "17 ~ 18": "17~18",
        "18 ~ 19": "18~19",
        "19 ~ 20": "19~20",
        "20 ~ 21": "20~21",
        "21 ~ 22": "21~22",
        "22 ~ 23": "22~23",
        "23 ~ 24": "23~24",
        "24시 이후": "24~",

        "00 ~ 01": "24~",
        "06:00 이전": "05~06",
        "06:00 ~ 07:00": "06~07",
        "07:00 ~ 08:00": "07~08",
        "08:00 ~ 09:00": "08~09",
        "09:00 ~ 10:00": "09~10",
        "10:00 ~ 11:00": "10~11",
        "11:00 ~ 12:00": "11~12",
        "12:00 ~ 13:00": "12~13",
        "13:00 ~ 14:00": "13~14",
        "14:00 ~ 15:00": "14~15",
        "15:00 ~ 16:00": "15~16",
        "16:00 ~ 17:00": "16~17",
        "17:00 ~ 18:00": "17~18",
        "18:00 ~ 19:00": "18~19",
        "19:00 ~ 20:00": "19~20",
        "20:00 ~ 21:00": "20~21",
        "21:00 ~ 22:00": "21~22",
        "22:00 ~ 23:00": "22~23",
        "23:00 ~ 24:00": "23~24",
        "24:00 이후": "24~",

        "06시-07시": "06~07",
        "07시-08시": "07~08",
        "08시-09시": "08~09",
        "09시-10시": "09~10",
        "10시-11시": "10~11",
        "11시-12시": "11~12",
        "12시-13시": "12~13",
        "13시-14시": "13~14",
        "14시-15시": "14~15",
        "15시-16시": "15~16",
        "16시-17시": "16~17",
        "17시-18시": "17~18",
        "18시-19시": "18~19",
        "19시-20시": "19~20",
        "20시-21시": "20~21",
        "21시-22시": "21~22",
        "22시-23시": "22~23",
        "23시 이후": "24~",
    }

    for old, new in rename_map.items():
        if old in pdf.columns and new not in pdf.columns:
            pdf.rename(columns={old: new}, inplace=True)

    if "호선" not in pdf.columns:
        pdf["호선"] = None
    if "날짜" not in pdf.columns:
        pdf["날짜"] = None
    if "역번호" not in pdf.columns:
        pdf["역번호"] = ""
    if "역명" not in pdf.columns:
        pdf["역명"] = ""
    if "구분" not in pdf.columns:
        pdf["구분"] = ""

    for col in TIME_COLS:
        if col not in pdf.columns:
            pdf[col] = "0"

    pdf = pdf[STANDARD_COLS]
    pdf["src_year"] = int(year)

    pdf["날짜"] = pdf["날짜"].astype(str).str.strip()

    pdf["호선"] = pdf["호선"].where(pd.notna(pdf["호선"]), None)
    pdf["호선"] = pd.Series(pdf["호선"], dtype="string").replace({pd.NA: None, "": None})

    pdf["역번호"] = (
        pd.to_numeric(
            pdf["역번호"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )
        .astype("Int64")
        .astype(str)
        .replace("<NA>", None)
    )

    pdf["역명"] = pdf["역명"].astype(str).str.strip()
    pdf["구분"] = pdf["구분"].astype(str).str.strip()

    for col in TIME_COLS:
        pdf[col] = (
            pdf[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace({"": None, "-": None, "NA": None, "nan": None})
        )
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0).astype(int)

    return pdf


# =========================
# 5. 전체 파일 읽기 + 통합
# =========================
def build_total_wide_df():
    global spark

    all_files = sorted(glob.glob(os.path.join(settings.data_dir, "*")))
    print("전체 파일:", all_files)

    metro_files = []
    for filepath in all_files:
        filename = os.path.basename(filepath)
        ext = os.path.splitext(filename)[1].lower()

        if ext not in [".csv", ".xlsx"]:
            continue
        if filename.startswith("~$"):
            continue
        if "승하차인원" not in filename:
            continue

        metro_files.append(filepath)

    print("승하차 대상 파일:", metro_files)

    dfs = []
    for filepath in metro_files:
        filename = os.path.basename(filepath)

        year = None
        for y in range(2008, 2022):
            if str(y) in filename:
                year = y
                break

        if year is None:
            print(f"연도 못 찾음, 스킵: {filename}")
            continue

        print(f"읽는 중: {filename}")
        pdf = read_any_file(filepath)
        pdf = normalize_metro_pdf(pdf, year)
        sdf = spark.createDataFrame(pdf, schema=METRO_SCHEMA)
        dfs.append(sdf)

    if not dfs:
        raise ValueError("승하차 파일을 찾지 못했습니다.")

    df_all = dfs[0]
    for d in dfs[1:]:
        df_all = df_all.unionByName(d)

    return df_all


# =========================
# 6. 호선 보정
# =========================
def fill_line_no(df_all):
    df_all = df_all.withColumn(
        "호선",
        F.when(F.trim(F.col("호선")) == "", None).otherwise(F.col("호선"))
    )

    line_map = (
        df_all
        .filter(F.col("호선").isNotNull() & F.col("역번호").isNotNull())
        .groupBy("역번호")
        .agg(F.first("호선", ignorenulls=True).alias("map_호선"))
    )

    df_filled = (
        df_all.alias("a")
        .join(line_map.alias("b"), on="역번호", how="left")
        .withColumn("호선", F.coalesce(F.col("a.호선"), F.col("b.map_호선")))
        .select(
            F.col("a.날짜").alias("날짜"),
            F.col("호선"),
            F.col("역번호"),
            F.col("a.역명").alias("역명"),
            F.col("a.구분").alias("구분"),
            *[F.col(f"a.{c}").alias(c) for c in TIME_COLS],
            F.col("a.src_year").alias("src_year")
        )
    )

    return df_filled


# =========================
# 7. 타입 보정 + 합계 컬럼
# =========================
def cast_and_add_total(df):
    result = df

    for c in TIME_COLS:
        result = result.withColumn(
            c,
            F.coalesce(
                F.regexp_replace(F.col(c).cast("string"), ",", "").cast("int"),
                F.lit(0)
            )
        )

    result = result.withColumn("날짜_raw", F.trim(F.col("날짜").cast("string")))

    result = result.withColumn(
        "날짜_raw",
        F.when(
            (F.col("날짜_raw").isNull()) |
            (F.col("날짜_raw") == "") |
            (F.lower(F.col("날짜_raw")) == "nan") |
            (F.lower(F.col("날짜_raw")) == "none"),
            None
        ).otherwise(F.col("날짜_raw"))
    )

    result = result.withColumn(
        "날짜",
        F.coalesce(
            F.to_date(F.col("날짜_raw"), "yyyy-MM-dd"),
            F.to_date(F.col("날짜_raw"), "yyyyMMdd"),
            F.to_date(F.col("날짜_raw"), "yyyy/MM/dd")
        )
    )

    total_expr = None
    for c in TIME_COLS:
        total_expr = F.col(c) if total_expr is None else total_expr + F.col(c)

    result = result.withColumn("합계", total_expr)

    return result


# =========================
# 8. Python MariaDB 저장 함수
# =========================
def save_to_mariadb_spark(df, table_name=None):
    """Spark DataFrame을 MariaDB에 JDBC를 사용하여 저장합니다."""
    target_table = table_name if table_name else settings.mariadb_table
    driver = "org.mariadb.jdbc.Driver"
    # Set sql_mode=ANSI_QUOTES for the session to make MariaDB accept double quotes for identifiers,
    # which is what Spark's default JDBC dialect uses.
    jdbc_url = f"jdbc:mariadb://{settings.mariadb_host}:{settings.mariadb_port}/{settings.mariadb_database}?characterEncoding=UTF-8&sessionVariables=sql_mode=ANSI_QUOTES"

    properties = {
        "user": settings.mariadb_user,
        "password": settings.mariadb_password,
        "driver": driver
    }

    # Spark의 내장 JDBC writer 사용
    # mode="overwrite"는 테이블이 존재하면 덮어쓰고, 없으면 새로 생성합니다.
    df.write.jdbc(
        url=jdbc_url,
        table=target_table,
        mode="overwrite",
        properties=properties
    )


# =========================
# 9. 상태 확인 API
# =========================
@app.get("/")
def read_root():
    jar_path = settings.jdbc_jar_path.strip().replace("\\", "/")
    jar_uri = f"file:///{jar_path}"

    return {
        "status": spark is not None,
        "spark_url": settings.spark_url,
        "host_ip": settings.host_ip,
        "jar_path": jar_path,
        "jar_uri": jar_uri,
        "jar_exists": os.path.exists(jar_path),
        "data_dir_exists": os.path.exists(settings.data_dir),
    }


@app.get("/test")
def test():
    if not spark:
        return {"status": False, "error": "Spark session not initialized"}

    return {
        "status": True,
        "message": "Spark session initialized",
        "spark_version": spark.version,
    }


# =========================
# 10. Python 직접 MariaDB 연결 테스트
# =========================
@app.get("/db-test")
def db_test():
    try:
        conn = mariadb.connect(
            host=settings.mariadb_host,
            port=settings.mariadb_port,
            user=settings.mariadb_user,
            password=settings.mariadb_password,
            database=settings.mariadb_database,
        )

        cur = conn.cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()

        cur.close()
        conn.close()

        return {
            "status": True,
            "message": "MariaDB direct connection success",
            "result": row[0]
        }

    except Exception as e:
        return {"status": False, "error": str(e)}


# =========================
# 11. Python 직접 쓰기 테스트
# =========================
@app.get("/db-write-test")
def db_write_test():
    try:
        conn = mariadb.connect(
            host=settings.mariadb_host,
            port=settings.mariadb_port,
            user=settings.mariadb_user,
            password=settings.mariadb_password,
            database=settings.mariadb_database,
        )
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS subway_total_test")
        cur.execute("""
            CREATE TABLE subway_total_test (
                날짜 DATE,
                호선 VARCHAR(50),
                역번호 VARCHAR(50),
                역명 VARCHAR(100),
                구분 VARCHAR(20),
                합계 INT
            )
        """)
        cur.execute("""
            INSERT INTO subway_total_test (날짜, 호선, 역번호, 역명, 구분, 합계)
            VALUES
            ('2026-03-18', '1호선', '150', '서울역', '승차', 100),
            ('2026-03-18', '1호선', '150', '서울역', '하차', 120)
        """)
        conn.commit()

        cur.close()
        conn.close()

        return {
            "status": True,
            "message": "Python direct write success",
            "table": "subway_total_test"
        }

    except Exception as e:
        return {"status": False, "error": str(e)}


# =========================
# 12. 전체 통합 + DB 적재 API
# =========================
@app.get("/build-all")
def build_all():
    if not spark:
        return {"status": False, "error": "Spark session not initialized"}

    try:
        df_total = build_total_wide_df()

        null_before = df_total.filter(F.col("호선").isNull()).count()

        df_total = fill_line_no(df_total)

        null_after = df_total.filter(F.col("호선").isNull()).count()

        df_total = cast_and_add_total(df_total)

        df_to_save = df_total.drop("날짜_raw")

        year_counts = (
            df_to_save.groupBy("src_year")
            .count()
            .orderBy("src_year")
            .toPandas()
            .to_dict(orient="records")
        )

        save_to_mariadb_spark(df_to_save)

        return {
            "status": True,
            "message": f"{settings.mariadb_table} 테이블 적재 완료",
            "total_count": df_to_save.count(),
            "line_null_before": null_before,
            "line_null_after": null_after,
            "year_counts": year_counts,
            "sample_total": df_to_save.limit(20).toPandas().to_dict(orient="records"),
        }

    except Exception as e:
        return {"status": False, "error": str(e)}
    
# =========================
# 13. 데이터 검증 API
# =========================
@app.get("/verify-data")
def verify_data():
    try:
        # 1. DB에서 집계 데이터 가져오기
        print("DB에서 집계 데이터 가져오는 중...")
        conn = mariadb.connect(
            host=settings.mariadb_host,
            port=settings.mariadb_port,
            user=settings.mariadb_user,
            password=settings.mariadb_password,
            database=settings.mariadb_database,
        )
        cur = conn.cursor()
        cur.execute(f"SELECT SUM(합계), COUNT(*) FROM {settings.mariadb_table}")
        db_sum, db_count = cur.fetchone()
        cur.close()
        conn.close()
        print(f"DB 집계: 합계={db_sum}, 개수={db_count}")

        # 2. 원본 파일에서 집계 데이터 가져오기
        print("원본 파일에서 집계 데이터 가져오는 중...")
        all_files = sorted(glob.glob(os.path.join(settings.data_dir, "*")))
        metro_files = []
        for filepath in all_files:
            filename = os.path.basename(filepath)
            ext = os.path.splitext(filename)[1].lower()
            if ext not in [".csv", ".xlsx"] or filename.startswith("~$") or "승하차인원" not in filename:
                continue
            metro_files.append(filepath)

        source_total_sum = 0
        source_total_count = 0
        for filepath in metro_files:
            filename = os.path.basename(filepath)
            year = None
            for y in range(2008, 2022):
                if str(y) in filename:
                    year = y
                    break
            if year is None:
                print(f"연도 못 찾음, 스킵: {filename}")
                continue

            print(f"파일 처리 중: {filename}")
            pdf = read_any_file(filepath)
            pdf = normalize_metro_pdf(pdf, year)

            # 각 행의 시간 컬럼들을 합산하여 총 승객 수 계산
            source_total_sum += pdf[TIME_COLS].sum(axis=1).sum()
            source_total_count += len(pdf)
        
        print(f"원본 파일 집계: 합계={source_total_sum}, 개수={source_total_count}")

        # 3. 결과 비교 및 반환
        sum_match = int(db_sum) == int(source_total_sum)
        count_match = int(db_count) == int(source_total_count)

        return {
            "status": True,
            "verification_results": {
                "sum_of_passengers": {
                    "source_files": f"{source_total_sum:,}",
                    "database": f"{db_sum:,}",
                    "match": sum_match,
                },
                "row_count": {
                    "source_files": f"{source_total_count:,}",
                    "database": f"{db_count:,}",
                    "match": count_match,
                },
                "overall_match": sum_match and count_match,
            },
        }

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"status": False, "error": str(e)}
