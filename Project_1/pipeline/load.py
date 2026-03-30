from sqlalchemy import create_engine
import duckdb

def run():
    # ambil data dari parquet
    con = duckdb.connect()

    df = con.execute("""
        SELECT * FROM read_parquet('data/processed/year=*/*.parquet')
    """).df()

    # koneksi postgres (pakai nama service docker!)
    engine = create_engine("postgresql://admin:admin@postgres:5432/crashdb")

    df.to_sql("crash", engine, if_exists="replace", index=False)

if __name__ == "__main__":
    run()