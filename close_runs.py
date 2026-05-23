from core.storage import get_connection

conn = get_connection()
cur = conn.cursor()
cur.execute("UPDATE runs SET ended_at = '2026-05-22T23:59:59+00:00' WHERE ended_at IS NULL")
print(f"Closed {cur.rowcount} open runs")
conn.commit()
conn.close()
