#core/db_inspect.py
from core.storage import get_connection


TABLE_ORDER_COLUMNS = {
    "tenants": "created_at",
    "workspaces": "created_at",
    "runs": "id",
    "events": "id",
    "issues": "id",
    "plan_versions": "id",
}


def show_table(name: str):
    conn = get_connection()
    cur = conn.cursor()

    order_col = TABLE_ORDER_COLUMNS.get(name, "rowid")
    rows = cur.execute(f"SELECT * FROM {name} ORDER BY {order_col} DESC LIMIT 20").fetchall()

    for row in rows:
        print(dict(row))

    conn.close()


if __name__ == "__main__":
    print("\nTENANTS")
    show_table("tenants")

    print("\nWORKSPACES")
    show_table("workspaces")

    print("\nRUNS")
    show_table("runs")

    print("\nEVENTS")
    show_table("events")

    print("\nISSUES")
    show_table("issues")

    print("\nPLAN VERSIONS")
    show_table("plan_versions")