import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request


BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", BASE_DIR / "data" / "attendance.db"))

app = Flask(__name__)


def get_connection():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                room_id TEXT NOT NULL,
                student_name TEXT NOT NULL,
                device_id TEXT NOT NULL,
                check_type TEXT NOT NULL DEFAULT 'check-in',
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_attendance_timestamp ON attendance(timestamp)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_attendance_student ON attendance(student_name)"
        )


def normalize_timestamp(value):
    if value is None or value == "":
        return datetime.now(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc).isoformat()
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string or Unix timestamp")
    return value


def validate_record(payload):
    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")

    required = ("room_id", "student_name", "device_id")
    missing = [field for field in required if not str(payload.get(field, "")).strip()]
    if missing:
        raise ValueError("missing required fields: " + ", ".join(missing))

    check_type = str(payload.get("check_type", "check-in")).strip().lower()
    if check_type not in {"check-in", "check-out"}:
        raise ValueError("check_type must be 'check-in' or 'check-out'")

    return {
        "timestamp": normalize_timestamp(payload.get("timestamp")),
        "room_id": str(payload["room_id"]).strip(),
        "student_name": str(payload["student_name"]).strip(),
        "device_id": str(payload["device_id"]).strip(),
        "check_type": check_type,
    }


def insert_record(record):
    created_at = datetime.now(timezone.utc).isoformat()
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO attendance
                (timestamp, room_id, student_name, device_id, check_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                record["timestamp"],
                record["room_id"],
                record["student_name"],
                record["device_id"],
                record["check_type"],
                created_at,
            ),
        )
        return cursor.lastrowid


def row_to_dict(row):
    return dict(row)


@app.get("/")
def dashboard():
    return render_template("index.html")


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/api/attendance")
def create_attendance():
    try:
        record = validate_record(request.get_json(silent=True))
        record_id = insert_record(record)
    except (ValueError, TypeError) as error:
        return jsonify({"error": str(error)}), 400

    return jsonify({"id": record_id, **record}), 201


@app.get("/api/attendance")
def list_attendance():
    limit = min(max(request.args.get("limit", default=100, type=int), 1), 500)
    filters = []
    values = []
    for field in ("room_id", "student_name", "device_id", "check_type"):
        value = request.args.get(field)
        if value:
            filters.append(field + " = ?")
            values.append(value)

    query = "SELECT * FROM attendance"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY timestamp DESC, id DESC LIMIT ?"
    values.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, values).fetchall()
    return jsonify({"items": [row_to_dict(row) for row in rows], "count": len(rows)})


@app.get("/api/attendance/summary")
def attendance_summary():
    month = request.args.get("month")
    query = """
        SELECT student_name, room_id, check_type, COUNT(*) AS total
        FROM attendance
    """
    values = []
    if month:
        query += " WHERE substr(timestamp, 1, 7) = ?"
        values.append(month)
    query += " GROUP BY student_name, room_id, check_type ORDER BY total DESC"

    with get_connection() as connection:
        rows = connection.execute(query, values).fetchall()
    return jsonify({"items": [row_to_dict(row) for row in rows]})


init_db()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
