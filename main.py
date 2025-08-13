from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone
from fastapi.middleware.cors import CORSMiddleware
import os

# from clickhouse_driver import Client
import clickhouse_connect

app = FastAPI(root_path="/analytics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Event(BaseModel):
    event_type: str
    page: str
    time_spent: str
    element_id: str
    user_info: str

def get_click_house_client():
    return clickhouse_connect.get_client(
        host='j94mx47wmo.ap-south-1.aws.clickhouse.cloud',
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        secure=True
    )

@app.get("/")
def get_events(limit: int = 100):
    client = get_click_house_client()
    result = client.query(f"""
        SELECT event_type, element_id, timestamp, page, time_spent, user_info
        FROM web_events
        ORDER BY timestamp DESC
        LIMIT {limit}
    """)
    rows = result.result_rows

    return [
        {"event_type": row[0], "element_id": row[1], "timestamp": row[2].isoformat(), "page": row[3], "time_spent": row[4], "user_info": row[5],}
        for row in rows
    ]

@app.post("/")
def add_event(event: Event):
    timestamp = datetime.now(timezone.utc)
    client = get_click_house_client()
    client.insert(
        "web_events",
        [[event.event_type, event.element_id, timestamp, event.page, event.time_spent, event.user_info]],
        column_names=["event_type", "element_id", "timestamp", "page", "time_spent", "user_info"]
    )
    return {"message": f"Event added with server timestamp - {event}"}










# @app.get("/")
# def get_events(limit: int = 100):
#     client = get_click_house_client()
#     rows = client.execute(f'''
#         SELECT event_type, element_id, timestamp
#         FROM web_events
#         ORDER BY timestamp DESC
#         LIMIT {limit}
#     ''')
#     return [
#         {"event_type": row[0], "element_id": row[1], "timestamp": row[2].isoformat()}
#         for row in rows
#     ]

# @app.post("/")
# def add_event(event: Event):
#     timestamp = datetime.now(timezone.utc)
#     client = get_click_house_client()
#     client.execute(
#         'INSERT INTO web_events (event_type, element_id, timestamp) VALUES',
#         [(event.event_type, event.element_id, timestamp)]
#     )
#     return {"message": "Event added with server timestamp"}
