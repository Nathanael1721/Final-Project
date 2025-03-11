from sqlalchemy import create_engine, text
import pandas as pd
import json
import os
import subprocess
import time

# Connection to local and cloud databases
local_engine = create_engine("mysql+mysqlconnector://username:password@localhost/dbname")
cloud_engine = create_engine("mysql+mysqlconnector://username:password@publicIP/dbname")

# Buffer file to store data temporarily while offline
BUFFER_FILE = "buffer_data.json"

# List of tables to be synchronized
tables = [
    "cluster1_suhu", "cluster1_kelembaban", "cluster1_tanah",
    "cluster2_suhu", "cluster2_kelembaban", "cluster2_tanah"
]

def check_internet():
    """Check if internet is available by pinging Google."""
    try:
        subprocess.run(["ping", "-c", "1", "8.8.8.8"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def save_to_buffer(data):
    """Save data to JSON buffer if internet is lost."""
    buffer = {}
    if os.path.exists(BUFFER_FILE):
        try:
            with open(BUFFER_FILE, "r") as f:
                buffer = json.load(f)
        except (json.JSONDecodeError, ValueError):
            print("⚠️ Buffer file corrupt. Deleting and recreating.")
            os.remove(BUFFER_FILE)

    for table, df in data.items():
        if table not in buffer:
            buffer[table] = []
        buffer[table].extend(df.to_dict(orient="records"))

    with open(BUFFER_FILE, "w") as f:
        json.dump(buffer, f, indent=4)

def send_buffered_data():
    """Sending data from the buffer to the cloud database after the internet is back."""
    if not os.path.exists(BUFFER_FILE):
        return

    try:
        with open(BUFFER_FILE, "r") as f:
            buffer = json.load(f)

        if not buffer:
            print("⚠️ Buffer is empty, no data sent.")
            os.remove(BUFFER_FILE)
            return

        with cloud_engine.connect() as conn:
            for table, records in buffer.items():
                if not records:
                    continue
                df = pd.DataFrame(records)
                df.dropna(inplace=True)  # Remove NULL data before sending

                for _, row in df.iterrows():
                    # **Use UPSERT to avoid duplication**
                    query = text(f"""
                        INSERT INTO {table} (timestamp, id, {table.split('_')[1]})
                        VALUES (:timestamp, :id, :value)
                        ON DUPLICATE KEY UPDATE 
                            {table.split('_')[1]} = VALUES({table.split('_')[1]}),
                            timestamp = VALUES(timestamp);
                    """)
                    conn.execute(query, {
                        "timestamp": row["timestamp"],
                        "id": row["id"],
                        "value": row[table.split('_')[1]]
                    })

            conn.commit()  # **Save changes**
        print(f"✅ All data from the buffer was successfully sent to the cloud.!")
        os.remove(BUFFER_FILE)  # Clear buffer after successful sending

    except (json.JSONDecodeError, ValueError):
        print("⚠️ File buffer corrupted while reading, deleting...")
        os.remove(BUFFER_FILE)
    except Exception as e:
        print(f"⚠️ Failed to send data from buffer: {e}")

def sync_table(table):
    """Sync local tables to the cloud, saving data if internet goes down."""
    try:
        # Get the latest timestamp in the cloud database
        cloud_query = f"SELECT MAX(timestamp) FROM {table}"
        try:
            cloud_last_timestamp = pd.read_sql(cloud_query, cloud_engine).iloc[0, 0]
        except Exception as e:
            print(f"⚠️ Failed to read last timestamp from cloud {table}: {e}")
            cloud_last_timestamp = "2000-01-01 00:00:00"

        # Fetch data from a local database that is newer than the one in the cloud
        local_query = f"SELECT * FROM {table} WHERE timestamp > '{cloud_last_timestamp}'"
        df = pd.read_sql(local_query, local_engine)

        # If there is no new data, skip
        if df.empty:
            print(f"There is no new data for {table}, skipping...")
            return

        # **Resampling: Calculate average per minute**
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        df_resampled = df.resample("min").mean()
        df_resampled.dropna(inplace=True)  # Remove NULL values ​​to avoid errors
        df_resampled.reset_index(inplace=True)

        if check_internet():
            with cloud_engine.connect() as conn:
                for _, row in df_resampled.iterrows():
                    # **Use UPSERT**
                    query = text(f"""
                        INSERT INTO {table} (timestamp, id, {table.split('_')[1]})
                        VALUES (:timestamp, :id, :value)
                        ON DUPLICATE KEY UPDATE 
                            {table.split('_')[1]} = VALUES({table.split('_')[1]}),
                            timestamp = VALUES(timestamp);
                    """)
                    conn.execute(query, {
                        "timestamp": row["timestamp"],
                        "id": row["id"],
                        "value": row[table.split('_')[1]]
                    })
                conn.commit()  # **Save changes**

            print(f"✅ Average data per minute from {table} successfully sent to cloud!")
        else:
            print(f"⚠️ Internet disconnected! Saving {table} data to buffer.")
            save_to_buffer({table: df_resampled})

    except Exception as e:
        print(f"⚠️ Error while syncing data {table}: {e}")

# **Main looping with continuous internet checking**
while True:
    print("\n🔄 Check connection and sync data...\n")

    if check_internet():
        send_buffered_data()  # Send data from buffer if internet is back

    for table in tables:
        sync_table(table)

    print("\n⏳ Wait 1 minute before next sync...\n")
    
    # **Keep checking if the internet is down so you don't get stuck.**
    for _ in range(60):
        if check_internet():
            send_buffered_data()  # Send buffered data as soon as internet is back
        time.sleep(1)  # Wait 1 second before rechecking
