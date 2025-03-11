from sqlalchemy import create_engine, text
import pandas as pd
import json
import os
import subprocess
import time

# Koneksi ke database lokal dan cloud
local_engine = create_engine("mysql+mysqlconnector://nodered:password@localhost/nodered")
cloud_engine = create_engine("mysql+mysqlconnector://nodered:password@13.215.228.201/nodered")

# Buffer file untuk menyimpan data sementara saat offline
BUFFER_FILE = "buffer_data.json"

# List tabel yang akan disinkronkan
tables = [
    "cluster1_suhu", "cluster1_kelembaban", "cluster1_tanah",
    "cluster2_suhu", "cluster2_kelembaban", "cluster2_tanah"
]

def check_internet():
    """Cek apakah internet tersedia dengan ping ke Google."""
    try:
        subprocess.run(["ping", "-c", "1", "8.8.8.8"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def save_to_buffer(data):
    """Menyimpan data ke buffer JSON jika internet terputus."""
    buffer = {}
    if os.path.exists(BUFFER_FILE):
        try:
            with open(BUFFER_FILE, "r") as f:
                buffer = json.load(f)
        except (json.JSONDecodeError, ValueError):
            print("⚠️ Buffer file corrupt. Menghapus dan membuat ulang.")
            os.remove(BUFFER_FILE)

    for table, df in data.items():
        if table not in buffer:
            buffer[table] = []
        buffer[table].extend(df.to_dict(orient="records"))

    with open(BUFFER_FILE, "w") as f:
        json.dump(buffer, f, indent=4)

def send_buffered_data():
    """Mengirim data dari buffer ke database cloud setelah internet kembali."""
    if not os.path.exists(BUFFER_FILE):
        return

    try:
        with open(BUFFER_FILE, "r") as f:
            buffer = json.load(f)

        if not buffer:
            print("⚠️ Buffer kosong, tidak ada data yang dikirim.")
            os.remove(BUFFER_FILE)
            return

        with cloud_engine.connect() as conn:
            for table, records in buffer.items():
                if not records:
                    continue
                df = pd.DataFrame(records)
                df.dropna(inplace=True)  # Hapus data NULL sebelum dikirim

                for _, row in df.iterrows():
                    # **Gunakan UPSERT untuk menghindari duplikasi**
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

            conn.commit()  # **Simpan perubahan**
        print(f"✅ Semua data dari buffer berhasil dikirim ke cloud!")
        os.remove(BUFFER_FILE)  # Hapus buffer setelah sukses dikirim

    except (json.JSONDecodeError, ValueError):
        print("⚠️ Buffer file corrupt saat membaca, menghapus...")
        os.remove(BUFFER_FILE)
    except Exception as e:
        print(f"⚠️ Gagal mengirim data dari buffer: {e}")

def sync_table(table):
    """Menyinkronkan tabel lokal ke cloud, menyimpan data jika internet mati."""
    try:
        # Ambil timestamp terbaru di database cloud
        cloud_query = f"SELECT MAX(timestamp) FROM {table}"
        try:
            cloud_last_timestamp = pd.read_sql(cloud_query, cloud_engine).iloc[0, 0]
        except Exception as e:
            print(f"⚠️ Gagal membaca timestamp terakhir dari cloud {table}: {e}")
            cloud_last_timestamp = "2000-01-01 00:00:00"

        # Ambil data dari database lokal yang lebih baru dari yang ada di cloud
        local_query = f"SELECT * FROM {table} WHERE timestamp > '{cloud_last_timestamp}'"
        df = pd.read_sql(local_query, local_engine)

        # Jika tidak ada data baru, lewati
        if df.empty:
            print(f"Tidak ada data baru untuk {table}, skipping...")
            return

        # **Resampling: Hitung rata-rata per menit**
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        df_resampled = df.resample("min").mean()
        df_resampled.dropna(inplace=True)  # Hapus nilai NULL agar tidak error
        df_resampled.reset_index(inplace=True)

        if check_internet():
            with cloud_engine.connect() as conn:
                for _, row in df_resampled.iterrows():
                    # **Gunakan UPSERT**
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
                conn.commit()  # **Simpan perubahan**

            print(f"✅ Data rata-rata per menit dari {table} berhasil dikirim ke cloud!")
        else:
            print(f"⚠️ Internet terputus! Menyimpan data {table} ke buffer.")
            save_to_buffer({table: df_resampled})

    except Exception as e:
        print(f"⚠️ Error saat menyinkronkan data {table}: {e}")

# **Looping utama dengan pengecekan internet terus-menerus**
while True:
    print("\n🔄 Mengecek koneksi dan menyinkronkan data...\n")

    if check_internet():
        send_buffered_data()  # Kirim data dari buffer jika internet kembali

    for table in tables:
        sync_table(table)

    print("\n⏳ Menunggu 1 menit sebelum sinkronisasi berikutnya...\n")
    
    # **Cek terus-menerus jika internet mati agar tidak stuck**
    for _ in range(60):
        if check_internet():
            send_buffered_data()  # Kirim data buffer segera setelah internet kembali
        time.sleep(1)  # Tunggu 1 detik sebelum cek ulang
