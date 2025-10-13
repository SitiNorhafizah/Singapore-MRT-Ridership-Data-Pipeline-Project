# populate_bronze_realistic.py
import random
from datetime import datetime, timedelta
from pyhive import hive

# ----------------------------
# Connect to Hive
# ----------------------------
conn = hive.Connection(host='localhost', port=10000, username='mrt_user', database='bronze')
cursor = conn.cursor()

# ----------------------------
# MRT North-South Line Stations
# ----------------------------
stations = [
    ('NS1', 'Jurong East', 1.3321, 103.7433),
    ('NS2', 'Bukit Batok', 1.3500, 103.7490),
    ('NS3', 'Bukit Gombak', 1.3575, 103.7518),
    ('NS4', 'Choa Chu Kang', 1.3873, 103.7448),
    ('NS5', 'Yew Tee', 1.4020, 103.7459),
    ('NS7', 'Kranji', 1.4250, 103.7625),
    ('NS8', 'Marsiling', 1.4365, 103.7765),
    ('NS9', 'Woodlands', 1.4370, 103.7860),
    ('NS10', 'Admiralty', 1.4425, 103.7915),
    ('NS11', 'Sembawang', 1.4490, 103.8190),
    ('NS12', 'Canberra', 1.4440, 103.8280),
    ('NS13', 'Yishun', 1.4295, 103.8350),
    ('NS14', 'Khatib', 1.4165, 103.8280),
    ('NS15', 'Yio Chu Kang', 1.3795, 103.8455),
    ('NS16', 'Ang Mo Kio', 1.3695, 103.8490),
    ('NS17', 'Bishan', 1.3500, 103.8490),
    ('NS18', 'Braddell', 1.3405, 103.8490),
    ('NS19', 'Toa Payoh', 1.3320, 103.8480),
    ('NS20', 'Novena', 1.3200, 103.8430),
    ('NS21', 'Newton', 1.3100, 103.8390),
    ('NS22', 'Orchard', 1.3045, 103.8310),
    ('NS23', 'Somerset', 1.3000, 103.8330),
    ('NS24', 'Dhoby Ghaut', 1.2960, 103.8450),
    ('NS25', 'City Hall', 1.2930, 103.8510),
    ('NS26', 'Raffles Place', 1.2830, 103.8510),
    ('NS27', 'Marina Bay', 1.2775, 103.8540)
]

# ----------------------------
# Populate Station Exits
# ----------------------------
for s in stations:
    exits = random.randint(2, 10)
    cursor.execute(f"""
        INSERT INTO bronze.station_exits
        VALUES ('{s[0]}', '{s[1]}', {s[2]}, {s[3]}, {exits})
    """)

# ----------------------------
# Populate Alerts
# ----------------------------
alert_messages = [
    "Signal fault reported",
    "Train delay 5-10 mins",
    "Maintenance work ongoing",
    "Temporary station closure",
]

for i in range(1, 31):  # 30 alerts
    station = random.choice(stations)
    message = f"{station[1]}: {random.choice(alert_messages)}"
    now = datetime.now() - timedelta(days=random.randint(0,5), hours=random.randint(0,23))
    cursor.execute(f"""
        INSERT INTO bronze.alerts
        VALUES ('alert_{i}', '{message}', '{now.strftime('%Y-%m-%d %H:%M:%S')}')
    """)

# ----------------------------
# Populate Ridership (7 days)
# ----------------------------
start_date = datetime(2025, 10, 1)
for s in stations:
    for i in range(7):
        ride_date = start_date + timedelta(days=i)
        total_riders = random.randint(5000, 20000)
        cursor.execute(f"""
            INSERT INTO bronze.ridership
            VALUES ('{s[0]}', '{ride_date.strftime('%Y-%m-%d')}', {total_riders})
        """)

# ----------------------------
# Populate Station Crowd
# ----------------------------
for s in stations:
    for i in range(7):
        ride_date = start_date + timedelta(days=i)
        total_riders = random.randint(5000, 20000)
        avg_riders = total_riders / 5
        std_riders = avg_riders * 0.1
        deviation = random.uniform(-2, 2)
        is_anomaly = 1 if abs(deviation) > 1.5 else 0
        cursor.execute(f"""
            INSERT INTO bronze.station_crowd
            VALUES ('{s[0]}', '{ride_date.strftime('%Y-%m-%d')}', {total_riders},
                    {avg_riders:.2f}, {std_riders:.2f}, {deviation:.2f}, {is_anomaly})
        """)

# ----------------------------
# Populate Train Alerts
# ----------------------------
trains = ['T1', 'T2', 'T3', 'T4', 'T5']
for i, train in enumerate(trains, start=1):
    station = random.choice(stations)
    now = datetime.now() - timedelta(days=random.randint(0,3), hours=random.randint(0,23))
    msg = f"Train {train} at {station[1]}: Delay reported"
    cursor.execute(f"""
        INSERT INTO bronze.train_alerts
        VALUES ('{train}', '{msg}', '{now.strftime('%Y-%m-%d %H:%M:%S')}')
    """)

print("✅ Bronze tables populated with realistic MRT data!")
conn.close()
