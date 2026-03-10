#!/usr/bin/env python3
"""
=============================================================================
Air & Noise Pollution Monitoring Server
=============================================================================
Flask server with real-time dashboard for industrial pollution monitoring.

Features:
- Receives sensor data from ESP32 gateway via HTTP POST
- SQLite database for persistent storage
- Real-time WebSocket updates to dashboard
- Interactive dashboard with zone classification
- Node health monitoring
- Historical data charts
- Export capabilities
=============================================================================
"""

import os
import json
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, g
from flask_socketio import SocketIO, emit

# =============================================================================
# Configuration
# =============================================================================

app = Flask(__name__)
app.config['SECRET_KEY'] = 'pollution-monitor-secret-key-2024'

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

DATABASE = 'pollution_data.db'
DATA_LOCK = threading.Lock()

# Pollution thresholds for zone classification
THRESHOLDS = {
    'air_quality': {
        'green': (0, 50),       # Good
        'yellow': (50, 100),    # Moderate
        'orange': (100, 200),   # Unhealthy for sensitive
        'red': (200, 5000),     # Hazardous
    },
    'noise': {
        'green': (0, 55),       # Acceptable
        'yellow': (55, 70),     # Moderate
        'orange': (70, 85),     # Loud
        'red': (85, 140),       # Dangerous
    },
    'temperature': {
        'green': (15, 30),
        'yellow': (30, 40),
        'orange': (40, 50),
        'red': (50, 100),
    }
}

# In-memory node status cache
node_status_cache = {}
gateway_status = {}

# =============================================================================
# Database
# =============================================================================

def get_db():
    """Get thread-local database connection."""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA synchronous=NORMAL")
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    """Initialize database tables."""
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_mac TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            received_at TEXT NOT NULL,
            air_quality_ppm REAL,
            temperature REAL,
            humidity REAL,
            noise_db REAL,
            mq135_raw INTEGER,
            sound_raw INTEGER,
            hop_count INTEGER,
            seq_num INTEGER,
            rssi INTEGER,
            recv_rssi INTEGER,
            battery_pct INTEGER,
            peer_count INTEGER,
            node_state INTEGER
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS node_registry (
            mac TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            total_packets INTEGER DEFAULT 0,
            status TEXT DEFAULT 'unknown',
            zone TEXT DEFAULT 'green',
            hop_count INTEGER DEFAULT 0,
            rssi INTEGER DEFAULT 0
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            node_mac TEXT,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            acknowledged INTEGER DEFAULT 0
        )
    ''')

    # Create indices for performance
    c.execute('CREATE INDEX IF NOT EXISTS idx_sensor_node ON sensor_data(node_mac)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sensor_time ON sensor_data(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sensor_node_time ON sensor_data(node_mac, timestamp)')

    conn.commit()
    conn.close()
    print("[DB] Database initialized successfully")


def get_standalone_db():
    """Get a database connection outside of Flask request context."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# Zone Classification
# =============================================================================

def classify_zone(air_ppm, noise_db, temperature):
    """Determine overall zone based on worst metric."""
    zones = []

    for metric, value in [('air_quality', air_ppm), ('noise', noise_db), ('temperature', temperature)]:
        if value is None or value < -900:
            continue
        thresholds = THRESHOLDS[metric]
        zone = 'green'
        for z in ['red', 'orange', 'yellow', 'green']:
            low, high = thresholds[z]
            if low <= value < high:
                zone = z
                break
            elif value >= thresholds['red'][0]:
                zone = 'red'
                break
        zones.append(zone)

    if not zones:
        return 'green'

    priority = {'red': 4, 'orange': 3, 'yellow': 2, 'green': 1}
    worst = max(zones, key=lambda z: priority.get(z, 0))
    return worst


def classify_metric(metric_name, value):
    """Classify a single metric into a zone."""
    if value is None or value < -900:
        return 'green'
    thresholds = THRESHOLDS.get(metric_name, {})
    for z in ['red', 'orange', 'yellow', 'green']:
        if z in thresholds:
            low, high = thresholds[z]
            if low <= value < high:
                return z
    if value >= thresholds.get('red', (0, 0))[0]:
        return 'red'
    return 'green'


# =============================================================================
# Alert Generation
# =============================================================================

def check_and_create_alerts(node_mac, air_ppm, noise_db, temperature):
    """Check thresholds and create alerts if needed."""
    alerts = []
    now_str = datetime.utcnow().isoformat()

    if air_ppm is not None and air_ppm > 200:
        alerts.append({
            'timestamp': now_str,
            'node_mac': node_mac,
            'alert_type': 'air_quality',
            'severity': 'critical' if air_ppm > 500 else 'warning',
            'message': f'Air quality critical: {air_ppm:.1f} PPM at node {node_mac}'
        })

    if noise_db is not None and noise_db > 85:
        alerts.append({
            'timestamp': now_str,
            'node_mac': node_mac,
            'alert_type': 'noise',
            'severity': 'critical' if noise_db > 100 else 'warning',
            'message': f'Noise level dangerous: {noise_db:.1f} dB at node {node_mac}'
        })

    if temperature is not None and temperature > 50:
        alerts.append({
            'timestamp': now_str,
            'node_mac': node_mac,
            'alert_type': 'temperature',
            'severity': 'critical' if temperature > 60 else 'warning',
            'message': f'Temperature extreme: {temperature:.1f}°C at node {node_mac}'
        })

    if alerts:
        conn = get_standalone_db()
        try:
            for alert in alerts:
                conn.execute(
                    'INSERT INTO alerts (timestamp, node_mac, alert_type, severity, message) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (alert['timestamp'], alert['node_mac'], alert['alert_type'],
                     alert['severity'], alert['message'])
                )
            conn.commit()
        finally:
            conn.close()

        for alert in alerts:
            socketio.emit('new_alert', alert)

    return alerts


# =============================================================================
# API Routes
# =============================================================================

@app.route('/api/data', methods=['POST'])
def receive_data():
    """Receive sensor data from gateway."""
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'No JSON data'}), 400

        node_mac = data.get('node_mac', 'unknown')
        now_str = datetime.utcnow().isoformat()

        air_ppm = data.get('air_quality_ppm')
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        noise_db = data.get('noise_db')

        # Store sensor data
        db = get_db()
        db.execute(
            '''INSERT INTO sensor_data
               (node_mac, timestamp, received_at, air_quality_ppm, temperature,
                humidity, noise_db, mq135_raw, sound_raw, hop_count, seq_num,
                rssi, recv_rssi, battery_pct, peer_count, node_state)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                node_mac,
                data.get('timestamp', int(time.time())),
                now_str,
                air_ppm,
                temperature,
                humidity,
                noise_db,
                data.get('mq135_raw', 0),
                data.get('sound_raw', 0),
                data.get('hop_count', 0),
                data.get('seq_num', 0),
                data.get('rssi', 0),
                data.get('recv_rssi', 0),
                data.get('battery_pct', 0),
                data.get('peer_count', 0),
                data.get('node_state', 0),
            )
        )

        # Update node registry
        zone = classify_zone(air_ppm, noise_db, temperature)

        db.execute(
            '''INSERT INTO node_registry (mac, first_seen, last_seen, total_packets, status, zone, hop_count, rssi)
               VALUES (?, ?, ?, 1, 'active', ?, ?, ?)
               ON CONFLICT(mac) DO UPDATE SET
                   last_seen = excluded.last_seen,
                   total_packets = total_packets + 1,
                   status = 'active',
                   zone = excluded.zone,
                   hop_count = excluded.hop_count,
                   rssi = excluded.rssi''',
            (node_mac, now_str, now_str, zone,
             data.get('hop_count', 0), data.get('rssi', 0))
        )
        db.commit()

        # Update in-memory cache
        node_status_cache[node_mac] = {
            'mac': node_mac,
            'air_quality_ppm': air_ppm,
            'temperature': temperature,
            'humidity': humidity,
            'noise_db': noise_db,
            'zone': zone,
            'rssi': data.get('rssi', 0),
            'hop_count': data.get('hop_count', 0),
            'peer_count': data.get('peer_count', 0),
            'node_state': data.get('node_state', 0),
            'battery_pct': data.get('battery_pct', 100),
            'last_seen': now_str,
            'status': 'active',
            'air_zone': classify_metric('air_quality', air_ppm),
            'noise_zone': classify_metric('noise', noise_db),
            'temp_zone': classify_metric('temperature', temperature),
        }

        # Check alerts
        check_and_create_alerts(node_mac, air_ppm, noise_db, temperature)

        # Emit real-time update via WebSocket
        socketio.emit('sensor_update', node_status_cache[node_mac])

        return jsonify({'status': 'ok', 'zone': zone}), 200

    except Exception as e:
        app.logger.error(f"Error receiving data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/node_status', methods=['POST'])
def receive_node_status():
    """Receive node status from gateway."""
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'No JSON data'}), 400

        global gateway_status
        gateway_status = {
            'mac': data.get('gateway_mac', 'unknown'),
            'uptime': data.get('uptime', 0),
            'last_report': datetime.utcnow().isoformat(),
        }

        nodes = data.get('nodes', [])
        for node in nodes:
            mac = node.get('mac', 'unknown')
            if mac in node_status_cache:
                node_status_cache[mac]['status'] = node.get('status', 'unknown')
                node_status_cache[mac]['hop_count'] = node.get('hop_count', 0)
                node_status_cache[mac]['rssi'] = node.get('rssi', 0)
                node_status_cache[mac]['peer_count'] = node.get('peer_count', 0)
                node_status_cache[mac]['node_state'] = node.get('node_state', 0)
            else:
                node_status_cache[mac] = {
                    'mac': mac,
                    'status': node.get('status', 'unknown'),
                    'hop_count': node.get('hop_count', 0),
                    'rssi': node.get('rssi', 0),
                    'peer_count': node.get('peer_count', 0),
                    'node_state': node.get('node_state', 0),
                    'air_quality_ppm': node.get('air_quality_ppm'),
                    'temperature': node.get('temperature'),
                    'humidity': node.get('humidity'),
                    'noise_db': node.get('noise_db'),
                    'last_seen': datetime.utcnow().isoformat(),
                    'zone': 'green',
                    'air_zone': 'green',
                    'noise_zone': 'green',
                    'temp_zone': 'green',
                    'battery_pct': 100,
                }

        socketio.emit('network_update', {
            'gateway': gateway_status,
            'nodes': list(node_status_cache.values()),
        })

        return jsonify({'status': 'ok'}), 200

    except Exception as e:
        app.logger.error(f"Error receiving status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    """Get all registered nodes."""
    return jsonify(list(node_status_cache.values()))


@app.route('/api/history/<node_mac>', methods=['GET'])
def get_history(node_mac):
    """Get historical data for a node."""
    hours = request.args.get('hours', 1, type=int)
    limit = request.args.get('limit', 500, type=int)

    cutoff = int(time.time()) - (hours * 3600)

    db = get_db()
    rows = db.execute(
        '''SELECT * FROM sensor_data
           WHERE node_mac = ? AND timestamp > ?
           ORDER BY timestamp DESC LIMIT ?''',
        (node_mac, cutoff, limit)
    ).fetchall()

    result = []
    for row in rows:
        result.append(dict(row))

    return jsonify(result)


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Get recent alerts."""
    limit = request.args.get('limit', 50, type=int)

    db = get_db()
    rows = db.execute(
        'SELECT * FROM alerts ORDER BY id DESC LIMIT ?', (limit,)
    ).fetchall()

    return jsonify([dict(r) for r in rows])


@app.route('/api/summary', methods=['GET'])
def get_summary():
    """Get system summary statistics."""
    db = get_db()

    total_nodes = len(node_status_cache)
    active_nodes = sum(1 for n in node_status_cache.values() if n.get('status') == 'active')

    # Get latest readings averages
    avg_air = 0
    avg_noise = 0
    avg_temp = 0
    count = 0
    for n in node_status_cache.values():
        if n.get('air_quality_ppm') is not None and n['air_quality_ppm'] > -900:
            avg_air += n['air_quality_ppm']
            avg_noise += n.get('noise_db', 0)
            avg_temp += n.get('temperature', 0)
            count += 1

    if count > 0:
        avg_air /= count
        avg_noise /= count
        avg_temp /= count

    # Zone counts
    zone_counts = {'green': 0, 'yellow': 0, 'orange': 0, 'red': 0}
    for n in node_status_cache.values():
        z = n.get('zone', 'green')
        zone_counts[z] = zone_counts.get(z, 0) + 1

    # Total records
    total_records = db.execute('SELECT COUNT(*) FROM sensor_data').fetchone()[0]

    # Recent alerts count
    one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    recent_alerts = db.execute(
        'SELECT COUNT(*) FROM alerts WHERE timestamp > ?', (one_hour_ago,)
    ).fetchone()[0]

    return jsonify({
        'total_nodes': total_nodes,
        'active_nodes': active_nodes,
        'avg_air_quality': round(avg_air, 1),
        'avg_noise': round(avg_noise, 1),
        'avg_temperature': round(avg_temp, 1),
        'zone_counts': zone_counts,
        'total_records': total_records,
        'recent_alerts': recent_alerts,
        'gateway': gateway_status,
        'overall_zone': classify_zone(avg_air, avg_noise, avg_temp) if count > 0 else 'green',
    })


@app.route('/api/alerts/<int:alert_id>/ack', methods=['POST'])
def acknowledge_alert(alert_id):
    """Acknowledge an alert."""
    db = get_db()
    db.execute('UPDATE alerts SET acknowledged = 1 WHERE id = ?', (alert_id,))
    db.commit()
    return jsonify({'status': 'ok'})


# =============================================================================
# Dashboard Route
# =============================================================================

@app.route('/')
def dashboard():
    """Serve the main dashboard page."""
    return render_template('dashboard.html')


# =============================================================================
# WebSocket Events
# =============================================================================

@socketio.on('connect')
def handle_connect():
    """Handle new WebSocket connection."""
    emit('network_update', {
        'gateway': gateway_status,
        'nodes': list(node_status_cache.values()),
    })


@socketio.on('request_history')
def handle_history_request(data):
    """Handle history data request from client."""
    node_mac = data.get('node_mac')
    hours = data.get('hours', 1)

    if not node_mac:
        return

    cutoff = int(time.time()) - (hours * 3600)

    conn = get_standalone_db()
    try:
        rows = conn.execute(
            '''SELECT air_quality_ppm, temperature, humidity, noise_db, timestamp
               FROM sensor_data WHERE node_mac = ? AND timestamp > ?
               ORDER BY timestamp ASC LIMIT 500''',
            (node_mac, cutoff)
        ).fetchall()

        result = [dict(r) for r in rows]
        emit('history_data', {'node_mac': node_mac, 'data': result})
    finally:
        conn.close()


# =============================================================================
# Data Cleanup Task
# =============================================================================

def cleanup_old_data():
    """Remove data older than 30 days."""
    while True:
        time.sleep(3600)  # Run every hour
        try:
            cutoff = int(time.time()) - (30 * 24 * 3600)
            conn = get_standalone_db()
            conn.execute('DELETE FROM sensor_data WHERE timestamp < ?', (cutoff,))
            conn.execute('DELETE FROM alerts WHERE timestamp < ?',
                        ((datetime.utcnow() - timedelta(days=7)).isoformat(),))
            conn.commit()
            conn.close()
            print("[CLEANUP] Old data removed")
        except Exception as e:
            print(f"[CLEANUP] Error: {e}")


# =============================================================================
# Main
# =============================================================================

if __name__ == '__main__':
    init_db()

    # Start cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_old_data, daemon=True)
    cleanup_thread.start()

    print("=" * 60)
    print("  Air & Noise Pollution Monitoring Server")
    print("  Dashboard: http://0.0.0.0:5000")
    print("=" * 60)

    socketio.run(app, host='0.0.0.0', port=5000, debug=False)