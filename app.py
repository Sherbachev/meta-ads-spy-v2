from flask import Flask, render_template, request, jsonify
import psycopg2
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

def get_db():
    return psycopg2.connect(os.environ['DATABASE_URL'])

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS competitors (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            page_id VARCHAR(255) NOT NULL UNIQUE,
            page_url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS ads (
            id SERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(id) ON DELETE CASCADE,
            ad_id VARCHAR(255) NOT NULL UNIQUE,
            status VARCHAR(50) DEFAULT 'active',
            body TEXT,
            snapshot_url TEXT,
            started_at TIMESTAMP,
            stopped_at TIMESTAMP,
            days_active INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()

def sync_competitor(competitor_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM competitors WHERE id = %s', (competitor_id,))
    comp = cur.fetchone()
    if not comp:
        return 0

    token = os.environ.get('META_ACCESS_TOKEN')
    page_id = comp[2]

    url = 'https://graph.facebook.com/v21.0/ads_archive'
    params = {
        'access_token': token,
        'search_terms': page_id,
        'ad_reached_countries': 'UA',
        'fields': 'id,ad_creative_bodies,ad_snapshot_url,ad_delivery_start_time,ad_delivery_stop_time',
        'limit': 50
    }

    res = requests.get(url, params=params)
    data = res.json()

    new_ads = 0
    for ad in data.get('data', []):
        cur.execute('SELECT id FROM ads WHERE ad_id = %s', (ad['id'],))
        if cur.fetchone():
            continue

        started_at = datetime.fromisoformat(ad['ad_delivery_start_time']) if ad.get('ad_delivery_start_time') else datetime.now()
        stopped_at = datetime.fromisoformat(ad['ad_delivery_stop_time']) if ad.get('ad_delivery_stop_time') else None
        days = (stopped_at or datetime.now()) - started_at
        status = 'inactive' if stopped_at else 'active'
        body = ad.get('ad_creative_bodies', [''])[0]

        cur.execute('''
            INSERT INTO ads (competitor_id, ad_id, status, body, snapshot_url, started_at, stopped_at, days_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', (competitor_id, ad['id'], status, body, ad.get('ad_snapshot_url'), started_at, stopped_at, days.days))
        new_ads += 1

    conn.commit()
    cur.close()
    conn.close()
    return new_ads

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/competitors', methods=['GET'])
def get_competitors():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT id, name, page_url, created_at FROM competitors ORDER BY created_at DESC')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([{'id': r[0], 'name': r[1], 'page_url': r[2], 'created_at': str(r[3])} for r in rows])

@app.route('/api/competitors', methods=['POST'])
def add_competitor():
    data = request.json
    page_url = data['page_url'].rstrip('/')
    page_id = page_url.split('/')[-1]
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO competitors (name, page_id, page_url) VALUES (%s, %s, %s) RETURNING id, name, page_url',
        (page_id, page_id, page_url)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'id': row[0], 'name': row[1], 'page_url': row[2]})

@app.route('/api/ads', methods=['GET'])
def get_ads():
    competitor_id = request.args.get('competitor_id')
    status = request.args.get('status')
    conn = get_db()
    cur = conn.cursor()
    query = '''SELECT ads.id, ads.ad_id, ads.status, ads.body, ads.snapshot_url,
               ads.days_active, ads.started_at, competitors.name
               FROM ads JOIN competitors ON ads.competitor_id = competitors.id WHERE 1=1'''
    params = []
    if competitor_id:
        params.append(competitor_id)
        query += ' AND competitor_id = %s'
    if status:
        params.append(status)
        query += ' AND ads.status = %s'
    query += ' ORDER BY ads.created_at DESC'
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([{
        'id': r[0], 'ad_id': r[1], 'status': r[2], 'body': r[3],
        'snapshot_url': r[4], 'days_active': r[5],
        'started_at': str(r[6]), 'competitor_name': r[7]
    } for r in rows])

@app.route('/api/sync', methods=['POST'])
def sync():
    competitor_id = request.json['competitor_id']
    new_ads = sync_competitor(competitor_id)
    return jsonify({'synced': new_ads})

init_db()

if __name__ == '__main__':
    app.run(debug=True)
