# -*- coding: utf-8 -*-

import os
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import datetime

RATE_PER_MINUTE = 3
MINUTE_TIME_DELTA = datetime.timedelta(seconds=10)
request_amounts = 0
previous_time = datetime.datetime.utcnow()

# web app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQL_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


# database engine

'''
Implement rate-limiting on all of the API endpoint

endpoints:
  /
  /events/hourly
  /events/daily
  /stats/hourly
  /stats/daily
  /poi 
'''


@app.before_request
def before_request():
    global previous_time
    global request_amounts
    right_now = datetime.datetime.utcnow()

    if right_now - previous_time > MINUTE_TIME_DELTA:
        previous_time = right_now
        request_amounts = 0

    if not (request_amounts < RATE_PER_MINUTE and right_now - previous_time < MINUTE_TIME_DELTA):
        return format_error_message(), 404
    request_amounts += 1


def format_error_message():
    error_type = f'''rate limiting error'''
    error_message = f'''You have exceded your limit. You can only make {RATE_PER_MINUTE} per {MINUTE_TIME_DELTA}'''

    return jsonify({'success': False, 'error': error_type, 'message': error_message})


@app.route('/')
def index():

    global previous_time
    global request_amounts
    right_now = datetime.datetime.utcnow()

    resp = 'Welcome to EQ Works 😎'
    resp += f'<h1>{request_amounts} time passed {right_now - previous_time}<h1>'

    return resp


@app.route('/events/hourly')
def events_hourly():
    return queryHelper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/events/daily')
def events_daily():
    return queryHelper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')


@app.route('/stats/hourly')
def stats_hourly():
    return queryHelper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/stats/daily')
def stats_daily():
    return queryHelper('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')


@app.route('/poi')
def poi():
    return queryHelper('''
        SELECT *
        FROM public.poi;
    ''')


def queryHelper(query):
    result = db.engine.execute(query).fetchall()
    return jsonify([dict(row.items()) for row in result])
