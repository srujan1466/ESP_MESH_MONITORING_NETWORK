from flask import Flask,request,render_template,jsonify
import sqlite3
import json
import datetime

app=Flask(__name__)

def init_db():
    conn=sqlite3.connect("data.db")
    c=conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS pollution(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node TEXT,
    temp REAL,
    hum REAL,
    air REAL,
    noise REAL,
    time TEXT)
    """)
    conn.commit()
    conn.close()

@app.route("/data",methods=["POST"])
def receive_data():

    data=request.json

    node=data["node"]
    temp=data["temp"]
    hum=data["hum"]
    air=data["air"]
    noise=data["noise"]

    conn=sqlite3.connect("data.db")
    c=conn.cursor()

    c.execute(
    "INSERT INTO pollution(node,temp,hum,air,noise,time) VALUES(?,?,?,?,?,?)",
    (node,temp,hum,air,noise,str(datetime.datetime.now()))
    )

    conn.commit()
    conn.close()

    return jsonify({"status":"ok"})


@app.route("/")
def dashboard():

    conn=sqlite3.connect("data.db")
    c=conn.cursor()

    rows=c.execute("SELECT * FROM pollution ORDER BY id DESC LIMIT 50").fetchall()

    conn.close()

    return render_template("dashboard.html",rows=rows)

if __name__=="__main__":
    init_db()
    app.run(host="0.0.0.0",port=5000)
