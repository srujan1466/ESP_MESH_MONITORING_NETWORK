import sqlite3

conn=sqlite3.connect("pollution_data.db")
cur=conn.cursor()

res=cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
l=[]
for i in res:
    l.append(i[0])

for i in range(len(l)):
    res=cur.execute(f"SELECT * FROM {l[i]}");
    print(list(res))

conn.close()
