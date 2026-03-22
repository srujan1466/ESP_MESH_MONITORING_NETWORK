import sqlite3

conn=sqlite3.connect("./server/pollution_data.db")
cur=conn.cursor()


res=cur.execute("Select * from sqlite_master")
for i in res:
    print(str(i))
    print()

res=cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
l=[]
for i in res:
    l.append(i[0])

for i in range(len(l)):
    with open(f"Hello{i}.csv","w") as f:
        res=cur.execute(f"SELECT * FROM {l[i]} ")
        col=[description[0] for description in res.description]
        f.write(",".join(col)+"\n")
        for i in res:
            print(i)
            f.write(str(i)+"\n")

conn.close()
