import sqlite3

def sendNotifications():
    con = sqlite3.connect('/Users/andrewdaniels/Desktop/Oregon_try_2/Oregon_IFTA_try2.db')
    cur = con.cursor()

    cur.execute("SELECT * FROM vNotifyResults")
    rows = cur.fetchall()
    con.close()

    