import mariadb
from settings import settings

conn_params = {
  "user": settings.maria_user,
  "password": settings.maria_password,
  "host": settings.maria_host,
  "database" : settings.maria_database,
  "port" : int(settings.maria_port)
}

def getConn():
  try:
    conn = mariadb.connect(**conn_params)
    if conn == None:
      return None
    return conn
  except mariadb.Error as e:
    print(f"접속 오류 : {e}")
    return None

def findOne(sql):
  result = None
  try:
    conn = getConn()
    if conn:
      cur = conn.cursor()
      cur.execute(sql)
      row = cur.fetchone()
      columns = [desc[0] for desc in cur.description]
      cur.close()
      conn.close()
      result = dict(zip(columns, row)) if row else None
  except mariadb.Error as e:
    print(f"MariaDB Error : {e}")
  return result

def findAll(sql):
  result = []
  try:
    conn = getConn()
    if conn:
      cur = conn.cursor()
      cur.execute(sql)
      rows = cur.fetchall()
      columns = [desc[0] for desc in cur.description]
      cur.close()
      conn.close()
      result = [dict(zip(columns, row)) for row in rows]
  except mariadb.Error as e:
    print(f"MariaDB Error : {e}")
  return result

def save(sql):
  result = False
  try:
    conn = getConn()
    if conn:
      cur = conn.cursor()
      cur.execute(sql)
      conn.commit()
      cur.close()
      conn.close()
      result = True
  except mariadb.Error as e:
    print(f"MariaDB Error : {e}")
  return result


def saveMany(sql2: str, values):
  result = False
  try:
    conn = getConn()
    if conn:
      cur = conn.cursor()
    #   cur.execute(sql1)
      cur.executemany(sql2, values)
      conn.commit()
      cur.close()
      conn.close()
      result = True
  except mariadb.Error as e:
    print(f"MariaDB Error : {e}")
  return result

def add_key(sql):
  result = [False, 0]
  try:
    conn = getConn()
    if conn:
      cur = conn.cursor()
      cur.execute(sql)
      sql2 = "SELECT LAST_INSERT_ID() as no"
      cur.execute(sql2)
      row = cur.fetchone()
      columns = [desc[0] for desc in cur.description]
      data = dict(zip(columns, row)) if row else None      
      conn.commit()
      cur.close()
      conn.close()
      result[0] = True
      if data:
        result[1] = data["no"]
  except mariadb.Error as e:
    print(f"MariaDB Error : {e}")
  return result