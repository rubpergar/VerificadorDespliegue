# src/db.py — SSH + MySQL (túnel y get_conn)
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder
import pymysql
import streamlit as st

@contextmanager
def ssh_tunnel():
    SSH = st.secrets["ssh"]
    DB = st.secrets["mysql"]
    kwargs = dict(
        ssh_address_or_host=(SSH["host"], int(SSH.get("port", 22))),
        ssh_username=SSH["user"],
        remote_bind_address=(DB["host"], int(DB["port"])),
        set_keepalive=10.0,
        threaded=True,
        allow_agent=False,
    )
    if SSH.get("key_file"):
        kwargs["ssh_pkey"] = SSH["key_file"]
    else:
        kwargs["ssh_password"] = SSH.get("password")

    server = SSHTunnelForwarder(**kwargs)
    server.start()
    try:
        yield ("127.0.0.1", server.local_bind_port)
    finally:
        server.stop()

def get_conn(local_host: str, local_port: int):
    DB = st.secrets["mysql"]
    return pymysql.connect(
        host=local_host,
        port=local_port,
        user=DB["user"],
        password=DB["password"],
        database=DB["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=20,
        read_timeout=60,
        write_timeout=60
    )
