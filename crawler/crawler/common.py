# -*- coding: utf-8 -*-
from Crypto.Cipher import AES
import binascii
import base64, hashlib
import os
# from elasticsearch import Elasticsearch, helpers
from crawler.settings import ES_SERVER, ES_USER_NAME, ES_PASSWORD, ES_CERT, REDIS_PORT, REDIS_PASSWORD, \
    CACHE_REDIS_HOST, ES_INDEX, ES_TYPE, GP_HOST, GP_DB, GP_USER, GP_PW, GP_PORT, MONGO_HOST, MONGO_PORT, \
    MONGO_DB, MONGO_USER, MONGO_PASS, REDIS_OTHER_DB
import psycopg2
import psycopg2.extensions
# import redis
import time
#

AES_KEY = b'1qazXSW@3edcVFR$'
AES_IV = b'0okmNJI(8uhbVGY&'
CHUNKSIZE = 10000

CNT = 0


def decrypt(message):
    cipher = AES.new(AES_KEY, mode=AES.MODE_CBC, IV=AES_IV)
    bytestr = binascii.a2b_hex(message)
    plaintext = cipher.decrypt(bytestr)
    return to_str(plaintext.rstrip())


def encrypt(message):
    cipher = AES.new(AES_KEY, mode=AES.MODE_CBC, IV=AES_IV)

    length = len(message)
    mod = length % AES.block_size
    if mod > 0:
        width = length + AES.block_size - mod
        message = message.ljust(width)
    encrypted_message = cipher.encrypt(message)
    return binascii.b2a_hex(encrypted_message)


def hash(str):
    return hashlib.sha1(str.encode('utf-8')).hexdigest()


# def get_ldf_reader(parameters):
#     return pd.read_csv(parameters.etlFile, quotechar='"', sep="|", keep_default_na=False, iterator=True,
#                        chunksize=CHUNKSIZE, encoding='utf-8')


def write_ldf_reader(df, file_name):
    return df.to_csv("%s" % file_name, mode='a', sep="|", encoding='utf-8', header=False, index=False)

#
# def get_mongodb():
#     client = mc(MONGO_HOST, int(MONGO_PORT))
#     mongodb = client[MONGO_DB]
#     mongodb.authenticate(MONGO_USER, to_str(decrypt(MONGO_PASS)))
#     return mongodb


# def get_es_client():
#     es = Elasticsearch(
#         ES_SERVER.split(','),
#         # http_auth=(ES_USER_NAME, to_str(decrypt(ES_PASSWORD))),
#         port=9200,
#         # use_ssl=True,
#         use_ssl=False,
#         verify_certs=False,
#         ca_certs=ES_CERT,
#         ssl_assert_hostname=False,
#         timeout=300
#     )
#     return es


# def get_redis_client(db=None):
#     pool = redis.ConnectionPool(host=CACHE_REDIS_HOST, port=REDIS_PORT, password=to_str(decrypt(REDIS_PASSWORD)))
#     if db:
#         pool = redis.ConnectionPool(host=CACHE_REDIS_HOST, port=REDIS_PORT, password=to_str(decrypt(REDIS_PASSWORD)),
#                                     db=REDIS_OTHER_DB)
#     rc = redis.Redis(connection_pool=pool)
#     return rc


def es_rest_call(es, url='/', method='GET', body=None):
    return es.transport.perform_request(method, url, None, body)


def to_str(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        value = bytes_or_str.decode('utf-8')
    else:
        value = bytes_or_str
    return value


def to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, str):
        value = bytes_or_str.encode('utf-8')
    else:
        value = bytes_or_str
    return value


def get_gp_connect():
    conn = psycopg2.connect(database=GP_DB, user=GP_USER, password=to_str(decrypt(GP_PW)), host=GP_HOST, port=GP_PORT)
    conn.autocommit = True
    return conn


def gp_execute(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)


def gp_select(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


def gp_get(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchone()
    return rows



