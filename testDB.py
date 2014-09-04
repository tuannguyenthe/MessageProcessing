__author__ = 'user'

import pymysql as mysql
import os

'''
ssl_path = os.getenv('SSL_PATH')
ssl = {'ca': ssl_path + '/ca-cert.pem', 'cert': ssl_path + '/client-cert.pem', 'key': ssl_path + '/client-key.pem'}
conn = mysql.connect(host='ngovantam.ddns.net', user='monty', passwd='python', ssl=ssl)
cursor = conn.cursor()
cursor.execute('SHOW STATUS like "Ssl_cipher"')
print(cursor.fetchone())
'''

for i in range(1,10):
    with open('D:/Logs/temp.txt', 'a') as filehander:
        filehander.write(str(i))

