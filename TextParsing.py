
import struct
from datetime import date
import os
import message
try:
    import mysql.connector as connector
except:
    pass
def toInt(byte):
    return int.from_bytes(byte, byteorder='big', signed=False)

#just ignore header record
def readheader(input):
    if len(input) != 12:
        return -1
    else:
        #lastUpdate = [toInt(input[0:4]), toInt(input[4:8])]
        lastUpdate = toInt(input[0:4])
        lastSequence = toInt(input[8:12])
        return [lastUpdate, lastSequence]


def readrecord(input):
    if len(input) != 24:
        return -1
    else:
        return [toInt(input[0:4]), toInt(input[8:12]), toInt(input[12:16]), toInt(input[16:20]), toInt(input[20:22])]

def parseLog(direction, path, linkID, cnx):
    fileIdx = direction + 'cont' + str(linkID) + '.idx'
    fileLog = direction + 'cont' + str(linkID) + '.log'

    if(mode == 'db'):
        cur = cnx.cursor()

        insert_new_order = ('INSERT INTO orderz (broker_id, order_number, entry_date, client_id, '
                            'client_type, buy_sell, symbol, volume, price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)')
        insert_matched_order = ('INSERT INTO match_status (broker_id, order_number, entry_date, buy_sell, '
                                'volume, price) VALUES (%s, %s, %s, %s, %s, %s)')

    with open(path + fileIdx, "rb") as file_idx_handler, \
            open(path + fileLog, "rb") as file_log_handler, \
            open(csv_path + 'new_order.csv', "a") as csv_new_order_handler, \
            open(csv_path + 'deal.csv', "a") as csv_deal_handler:

        #just ignore the readheader function
        header_date = date.fromtimestamp(readheader(file_idx_handler.read(12))[0])
        rec = 0

        while rec != -1:
            rec = readrecord(file_idx_handler.read(24))
            if rec == -1:
                break
            file_log_handler.seek(rec[3], 0)
            currentsequence = file_log_handler.read(rec[4])
            if currentsequence.decode(errors='replace').find('ARP') != -1\
                    or currentsequence.decode(errors='replace').find('ARN') != -1:
                continue

            #removing front padding
            currentsequence = currentsequence[14:]

            #read messages
            while len(currentsequence) > 1:
                msg = currentsequence[:message.messagelength.get(currentsequence[0:2].decode())].decode()
                #print(msg)
                if msg[0:2] == '1I':
                    data_new_order = (str(int(msg[2:5])),  #broker_id
                                      msg[9:17].strip(),  #order_number
                                      header_date,  #entry_date
                                      msg[17:27],  #client_id
                                      msg[64:65],  #client_type
                                      msg[35:36],  #buy_sell
                                      msg[27:35].strip(),  #symbol
                                      msg[36:44].strip(),  # volume
                                      msg[52:58].strip())  # price
                    if mode == 'db':
                        cur.execute(insert_new_order, data_new_order)
                        cnx.commit()
                    elif mode == 'csv':
                        csv_new_order_handler.write(data_new_order[0] + delimiter + data_new_order[1] + delimiter +
                                                         str(data_new_order[2]) + delimiter + data_new_order[3] + delimiter +
                                                         data_new_order[4] + delimiter + data_new_order[5] + delimiter +
                                                         data_new_order[6] + delimiter + data_new_order[7] + delimiter +
                                                         data_new_order[8] + '\n')
                elif msg[0:2] == '2E':
                    data_match_status = (str(int(msg[2:5])),  # broker_id
                                      msg[6:14].strip(),  # order_number
                                      header_date,  # entry_date
                                      msg[5:6],  # buy_sell
                                      msg[20:28].strip(),  # volume
                                      msg[28:34].strip())  # price
                    if(mode == 'db'):
                        cur.execute(insert_matched_order, data_match_status)
                        cnx.commit()
                    elif mode == 'csv':
                        csv_deal_handler.write(data_match_status[0] + delimiter + data_match_status[1] + delimiter +
                                                         str(data_match_status[2]) + delimiter + data_match_status[3] + delimiter +
                                                         data_match_status[4] + delimiter + data_match_status[5] + '\n')
                elif msg[0:2] == '2I':
                    data_match_status_buy = (str(int(msg[2:5])),  # broker_id
                                      msg[5:13].strip(),  # order_number
                                      header_date,  # entry_date
                                      'B',  # buy_sell
                                      msg[29:37].strip(),  # volume,
                                      msg[37:43].strip())  # price
                    data_match_status_sell = (str(int(msg[2:5])),  # broker_id
                                      msg[17:25].strip(),  # order_number
                                      header_date,  # entry_date
                                      'S',  # buy_sell
                                      msg[29:37].strip(),  # volume
                                      msg[37:43].strip())  # price
                    if(mode == 'db'):
                        cur.executemany(insert_matched_order, [data_match_status_buy, data_match_status_sell, ])
                        cnx.commit()
                    elif mode == 'csv':
                        csv_deal_handler.writelines(data_match_status_buy[0] + delimiter + data_match_status_buy[1] + delimiter +
                                                    str(data_match_status_buy[2]) + delimiter + data_match_status_buy[3] + delimiter +
                                                    data_match_status_buy[4] + delimiter + data_match_status_buy[5] + '\n')
                        csv_deal_handler.writelines(data_match_status_sell[0] + delimiter + data_match_status_sell[1] + delimiter +
                                                    str(data_match_status_sell[2]) + delimiter + data_match_status_sell[3] + delimiter +
                                                    data_match_status_sell[4] + delimiter + data_match_status_sell[5] + '\n')
                currentsequence = currentsequence[1 + message.messagelength.get(currentsequence[0:2].decode()):]

    return 0


path = 'D:/Logs/03-09/'
csv_path = path + 'csv/'

mode = 'csv'
delimiter = ','

if mode == 'db':
    cnx = connector.connect(user='monty', password='python', host='localhost', database='orders')
    cursorA = cnx.cursor(buffered=True)
    get_broker = ('SELECT broker_id FROM brokers WHERE broker_status !="N"')
    cursorA.execute(get_broker)

    for broker_id in cursorA:
        parseLog('i', path, broker_id[0], cnx)
        parseLog('o', path, broker_id[0], cnx)
    cnx.close()

elif mode == 'csv':
    with open(csv_path + 'new_order.csv', "a") as csv_new_order_handler, \
            open(csv_path + 'deal.csv', "a") as csv_deal_handler:
        csv_new_order_handler.write('broker_id' + delimiter + 'order_number' + delimiter +
                                             'entry_date' + delimiter + 'client_id' + delimiter +
                                             'client_type' + delimiter + 'buy_sell' + delimiter +
                                             'symbol' + delimiter + 'volume' + delimiter + 'price' + '\n')
        csv_deal_handler.write('broker_id' + delimiter + 'order_number' + delimiter +
                                        'entry_date' + delimiter + 'buy_sell' + delimiter +
                                        'volume' + delimiter + 'price' + '\n')

    for broker_id in range(1, message.last_broker):
        if(broker_id not in message.non_broker_list):
            parseLog('i', path, broker_id, None)
            parseLog('o', path, broker_id, None)
