import struct
import datetime
import os
import message


def toInt(byte):
    return int.from_bytes(byte, byteorder='big', signed=False)

#just ignore header record
def readheader(input):
    if len(input) != 12:
        return -1
    else:
        lastUpdate = [toInt(input[0:4]), toInt(input[4:8])]
        lastSequence = toInt(input[8:12])
        return [lastUpdate, lastSequence]


def readrecord(input):
    if len(input) != 24:
        return -1
    else:
        return [toInt(input[0:4]), toInt(input[8:12]), toInt(input[12:16]), toInt(input[16:20]), toInt(input[20:22])]

def parseLog(direction, path, linkID):
    fileIdx = direction + 'cont' + str(linkID) + '.idx'
    fileLog = direction + 'cont' + str(linkID) + '.log'

    with open(path + fileIdx, "rb") as fileIdxHandler, open(path + fileLog, "rb") as fileLogHandler:
        #just ignore the readheader function
        readheader(fileIdxHandler.read(12))
        rec = 0
        while rec != -1:
            rec = readrecord(fileIdxHandler.read(24))
            if rec == -1:
                break
            fileLogHandler.seek(rec[3], 0)
            currentsequence = fileLogHandler.read(rec[4])
            if currentsequence.decode(errors='replace').find('ARP') != -1:
                continue


            #removing front padding
            currentsequence = currentsequence[14:]


            #read messages
            while len(currentsequence) > 1:
                msg = currentsequence[:message.messagelength.get(currentsequence[0:2].decode())].decode()
                print(msg)  # this is what we need put this to database
                currentsequence = currentsequence[1 + message.messagelength.get(currentsequence[0:2].decode()):]

            #timestamp is here
            #if idxSequence[1] != 0 and post !=-1:
            #    datetime_temp = datetime.datetime.fromtimestamp(idxSequence[0])
            #    #datetime_temp.milisecond = idxSequence[0][1]
            #    post['datetime'] = datetime_temp
            #    post['sequenceNO'] = idxSequence[1]

    return 0

def readInbound(controlledInput, input):
    # Read new order message 1I
    if(controlledInput == '1I' and len(input) == 70 and str(input[0:2]) == '1I'):
        message = input[0:2]
        firm = input[2:5]
        traderID = input[5:9]
        orderNO = input[9:17]
        clientID = input[17:27]
        symbol = input[27:35]
        side = input[35:36]
        volume = input[36:44]
        publishedVolume = input[44:52]
        price = input[52:58]
        board = input[58:59]
        filler1 = input[59:64]
        clientFlag = input[64:65]
        filler2 = input[65:70]
        return {"message": message.strip(), 'firm': firm.strip(), 'traderID': traderID.strip(), 
                      'orderNO': orderNO.strip(), 'clientID': clientID.strip(), 'symbol': symbol.strip(), 'side':side.strip(),
                      'volume': volume.strip() , 'publishedVolume': publishedVolume.strip(), 'price': price.strip(), 
                      'board': board.strip() ,'clientFlag' : clientFlag.strip()}

    # Read cancellation order message 1C
    elif(controlledInput == '1C' and len(input) != 17 and str(input[0:2]) != '1C'):
        message = input[0:2]
        firm = input[2:5]
        orderNO = input[5:13]
        entryDate = input[13:17]
        return {'message': message.strip(), 'firm': firm.strip(), 'orderNO': orderNO.strip(), 'entryDate': entryDate.strip()}
    else:
        return -1 # should raise exception rather than -1

def readOutbound(controlledInput, input):
    #Read matched order message
    if(controlledInput == '2E' and len(input) == 40 and str(input[0:2] == '2E')):
        message = input[0:2]
        firm = input[2:5]
        side = input[5:6]
        orderNO = input[6:14]
        orderEntryDate = input[14:18]
        filler = input[18:20]
        volume = input[20:28]
        price = input[28:34]
        confirmNO = input[34:40]
        return [message, firm, side, orderNO, orderEntryDate, volume, price, confirmNO]

    #Read reject messsage
    elif (controlledInput == '2G' and len(input) == 240 and str(input[0:2] == '2G')):
        message = input[0:2]
        firm = input[2:5]
        reasonCode = input[5:7]
        originalMessage = input[7:240]  
        return [message, firm, reasonCode, originalMessage]

    #Read confirmed cancel message
    elif (controlledInput == '2C' and len(input) == 26 and str(input[0:2] == '2C')):
        message = input[0:2]
        firm = input[2:5]
        cancelShares = input[5:13]
        orderNO = input[13:21]
        orderEntryDate = input[21:25]
        orderCancelStatus = input[25:26]
        return [message, firm, cancelShares, orderNO, orderEntryDate,orderCancelStatus]
    else:
            return -1  # should raise exception rather than -1

def read2G(input):
    return 0

def read2E(input):
    return 0

path = "D:/Logs/15-08/"
linkID = 1
#idx1 = parseIdx(path, linkID)
parseLog('i', path, linkID)