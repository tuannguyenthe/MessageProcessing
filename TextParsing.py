import struct
import datetime
import os
from pymongo import *


def toInt(byte):
    return int.from_bytes(byte, byteorder='big',signed=False)

def readHeader(input):
    if(len(input) != 12):
        return -1
    else:
        lastUpdate = [toInt(input[0:4]), toInt(input[4:8])]
        lastSequence = toInt(input[8:12])
        return [lastUpdate, lastSequence]

def readRecord(input):
    if(len(input) !=24):
        return -1
    else:
        timeStamp = [toInt(input[0:4]), toInt(input[4:8])]
        sequence = toInt(input[8:12])
        opcode = toInt(input[12:16])
        position = toInt(input[16:20])
        contSize = toInt(input[20:22])
        return [timeStamp, sequence, opcode, position, contSize]

def parseIdx(direction, path, linkID):
    fileIdx = direction + 'cont' + str(linkID) + '.idx'
    fileIdxsize = os.path.getsize(path + fileIdx)
    fileIdxHandler = open(path + fileIdx, "rb")
    #print('Filesize is: {0} and Valid: {1} '.format(fileIdxsize, (fileIdxsize - 12)%24==0))
    readHeader(fileIdxHandler.read(12))
    output = []
    rec=0
    while rec!= -1:
        rec = readRecord(fileIdxHandler.read(24))
        if rec != -1:
            output.append(rec)
            #print(rec)
    fileIdxHandler.close()
    return output

def parseLog(posts, direction, path, linkID, idx):
    fileLog = direction + 'cont'+ str(linkID) + '.log'
    fileLogsize = os.path.getsize(path + fileLog)
    fileLogHandler = open(path + fileLog, "r", encoding="latin1")
    #print('Filesize is: {0}'.format(fileLogsize))
    output = []
    while idx != []:
        idxSequence = idx.pop()
        fileLogHandler.seek(idxSequence[3],0)
        currentSequence = fileLogHandler.read(idxSequence[4])
        str1 = ''
        post = {}
        
        #handle inbound messages
        while direction =='i' and (currentSequence.find('1I') != -1 or currentSequence.find('1C') != -1):
            if currentSequence.find('1I') > -1:
                str1 = currentSequence[0:currentSequence.find('1I')]
                post = readInbound('1I',currentSequence[currentSequence.find('1I'): currentSequence.find('1I')+70] )
                currentSequence = currentSequence[0:currentSequence.find('1I')] + currentSequence[currentSequence.find('1I') + 70: -1]
            if currentSequence.find('1C') > -1:
                str1 = currentSequence[0:currentSequence.find('1C')]
                readInbound('1C', currentSequence[currentSequence.find('1C'): currentSequence.find('1C')+17] )
                currentSequence = currentSequence[0:currentSequence.find('1C')] + currentSequence[currentSequence.find('1C') + 17: -1]
            #print(str1)
        
        #handle outbound messages
        while direction == 'o' and (currentSequence.find('2C') != -1 or currentSequence.find('2E') != -1 or currentSequence.find('2G') != -1):
            if currentSequence.find('2C') > -1:
                str1 = currentSequence[0:currentSequence.find('2C')]
                readOutbound('1I',currentSequence[currentSequence.find('2C'): currentSequence.find('2C')+26] )
                currentSequence = currentSequence[0:currentSequence.find('2C')] + currentSequence[currentSequence.find('1I') + 26: -1]
            if currentSequence.find('2E') > -1:
                str1 = currentSequence[0:currentSequence.find('2E')]
                readOutbound('2E', currentSequence[currentSequence.find('2E'): currentSequence.find('2E')+40] )
                currentSequence = currentSequence[0:currentSequence.find('2E')] + currentSequence[currentSequence.find('2E') + 40: -1]
            if currentSequence.find('2G') > -1:
                str1 = currentSequence[0:currentSequence.find('2G')]
                readOutbound('2G', currentSequence[currentSequence.find('2G'): currentSequence.find('2G')+ 240] )
                currentSequence = currentSequence[0:currentSequence.find('2G')] + currentSequence[currentSequence.find('2G') + 240: -1]
            #print(str1)
        
        if idxSequence[1] != 0 and post !=-1:
            datetime_temp = datetime.datetime.fromtimestamp(idxSequence[0][0])
            #datetime_temp.milisecond = idxSequence[0][1]
            post['datetime'] = datetime_temp
            post['sequenceNO'] = idxSequence[1]
            posts.insert(post)
    fileLogHandler.close()
    print(output)    
    return output

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
            return -1 # should raise exception rather than -1

def read2G(input):
    return 0

def read2E(input):
    return 0


client = MongoClient('localhost', 27017)
db = client['Message-Database']
collection = db['Message-Collection']
posts = db.posts

path = "E:/HOSE/Ho Tro Phan Mem/Check2G/"
linkID = 2
idx1 = parseIdx('i', path, linkID)
parseLog(posts, 'i', path, linkID, idx1 )
#idx2 = parseIdx('o', path, linkID)
#parseLog('o', path, linkID, idx2 )

