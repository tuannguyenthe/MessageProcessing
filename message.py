__author__ = 'TuanNT'

def parse1I(input):
    message = input[0:2].strip()
    firm = input[2:5].strip()
    traderID = input[5:9].strip()
    orderNO = input[9:17].strip()
    clientID = input[17:27].strip()
    symbol = input[27:35].strip()
    side = input[35:36].strip()
    volume = input[36:44].strip()
    publishedVolume = input[44:52].strip()
    price = input[52:58].strip()
    board = input[58:59].strip()
    clientFlag = input[64:65].strip()
    return [message, firm, traderID, orderNO, clientID,
            symbol, side,volume, publishedVolume, price, board ,clientFlag]

def parse1C(input):
    message = input[0:2]
    firm = input[2:5]
    orderNO = input[5:13]
    entryDate = input[13:17]
    return [message, firm, orderNO, entryDate]



def parse2C(input):
    message = input[0:2].strip()
    firm = input[2:5].strip()
    cancelShares = input[5:13].strip()
    orderNO = input[13:21].strip()
    orderEntryDate = input[21:25].strip()
    orderCancelStatus = input[25:26].strip()
    return [message, firm, cancelShares, orderNO, orderEntryDate,orderCancelStatus]

def parse2B(input):
    message = input[0:2].strip()
    firm = input[2:5].strip()
    orderNo = input[5:13].strip()
    entryDate = input[13:17].strip()
    return [message, firm, orderNo, entryDate]

def parse2E(input):
    message = input[0:2].strip()
    firm = input[2:5].strip()
    side = input[5:6].strip()
    orderNO = input[6:14].strip()
    orderEntryDate = input[14:18].strip()
    filler = input[18:20].strip()
    volume = input[20:28].strip()
    price = input[28:34].strip()
    confirmNO = input[34:40].strip()
    return [message, firm, side, orderNO, orderEntryDate, volume, price, confirmNO]



def parse2G(input):
    message = input[0:2].strip()
    firm = input[2:5].strip()
    reasonCode = input[5:7].strip()
    originalMessage = input[7:240].strip()
    return [message, firm, reasonCode, originalMessage]

