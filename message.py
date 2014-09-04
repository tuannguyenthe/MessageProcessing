__author__ = 'TuanNT'


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


def parse2E(input):
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

def parse2G(input):
    message = input[0:2]
    firm = input[2:5]
    reasonCode = input[5:7]
    originalMessage = input[7:240]
    return [message, firm, reasonCode, originalMessage]

# A bunch of constant for message types
messagelength = {'1C': 17, '1D': 44, '1E': 66, '1F': 191, '1G': 120, '1I': 70, '2B':17, '2C': 26, '2D': 50,
                 '2E': 40, '2F': 52, '2G': 240, '2I': 49, '2L': 40, '3A': 82, '3B': 95, '3C': 27, '3D': 12,
                 'RN': 484, 'RP': 484, 'RQ': 11, 'AA': 52, 'BR': 53}
non_broker_list = [13, 25, 31, 34, 51, 52, 53, 55, 60, 63, 78, 98, 100, 103]
last_broker = 106