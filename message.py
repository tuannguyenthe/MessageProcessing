__author__ = 'TuanNT'

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

