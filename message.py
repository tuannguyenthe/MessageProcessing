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

# A bunch of constant for message types
messagelength = {'1C':17, '1D':44, '1E':66, '1F':191, '1G': 120, '1I':70, '2B':17, '2C': 26, '2D':50,
                 '2E':40, '2F':52, '2G': 240, '2I':49, '2L': 40, '3A':82, '3B': 95, '3C':27, '3D': 12,
                 'RN': 484, 'RP':484, 'RQ': 11, 'AA': 52, 'BR': 53}