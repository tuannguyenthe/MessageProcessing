__author__ = 'TuanNT'

import datetime

class Messages:

    def __init__(self, orderNo):
        self.id = orderNo
        self.messages = []

    def insert(self, item):
        if(self.id == item.getOrderNo()):
            self.messages.insert(item)
        else:
            raise Exception

    def getOrderNo(self):
        return self.id

    def __str__(self):
        padding = ' '
        output = ''
        for item in self.messages:
            output = item + ''
            padding *= 2

        return output


class Message:

    def __init__(self, type, sequence, time, orderNo, content):
        self.type = type
        self.sequence = sequence
        self.time = time
        self.orderNo = orderNo
        self.content = content

    def getOrderNo(self):
        return self.orderNo

    def __str__(self):
        return '----< Sq[' + self.sequence + '], OP[DT] @' + datetime.datetime.fromtimestamp(self.time)
        + ' >----\n' + content + '\n|<END'

allMessages = { '1I': 70, '1C': 17, '1D':44, '1E': 66, '1F': 191, '1G': 120, '2B': 17, '2C': 26,
               '2D': 50, '2E': 40, '2F': 52, '2G': 240, '2I': 49, '2L': 40, '3A': 82, '3B': 95, '3C': 27,
               '3D': 12, 'RN': 484, 'RP':484, 'RQ': 11, 'AA': 52, 'BR': 53, 'BS': 6, 'CO': 5, 'DC': 16,
               'GA': 73, 'IU': 56, 'LO': 16, 'LS': 16, 'NH': 85, 'NS': 223, 'OL': 14, 'OS': 8, 'PD': 16,
               'PO': 8, 'SC': 6, 'SI':129, 'SR': 36, 'SS': 37, 'SU': 110, 'TC': 8, 'TP': 29, 'TR': 16, 'TS': 5
}

GReasonCodes = {
    '00': 'Lệnh MP nhưng không có lệnh đối ứng',
	'01': 'Sai bước giá',
	'02': 'Khối lượng đặt sai',
	'03': 'Yêu cầu không hợp lệ. Thị trường đã đóng cửa',
	'04': 'Sai mã chứng khoán',
	'05': 'Sai mã thành viên',
	'06': 'Sai mã trader',
	'07': 'Sai mã xác nhận',
	'08': 'Đã trễ để thực hiện yêu cầu trên',
	'09': 'Sai số tham chiếu',
	'10': '“Điều kiện sai',
	'11': 'Chứng khoán bị tạm dừng giao dịch',
	'12': 'Sai bảng giao dịch',
	'13': 'Thiếu thông tin mã khách hàng',
	'14': 'Sai loại lệnh',
	'15': 'Cờ PC nhập sai',
	'16': 'Reply Code hoặc Request Code bị sai',
	'17': 'Sai trường Side:chỉ được là mua(B),hoặc bán(S)',
	'18': 'Sai thông tin số hiệu lệnh',
	'19': 'Sai thông tin Time',
	'20': 'Sai thông tin Date',
	'24': 'Chứng khoán bị đình chỉ giao dịch',
	'25': 'Thiếu thông tin trường cờ PC',
	'27': 'Không còn room cho CCQ',
	'28': 'Thị trường đang trong giờ nghỉ',
	'29': 'Thị trường đang tạm ngừng giao dịch',
	'31': 'Không được phép thay đổi thông tin đối với lệnh đã khớp',
	'33': 'Không được phép giao dịch với CP này',
	'34': 'Giá nhập lớn hơn giá trần',
	'35': 'Giá nhập thấp hơn giá sàn',
	'36': 'Sai định dạng của giá trong lệnh thoả thuận',
    '37': 'Không được phép huỷ một lệnh đã được khớp',
	'38': 'Sai thông tin Volume trong giao dịch thoả thuận',
	'41': 'Sai thông tin Market ID',
	'42': 'Sai loại message - trường message type',
    '43': 'Thông tin về Chiều dài message \(Message Length\) sai',
    '44': 'Sai thông tin mã khách hàng',
    '45': 'Sai giá trị Filler',
	'99': 'Lỗi không xác định'
}