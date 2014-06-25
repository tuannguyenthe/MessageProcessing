__author__ = 'tuannt-qltv'
from xml.dom import minidom

xmldoc = minidom.parse('./xml/elements.xml')
itemlist = xmldoc.getElementsByTagName('firm')
print(itemlist[0].attributes['length'].value)
print(itemlist[0].attributes['type'].value)



