import csv
import sys
import re

class IOClass:
    def __init__(self):
        self.dictionary={}
        self.files={}
        f = open('./metadata.txt','r')
        check = 0
        for line in f:
            if line.strip() == "<begin_table>":
                check = 1
                continue
            if check == 1:
                tableName = line.strip()
                self.dictionary[tableName] = []
                check = 0
                continue
            if not line.strip() == '<end_table>':
                self.dictionary[tableName].append(line.strip())

    def readMetadata(self):
        return self.dictionary

    def readFile(self, tName, fileData):
        if tName in self.files:
            fileData = list(self.files[tName])
        else:
            with open(tName,'rb') as f:
                reader = csv.reader(f)
                for row in reader:
                    fileData.append(row)
            self.files[tName] = fileData

    def printHeader(self, columnNames, tableNames):
        print "OUTPUT : "
        # Table headers
        string = ""
        for col in columnNames:
            for tab in tableNames:
                if col in self.dictionary[tab]:
                    if not string == "":
                        string += ','
                    string += tab + '.' + col
        print string


    def printData(self, fileData, columnNames, tableNames):
        for data in fileData:
            for col in columnNames:
                print data[self.dictionary[tableNames[0]].index(col)],
            print

