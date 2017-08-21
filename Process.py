import csv
import sys
import re
from Read import IOClass
from collections import OrderedDict

class QueryProcessor:
	def __init__(self):
		self.IO = IOClass()
		self.dictionary = self.IO.readMetadata()

	def processQuery(self, query):
		query = (re.sub(' +', ' ', query)).strip()

		if "from" in query:
			obj1 = query.split('from')
		else:
			sys.exit("Incorrect Syntax")

		obj1[0] = (re.sub(' +', ' ', obj1[0])).strip()

		if "select" not in obj1[0].lower():
			sys.exit("Incorrect Syntax")
		object1 = obj1[0][7:]

		object1 = (re.sub(' +', ' ', object1)).strip()
		l = []
		l.append("select")

		if "distinct" in object1 and "distinct(" not in object1:
			object1 = object1[9:]
			l.append("distinct")

		l.append(object1)
		object1 = l

		# select distinct List<colnames> from <table>
		object3 = ""
		if "distinct" in object1[1] and "distinct(" not in object1[1]:
			object3 = object1[1]
			object3 = (re.sub(' +', ' ', object3)).strip()
			object1[1] = object1[2]

		colStr = object1[1]
		colStr = (re.sub(' +', ' ', colStr)).strip()
		columnNames = colStr.split(',')
		for i in columnNames:
			columnNames[columnNames.index(i)] = (re.sub(' +', ' ', i)).strip()

		obj1[1] = (re.sub(' +', ' ', obj1[1])).strip()
		object2 = obj1[1].split('where')

		tableStr = object2[0]
		tableStr = (re.sub(' +', ' ', tableStr)).strip()
		tableNames = tableStr.split(',')
		for i in tableNames:
			tableNames[tableNames.index(i)] = (re.sub(' +', ' ', i)).strip()
		for i in tableNames:
			if i not in self.dictionary.keys():
				sys.exit("Table not found")

		if len(object2) > 1 and len(tableNames) == 1:
			object2[1] = (re.sub(' +', ' ', object2[1])).strip()
			self.processWhere(object2[1], columnNames, tableNames)
			return
		elif len(object2) > 1 and len(tableNames) > 1:
			object2[1] = (re.sub(' +', ' ', object2[1])).strip()
			self.processWhereJoin(object2[1], columnNames, tableNames)
			return

		if (len(tableNames) > 1):
			self.join(columnNames, tableNames)
			return

		if object3 == "distinct":
			self.distinctMany(columnNames, tableNames)
			return

		if len(columnNames) == 1:
			# aggregate -- Assuming (len(columnNames) == 1) i.e aggregate function
			for col in columnNames:
				if '(' in col and ')' in col:
					funcName = ""
					colName = ""
					a1 = col.split('(')
					funcName = (re.sub(' +', ' ', a1[0])).strip()
					colName = (re.sub(' +', ' ', a1[1].split(')')[0])).strip()
					self.aggregate(funcName, colName, tableNames[0])
					return
				elif '(' in col or ')' in col:
					sys.exit("Syntax error")

		self.selectColumns(columnNames, tableNames)

	def processWhere(self,whereStr,columnNames,tableNames):
		a = whereStr.split(" ")

		# print a

		if(len(columnNames) == 1 and columnNames[0] == '*'):
			columnNames = self.dictionary[tableNames[0]]

		self.IO.printHeader(columnNames,tableNames)

		tName = tableNames[0] + '.csv'
		fileData = []
		self.IO.readFile(tName,fileData)

		check = 0
		for data in fileData:
			string = self.evaluate(a,tableNames,data)
			for col in columnNames:
				if eval(string):
					check = 1
					print data[self.dictionary[tableNames[0]].index(col)],
			if check == 1:
				check = 0
				print

	def evaluate(self, a,tableNames,data):
		string = ""
		for i in a:
			# print i
			if i == '=':
				string += i*2
			elif i in self.dictionary[tableNames[0]] :
				string += data[self.dictionary[tableNames[0]].index(i)]
			elif i.lower() == 'and' or i.lower() == 'or':
				string += ' ' + i.lower() + ' '
			else:
				string += i
			# print string
		return string


	def processWhereJoin(self,whereStr,columnNames,tableNames):
		tableNames.reverse()

		l1 = []
		l2 = []
		self.IO.readFile(tableNames[0] + '.csv',l1)
		self.IO.readFile(tableNames[1] + '.csv',l2)

		fileData = []
		for item1 in l1:
			for item2 in l2:
				fileData.append(item2 + item1)

		# dictionary["sample"] = dictionary[b] + dictionary[a]
		self.dictionary["sample"] = []
		for i in self.dictionary[tableNames[1]]:
			self.dictionary["sample"].append(tableNames[1] + '.' + i)
		for i in self.dictionary[tableNames[0]]:
			self.dictionary["sample"].append(tableNames[0] + '.' + i)

		self.dictionary["test"] = self.dictionary[tableNames[1]] + self.dictionary[tableNames[0]]

		tableNames.remove(tableNames[0])
		tableNames.remove(tableNames[0])
		tableNames.insert(0,"sample")

		if(len(columnNames) == 1 and columnNames[0] == '*'):
			columnNames = self.dictionary[tableNames[0]]

		# print header
		for i in columnNames:
			print i,
		print

		a = whereStr.split(" ")

		# check = 0
		# for data in fileData:
		# 	string = evaluate(a,tableNames,dictionary,data)
		# 	for col in columnNames:
		# 		if eval(string):
		# 			check = 1
		# 			print data[dictionary[tableNames[0]].index(col)],
		# 	if check == 1:
		# 		check = 0
		# 		print

		check = 0
		for data in fileData:
			string = self.evaluate(a,tableNames,data)
			for col in columnNames:
				if eval(string):
					check = 1
					if '.' in col:
						print data[self.dictionary[tableNames[0]].index(col)],
					else:
						print data[self.dictionary["test"].index(col)],
			if check == 1:
				check = 0
				print

		del self.dictionary['sample']


	def selectColumns(self, columnNames, tableNames):
		if len(columnNames) == 1 and columnNames[0] == '*':
			columnNames = self.dictionary[tableNames[0]]

		for i in columnNames:
			if i not in self.dictionary[tableNames[0]]:
				sys.exit("error")

		self.IO.printHeader(columnNames, tableNames)

		tName = tableNames[0] + '.csv'
		fileData = []
		self.IO.readFile(tName, fileData)

		self.IO.printData(fileData, columnNames, tableNames)


	def join(self, columnNames,tableNames):
		tableNames.reverse()

		l1 = []
		l2 = []
		self.IO.readFile(tableNames[0] + '.csv',l1)
		self.IO.readFile(tableNames[1] + '.csv',l2)

		fileData = []
		for item1 in l1:
			for item2 in l2:
				fileData.append(item2 + item1)

		# dictionary["sample"] = dictionary[b] + dictionary[a]
		self.dictionary["sample"] = []
		for i in self.dictionary[tableNames[1]]:
			self.dictionary["sample"].append(tableNames[1] + '.' + i)
		for i in self.dictionary[tableNames[0]]:
			self.dictionary["sample"].append(tableNames[0] + '.' + i)

			self.dictionary["test"] = self.dictionary[tableNames[1]] + self.dictionary[tableNames[0]]
		# print dictionary["test"]

		tableNames.remove(tableNames[0])
		tableNames.remove(tableNames[0])
		tableNames.insert(0,"sample")

		if(len(columnNames) == 1 and columnNames[0] == '*'):
			columnNames = self.dictionary[tableNames[0]]

		# print header
		for i in columnNames:
			print i,
		print

		# printData(fileData,columnNames,tableNames,dictionary)

		for data in fileData:
			for col in columnNames:
				if '.' in col:
					print data[self.dictionary[tableNames[0]].index(col)],
				else:
					print data[self.dictionary["test"].index(col)],
			print

		# del dictionary[tableNames[0]]


	def aggregate(self, func,columnName,tableName):

		if columnName == '*':
			sys.exit("error")
		if columnName not in self.dictionary[tableName]:
			sys.exit("error")

		tName = tableName + '.csv'
		fileData = []
		self.IO.readFile(tName,fileData)
		colList = []
		for data in fileData:
			colList.append(int(data[self.dictionary[tableName].index(columnName)]))

		if func.lower() == 'max':
			print max(colList)
		elif func.lower() == 'min':
			print min(colList)
		elif func.lower() == 'sum':
			print sum(colList)
		elif func.lower() == 'avg':
			print sum(colList)/len(colList)
		elif func.lower() == 'distinct':
			self.distinct(colList,columnName,tableName)
		else :
			print "ERROR"
			print "Unknown function : ", '"' + func + '"'


	def distinct(self, colList, columnName, tableName):
		print "OUTPUT :"
		string = tableName + '.' + columnName
		print string

		colList = list(OrderedDict.fromkeys(colList))
		for col in range(len(colList)):
			print colList[col]


	def distinctMany(self, columnNames, tableNames):
		self.IO.printHeader(columnNames, tableNames)

		temp = []
		check = 0
		for tab in tableNames:
			tName = tab + '.csv'
			with open(tName, 'rb') as f:
				reader = csv.reader(f)
				for row in reader:
					for col in columnNames:
						x = row[self.dictionary[tableNames[0]].index(col)]
						if x not in temp:
							temp.append(x)
							check = 1
							print x,
					if check == 1:
						check = 0
						print
