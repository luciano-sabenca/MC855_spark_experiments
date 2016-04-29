import plotly.plotly as py
import csv
import random

cities = {}
with open('optimized_localization/part-00000', 'rb') as csvfile:
	spamreader = csv.reader(csvfile, delimiter=',')
	i = 0
	for row in spamreader:
		if i == 0:
			i += 1
		else:
			if not row[3] in cities:
				city = dict(
					lat = float(row[5]),
					lon = float(row[6]),
					size = 1,
					name = row[3]
				)
				cities[row[3]] = city
			else:
				cities[row[3]]['size'] = cities[row[3]]['size'] + 1

def toStr(size, num):
	s = str(num)
	while len(s) < size:
		s = '0' + s
	return s

#for name in cities:
#	print toStr(6, cities[name]['size']) + '\t' + cities[name]['name']
total = 0
totalC = 0
for name in cities:
	totalC += 1
	total += cities[name]['size']

print total
print totalC
data = [0,0,0,0,0,0]
data2 = [0,0,0,0,0,0]
for name in cities:
	i = -1
	s = cities[name]['size']
	if s == 1:
		i = 0
	elif s < 10:
		i = 1
	elif s < 100:
		i = 2
	elif s < 1000:
		i = 3
	elif s < 10000:
		i = 4
	else:
		i = 5

	data[i] = data[i] + 1
	data2[i] = data2[i] + s

print data
print data2

for i in range(0, len(data)):
	data[i] = data[i] * 1.0 / totalC
	data2[i] = data2[i] * 1.0 / total

print data
print data2
