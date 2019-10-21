import csv
import numpy as np
with open('GlobalDB.csv','r') as csvfile:
    reader = list(csv.reader(csvfile))
    rows= [row for row in reader[0:15]]
print (rows)
print("\n")#输出所有数据
# data=np.array(rows)#rows是数据类型是‘list',转化为数组类型好处理
# print("out0=",type(data),data.shape)
# print("out1=",data)