def insert_sort(list):
	for index in range(1,len(list)):
		value =list[index]
		i=index-1
		while i>=0:
			if value < list[i]:
				list[i+1]=list[i]
				list[i]=value
				i=i-1
			else:
				break

a=[2,3,5,8,45,2,4,9]
insert_sort(a)
print a