#!/user/bin/env python
#coding: 'utf-8'
import os,sys
perm_lst = []

def permutation(lst):
	if  lst == []:
		print "result:",perm_lst
	else:
		for elem in lst:
			perm_lst.append(elem)
			rest_lst = lst[:]
			print "================="
			print "perm_list1",perm_lst
			print "rest_list:",rest_lst
			print "================="
			rest_lst.remove(elem)
			permutation(rest_lst)
			sleep(1000)
			perm_lst.pop()
			print "perm_list2",perm_lst

if __name__ == '__main__':
	permutation(list('abc'))