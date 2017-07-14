#!/user/bin/env python
#coding: 'utf-8'

perm_lst = []

def permutation(lst):
	if  lst == []:
		print perm_lst
	else:
		for elem in lst:
			perm_lst.append(elem)
			rest_lst = lst[:]
			rest_lst.remove(elem)
			permutation(rest_lst)
			perm_lst.pop()

if __name__ == '__main__':
	permutation(list('abc'))