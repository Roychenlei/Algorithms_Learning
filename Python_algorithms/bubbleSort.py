#!/usr/bin/env python  
#-*-encoding:utf-8  
  
#BubbleSort  
def bubbleSort(bubble_list):
	list_length = len(bubble_list)

	while list_length > 0:

		for i in range(list_length-1):

			if bubble_list[i] > bubble_list[i+1]:

				bubble_list[i], bubble_list[i+1] = bubble_list[i+1], bubble_list[i] 

		list_length -= 1

	print (bubble_list)

  
  
if __name__=="__main__":
	bubbleSort([1,2,3,3,4,15,2,3])

