#!/user/bin/env python
#coding: 'utf-8'

class Solution:
	def rotate(self,nums,k):
		n = len(nums)
		k %= n
		self.reverse(nums,0,n-k)
		self.reverse(nums,n-k,n)
		self.reverse(nums,0,n)
		print (nums)


	def reverse(self,nums,start,end):
		for x in range(start,(start+end)/2):
			nums[x],nums[start + end -x -1]=nums[start + end -x -1],nums[x]


if __name__ == '__main__':
	print (Solution().rotate([1,2,3,4,5,6,7],4))