def fib(n):
	if n==0 or n==1:
		return 1
	else:
		return fib(n-2)+fib(n-1)

print [fib(n) for n in range(10)]