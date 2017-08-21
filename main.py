from Process import *

def main():

	query = QueryProcessor()
	query.processQuery(str(sys.argv[1]))

if __name__ == "__main__":
	main()
