make all: generate_data run_server

generate_data:
	mkdir -p Taiwan.rate/towns 
	spark-submit main.py

run_server:
	python -m SimpleHTTPServer 8080
clean:
	rm -r spark-warehouse
	rm -r Taiwan.rate
