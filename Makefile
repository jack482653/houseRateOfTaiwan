make all: generate_data run_server

generate_data:
	spark-submit main.py

run_server:
	python -m SimpleHTTPServer 8080
clean:
	rm -r spark-warehouse
	rm countrysAvg.json
