# House Rate of Taiwan

示範 Spark 計算台灣各地不動產每坪單價以及視覺化。

![Visualization of House Rate of Taiwan](http://i.imgur.com/G75PIbA.png)
## Requirement

[Spark 2.0.2](http://spark.apache.org/)

## Data Source

1. [不動產買賣實價登錄批次資料](http://data.gov.tw/node/6213)
2. [jason2506/Taiwan.TopoJSON](https://github.com/jason2506/Taiwan.TopoJSON)

## Usage

1. 產生資料: `make generate_data`
2. 執行 http server: `make run_server`
	* 結果網址: `http://${hostname}:8080/v.html`
