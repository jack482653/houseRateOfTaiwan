# -*- coding: utf-8 -*-
# import uniout
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, udf
from pyspark.sql.types import StructType, StructField, StringType
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

countrys = {
    u'C': u'基隆市', u'A': u'臺北市', u'F': u'新北市', u'H': u'桃園市',
    u'O': u'新竹市', u'J': u'新竹縣', u'K': u'苗栗縣', u'B': u'臺中市',
    u'M': u'南投縣', u'N': u'彰化縣', u'P': u'雲林縣', u'I': u'嘉義市',
    u'Q': u'嘉義縣', u'D': u'臺南市', u'E': u'高雄市', u'T': u'屏東縣',
    u'G': u'宜蘭縣', u'U': u'花蓮縣', u'V': u'臺東縣', u'X': u'澎湖縣',
    u'W': u'金門縣', u'Z': u'連江縣'
}

types = {'A': '不動產買賣', 'B': '預售屋買賣', 'C': '不動產租賃'}

# Initilize SparkContext and SparkSession
conf = SparkConf().setAppName('Hello Spark').setMaster('local[*]')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName(
        'Hello Spark'
    ).master('local[*]').getOrCreate()

rawDeals = sc.textFile('lvr_landcsv/[A-Z]_LVR_LAND_A\.CSV\.utf8\.2')

cleanDeals = rawDeals.map(
    lambda x: x.strip().split(',')
).filter(
    lambda x: len(x) == 29
)

# The schema is encoded in a string.
schemaString = '縣市,鄉鎮市區,交易標的,土地區段位置或建物區門牌,' \
    '土地移轉總面積平方公尺,都市土地使用分區,非都市土地使用分區,' \
    '非都市土地使用編定,交易年月日,交易筆棟數,移轉層次,總樓層數,' \
    '建物型態,主要用途,主要建材,建築完成年月,建物移轉總面積平方公尺,' \
    '建物現況格局-房,建物現況格局-廳,建物現況格局-衛,建物現況格局-隔間,' \
    '有無管理組織,總價元,單價每平方公尺,車位類別,車位移轉總面積平方公尺,' \
    '車位總價元,備註,編號'
fields = [
    StructField(
        field_name, StringType(), True
    ) for field_name in schemaString.split(',')
]
schema = StructType(fields)

# Apply the schema to the RDD.
dealsDF = ss.createDataFrame(cleanDeals, schema)
# schemaDeals.show()
# Creates a temporary view using the DataFrame
# schemaDeals.createOrReplaceTempView('deals')
udf_to_countrys = udf(lambda x: countrys[x], StringType())
udfstring_to_float = udf(lambda x: float(x), StringType())
dealsDF2 = dealsDF.select(
    '縣市', '鄉鎮市區', '交易標的', '土地移轉總面積平方公尺',
    '總價元', '單價每平方公尺'
).filter(
    dealsDF['交易標的'] == '房地(土地+建物)'
).replace('', '0').withColumn(
    '縣市', udf_to_countrys('縣市')
).withColumn(
    '土地移轉總面積平方公尺', udfstring_to_float('土地移轉總面積平方公尺')
).withColumn(
    '總價元', udfstring_to_float('總價元')
).withColumn(
    '單價每平方公尺',
    udfstring_to_float('單價每平方公尺')
)

dealsDF3 = dealsDF2.withColumn(
    '單價每坪',
    when(
        dealsDF2['單價每平方公尺'] == 0.0,
        3.3058 * dealsDF2['總價元'] / dealsDF2['土地移轉總面積平方公尺']
    ).otherwise(3.3058 * dealsDF2['單價每平方公尺'])
)

# Calculate rate per county
countiesAvgRDD = dealsDF3.groupBy('縣市').agg({'單價每坪': 'mean'}).rdd
countiesAvgDict = countiesAvgRDD.map(
    lambda x: (x['縣市'], x['avg(單價每坪)'])
).collectAsMap()
with open('Taiwan.rate/countiesAvg.json', 'w') as outfile:
    json.dump(countiesAvgDict, outfile, ensure_ascii=False)

# Calculate rate per town
townsAvgRDD = dealsDF3.groupBy(
    '鄉鎮市區', '縣市'
).agg({'單價每坪': 'mean'}).rdd
townsAvgs = townsAvgRDD.map(
    lambda x: (x['縣市'], (x['鄉鎮市區'], x['avg(單價每坪)']))
).groupByKey().mapValues(dict).collect()
for k, v in townsAvgs:
    filename = 'Taiwan.rate/towns/%s-townsAvg.json' % (k)
    with open(filename, 'w') as outfile:
        json.dump(v, outfile, ensure_ascii=False)
