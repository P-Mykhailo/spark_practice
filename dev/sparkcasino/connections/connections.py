import findspark

findspark.init()
import pyspark

spark = pyspark.sql.SparkSession.builder.master("local").appName("spark").getOrCreate()

class InputData():

    def curexdf(self):
        curexdf = spark. \
            read. \
            format("csv"). \
            option("inferSchema", "true"). \
            option("header", "true"). \
            load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/CurrencyExchange.csv")
        return curexdf

    def gamedf(self):
        gamedf = spark. \
            read. \
            format("csv"). \
            option("inferSchema", "true"). \
            option("header", "true"). \
            load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/Game.csv")
        return gamedf

    def gamecatdf(self):
        gamecatdf = spark. \
            read. \
            format("csv"). \
            option("inferSchema", "true"). \
            option("header", "true"). \
            load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/GameCategory.csv")
        return gamecatdf

    def gameprovdf(self):
        gameprovdf = spark. \
            read. \
            format("csv"). \
            option("inferSchema", "true"). \
            option("header", "true"). \
            load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/GameProvider.csv")
        return gameprovdf

    @classmethod
    def gametransdf(cls):
        schema_def = pyspark.sql.types.StructType()  # Created a StructType object
        schema_def.add("Date", "date", True)
        schema_def.add("realAmount", "integer", True)
        schema_def.add("bonusAmount", "integer", True)
        schema_def.add("channelUID", "string", True)
        schema_def.add("txCurrency", "string", True)
        schema_def.add("gameID", "integer", True)
        schema_def.add("txType", "string", True)
        schema_def.add("BetId", "integer", True)
        schema_def.add("PlayerId", "integer", True)
        gametransdf = spark. \
            read. \
            format("csv"). \
            option("header", "true"). \
            option("delimiter", ";"). \
            load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/GameTransaction.csv",
                 schema=schema_def)
        return gametransdf

    @classmethod
    def playerdf(cls):
        playerdf = spark. \
            read. \
            format("csv"). \
            option("inferSchema", "true"). \
            option("header", "true"). \
            load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/Player.csv")
        return playerdf


gametransdf1 = InputData.gametransdf().createOrReplaceTempView("gametransdf1")
playerdf1 = InputData.playerdf().createOrReplaceTempView("playerdf1")
#gcount = InputData.gametransdf().join(InputData.playerdf(), InputData.gametransdf().PlayerId == InputData.playerdf().playerID, how='inner'). \
#    groupby("country").count().orderBy('count', ascending=False).limit(5)

a = InputData.gametransdf()
b = InputData.playerdf()
gcount = a.join(b, a.PlayerId == b.playerID, how = 'inner'). \
    groupby("country").count

countgrres = spark.sql("""
    with wag AS (SELECT (sum(realamount) + sum(bonusamount)) AS summawag, country, txType 
    FROM playerdf1 a INNER JOIN gametransdf1 b ON a.playerID = b.PlayerId 
    WHERE txType = "WAGER"
    GROUP BY country, txType),
    res as (SELECT (sum(realamount) + sum(bonusamount)) AS summares, country, txType 
    FROM playerdf1 a INNER JOIN gametransdf1 b ON a.playerID = b.PlayerId 
    WHERE txType = "RESULT"
    GROUP BY country, txType)
    SELECT wag.country, (summares - summawag) AS summa 
    FROM wag INNER JOIN res ON wag.country = res.country
    ORDER BY summa DESC
    LIMIT 5; 
""").show()

