import findspark
findspark.init()
import pyspark

spark = pyspark.sql.SparkSession.builder.master("local").appName("spark").getOrCreate()
class inputdata():


    def curexdf(self):
        curexdf = spark.read.format("csv").option("inferSchema", "true").option("header", "true"). \
        load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/CurrencyExchange.csv")
        return curexdf

    def gamedf(self):
        gamedf = spark.read.format("csv").option("inferSchema", "true").option("header", "true"). \
        load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/Game.csv")
        return gamedf

    def gamecatdf(self):
        gamecatdf = spark.read.format("csv").option("inferSchema", "true").option("header", "true"). \
        load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/GameCategory.csv")
        return gamecatdf

    def gameprovdf(self):
        gameprovdf = spark.read.format("csv").option("inferSchema", "true").option("header", "true"). \
        load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/GameProvider.csv")
        return gameprovdf

    def gametransdf(self):
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
        gametransdf = spark.read.format("csv").option("header", "true"). \
        option("delimiter", ";"). \
        load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/GameTransaction.csv", schema=schema_def)
        return gametransdf

    def playerdf(self):
        playerdf = spark.read.format("csv").option("inferSchema", "true").option("header", "true"). \
        load("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcasino/input-tables/Player.csv")

    gametransdf.createOrReplaceTempView("gametransdf1")
    playerdf.createOrReplaceTempView("playerdf1")

# Top 5 Countries by Games Count
gcount = gametransdf.join(playerdf, gametransdf.PlayerId == playerdf.playerID, how='inner'). \
    groupby("country").count().orderBy('count', ascending=False).limit(5)
gcount.toPandas().to_csv("FirstDataset.csv", index = False)
#gcount.write.format("com.databricks.spark.csv").save("C:/Users/mpuga/PycharmProjects/pythonProject/venv/FirstDataset.csv")

# Top 5 Countries by GrossResult
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
#countgrres.show()

# Top 5 Players by GrossResult
peoplegrres = spark.sql("""
    with wag AS (SELECT (sum(realamount) + sum(bonusamount)) AS summawag, a.playerID, txType 
    FROM playerdf1 a INNER JOIN gametransdf1 b ON a.playerID = b.PlayerId 
    WHERE txType = "WAGER"
    GROUP BY a.playerID, txType),
    res as (SELECT (sum(realamount) + sum(bonusamount)) AS summares, a.playerID, txType 
    FROM playerdf1 a INNER JOIN gametransdf1 b ON a.playerID = b.PlayerId 
    WHERE txType = "RESULT"
    GROUP BY a.playerID, txType)
    SELECT wag.playerID, (summares - summawag) AS summa 
    FROM wag INNER JOIN res ON wag.playerID = res.playerID
    ORDER BY summa DESC
    LIMIT 5; 
""").show()
#peoplegrres.show()

# Players count by gender
countgend = playerdf.groupby("gender").count().show()

"""curexdf.printSchema()
curexdf.show()
gamedf.printSchema()
gamedf.show()
gamecatdf.printSchema()
gamecatdf.show()
gameprovdf.printSchema()
gameprovdf.show()
gametransdf.printSchema()
gametransdf.show()
playerdf.printSchema()
playerdf.show()"""
