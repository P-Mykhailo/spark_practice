import findspark
findspark.init()
import pyspark
from dev.sparkcasino.connections.connections import InputData

spark = pyspark.sql.SparkSession.builder.master("local").appName("spark").getOrCreate()

gametransdf = InputData.gametransdf()
playerdf = InputData.playerdf()
gametransdf.createOrReplaceTempView("gametransdf1")
playerdf.createOrReplaceTempView("playerdf1")

# Top 5 Countries by Games Count
gcount = gametransdf.join(playerdf, gametransdf.PlayerId == playerdf.playerID, how='inner'). \
    groupby("country").count().orderBy('count', ascending=False).limit(5)
gcount.toPandas().to_csv("outputtables/top_5_count_by_game.csv", index = False)

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
""")
countgrres.toPandas().to_csv("outputtables/top_5_count_by_gross_res.csv", index = False)
countgrres.show()


# # Top 5 Players by GrossResult
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
""")
peoplegrres.toPandas().to_csv("outputtables/top_5_players_by_gross_res.csv", index = False)
peoplegrres.show()

# Players count by gender
countgend = playerdf.groupby("gender").count().show()







