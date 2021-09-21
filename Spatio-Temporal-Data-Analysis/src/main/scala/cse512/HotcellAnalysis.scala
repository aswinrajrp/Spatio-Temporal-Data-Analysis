package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickupInfoView")
  
  spark.udf.register("checkCellBoundary", (x: Double, y: Double, z: Int) => (x >= minX && x <= maxX && y >= minY && y <= maxY && z >= minZ && z <= maxZ))
  
  var pickupsInsideBoundary = spark.sql("select x, y, z, count(*) as noOfPickups from pickupInfoView where checkCellBoundary(x, y, z) group by z, y, x order by z, y, x")
  pickupsInsideBoundary.createOrReplaceTempView("pickupsInsideBoundaryView")
  //pickupsInsideBoundary.show()
  
  spark.udf.register("square", (a: Int) => (a*a).toDouble)
  
  var sumAndSumOfSquaresOfPickups = spark.sql("select sum(noOfPickups) as sumOfPickups, sum(square(noOfPickups)) as sumOfSquaresOfPickups from pickupsInsideBoundaryView")
  sumAndSumOfSquaresOfPickups.createOrReplaceTempView("sumAndSumOfSquaresOfPickupsView")
  //sumAndSumOfSquaresOfPickups.show()
  
  var sumOfPickups = sumAndSumOfSquaresOfPickups.first().getLong(0).toDouble
  var sumOfSquaresOfPickups = sumAndSumOfSquaresOfPickups.first().getDouble(1)
  
  var meanOfPickups = sumOfPickups / numCells
  var s = math.sqrt((sumOfSquaresOfPickups / numCells) - (meanOfPickups * meanOfPickups))
  
  spark.udf.register("getNeighborCellsCount", (x: Double, y: Double, z: Int) => (HotcellUtils.getNeighborCellsCount(x, y, z, minX, maxX, minY, maxY, minZ, maxZ)))
  
  var neighborDetails = spark.sql("select a.x, a.y, a.z, getNeighborCellsCount(a.x, a.y, a.z) as neighborCellsCount, sum(b.noOfPickups) as neighborCellsPickupSum from pickupsInsideBoundaryView a, pickupsInsideBoundaryView b where abs(a.x - b.x) <= 1 and abs(a.y - b.y) <= 1 and abs(a.z - b.z) <= 1 group by a.z, a.y, a.x")
  neighborDetails.createOrReplaceTempView("neighborDetailsView")
  
  spark.udf.register("computeGStatisticZScore", (neighborCellsCount: Int, neighborCellsPickupSum: Int, meanOfPickups: Double, s: Double, numCells: Int) => (HotcellUtils.computeGStatisticZScore(neighborCellsCount, neighborCellsPickupSum, meanOfPickups, s, numCells)))
  
  var gScore = spark.sql("select *, computeGStatisticZScore(neighborCellsCount, neighborCellsPickupSum, " +  meanOfPickups + ", " + s + ", " + numCells + ") as gScore from neighborDetailsView order by gScore desc")
  gScore.createOrReplaceTempView("gScoreView")
  //gScore.show()
  
  var finalOutput = spark.sql("select x, y, z from gScoreView order by gScore desc")
  
  return finalOutput // YOU NEED TO CHANGE THIS PART
}
}
