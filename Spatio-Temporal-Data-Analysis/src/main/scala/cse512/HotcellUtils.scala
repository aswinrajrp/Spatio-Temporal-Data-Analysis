package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def getNeighborCellsCount(x: Double, y: Double, z: Int, minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Int, maxZ: Int): Int = {
	var count = 0
	count += (if(x == minX || x == maxX) 1 else 0)
	count += (if(y == minY || y == maxY) 1 else 0)
	count += (if(z == minZ || z == maxZ) 1 else 0)
	
	val cellPositionMap: Map[Int, String] = Map(0 -> "inside", 1 -> "face", 2 -> "edge", 3 -> "corner")
	val neighborCellsCountMap: Map[String, Int] = Map("inside" -> 26, "face" -> 17, "edge" -> 11, "corner" -> 7)
	
	var cellPosition = cellPositionMap.get(count).get
	var neighborCellsCount = neighborCellsCountMap.get(cellPosition).get
	
	return neighborCellsCount
  }
  
  def computeGStatisticZScore(neighborCellsCount: Int, neighborCellsPickupSum: Int, meanOfPickups: Double, s: Double, numCells: Int): Double ={
	val num = (neighborCellsPickupSum.toDouble - (meanOfPickups * neighborCellsCount.toDouble))
	val den = s * math.sqrt((((numCells.toDouble * neighborCellsCount.toDouble) - (neighborCellsCount.toDouble * neighborCellsCount.toDouble)) / (numCells.toDouble - 1.0)))
	return (num/den)
  }
}
