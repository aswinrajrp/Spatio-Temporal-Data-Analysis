package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var rt_coord : Array[Double] = queryRectangle.split(",").map(x => x.toDouble)
    var point : Array[Double] = pointString.split(",").map(x => x.toDouble)

    var x1 = math.min(rt_coord(0), rt_coord(2))
    var y1 = math.min(rt_coord(1), rt_coord(3))
    var x2 = math.max(rt_coord(0), rt_coord(2))
    var y2 = math.max(rt_coord(1), rt_coord(3))

    //checking whether the point is inside the rectangle or not
    if(point(0) >= x1 && point(0) <= x2 && point(1) >= y1 && point(1) <= y2) {
      return true
    } else {
      return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
