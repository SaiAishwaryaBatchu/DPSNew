package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle:String, queryPoint:String):Boolean={
    var rectangle = queryRectangle.split(",");
    var point = queryPoint.split(",");
    var rect = new Array[Double](4);
    var pt = new Array[Double](4);
    for(i <- 0 to rectangle.length-1){
      rect(i)=rectangle(i).trim.toDouble;
    }
    for(j <- 0 to point.length-1){
      pt(j)=point(j).trim.toDouble;
    }
    val high_bound_x = math.max(rect(0),rect(2));
    val low_bound_x = math.min(rect(0),rect(2));
    val high_bound_y = math.max(rect(1),rect(3));
    val low_bound_y = math.min(rect(1),rect(3));
    val point_x = pt(0);
    val point_y = pt(1);
    if(point_x > high_bound_x || point_x < low_bound_x || point_y > high_bound_y || point_y < low_bound_y){
      return false;
    }
    else{
      return true;
    }
}
  def ST_Within(pointString1:String, pointString2:String, distance:Double):Boolean={
    var point1 = pointString1.split(",");
    var point2 = pointString2.split(",");
    val pt_1_x = point1(0).trim.toDouble;
    val pt_1_y = point1(1).trim.toDouble;
    val pt_2_x = point2(0).trim.toDouble;
    val pt_2_y = point2(1).trim.toDouble;
    val distance_between_points = math.sqrt(math.pow((pt_2_x-pt_1_x),2)+ math.pow((pt_2_y-pt_1_y),2))
    if(distance_between_points>distance){
      return false;
    }
    else{
      return true;
    }
  }
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
