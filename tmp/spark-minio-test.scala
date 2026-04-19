import org.apache.hadoop.fs.{FileSystem, Path}
val conf = spark.sparkContext.hadoopConfiguration
val fs = FileSystem.get(new java.net.URI("s3a://raw"), conf)
println("SPARK_MINIO_TEST_EXISTS=" + fs.exists(new Path("s3a://raw/kafka-archive/")))
println("SPARK_MINIO_TEST_LIST_START")
fs.listStatus(new Path("s3a://raw/kafka-archive/")).take(10).foreach(s => println(s.getPath.toString))
println("SPARK_MINIO_TEST_LIST_END")
System.exit(0)
