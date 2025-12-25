o	Check restaurant data for incorrect (null) values (latitude and longitude). For incorrect values, map latitude and longitude from the OpenCage Geocoding API in a job via the REST API.

// ================= RESTAURANTS =================

val restaurantsDs = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("src/main/resources/restaurant_csv")
  .select(
    $"id",
    $"franchise_id",
    $"franchise_name",
    $"restaurant_franchise_id",
    $"country",
    $"city",
    $"lat",
    $"lng"
  )
  .as[(Long, Int, String, Int, String, String, Option[Double], Option[Double])]
  .map {
    case (id, fid, fname, rfid, country, city, lat, lng) =>
      Restaurant(id, fid, fname, rfid, country, city, lat, lng, None)
  }

println(s"NULL coordinates BEFORE: " +
  restaurantsDs.filter(r => r.lat.isEmpty || r.lng.isEmpty).count()
)

// ================= GEOCODING =================

val geocodedDs = restaurantsDs.mapPartitions { partition =>
  val client = new OpenCageClient(apiKey)

  val result = partition.map { r =>
    val withCoords =
      if (r.lat.isEmpty || r.lng.isEmpty) {
        try {
          val query = s"${r.franchise_name}, ${r.city}, ${r.country}"
          val response = Await.result(client.forwardGeocode(query), 5.seconds)
          val geom = response.results.headOption.flatMap(_.geometry)

          r.copy(
            lat = geom.map(_.lat.toDouble),
            lng = geom.map(_.lng.toDouble)
          )
        } catch {
          case _: Exception => r
        }
      } else r

    val geohash =
      for {
        lat <- withCoords.lat
        lng <- withCoords.lng
      } yield GeoHash.withCharacterPrecision(lat, lng, 4).toBase32

    withCoords.copy(geohash = geohash)
  }.toList.iterator   // материализация

  client.close()
  result
}

println(s"NULL coordinates AFTER: " +
  geocodedDs.filter(r => r.lat.isEmpty || r.lng.isEmpty).count()
)

o	Generate a geohash by latitude and longitude using a geohash library like geohash-java. Your geohash should be four characters long and placed in an extra column.

val geohash =
  for {
    lat <- withCoords.lat
    lng <- withCoords.lng
  } yield GeoHash.withCharacterPrecision(lat, lng, 4).toBase32

 withCoords.copy(geohash = geohash)

o	Left-join weather and restaurant data using the four-character geohash. Make sure to avoid data multiplication and keep your job idempotent.

val joinedDf =
  geocodedDs.toDF()
    .join(
      weatherAgg,
      Seq("geohash"),
      "left"
    )
        
o	Store the enriched data (i.e., the joined data with all the fields from both datasets) in the local file system, preserving data partitioning in the parquet format.

joinedDf
      .repartition($"year", $"month")   // оптимизация перед записью
      .write
      .mode("overwrite")                // идемпотентность
      .partitionBy("year", "month")     // физическое партиционирование
      .parquet("src/main/resources/restaurant_weather")
