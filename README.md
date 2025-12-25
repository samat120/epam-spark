1. Load Restaurant Data

~~~scala
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

println(
  s"NULL coordinates BEFORE: " +
    restaurantsDs.filter(r => r.lat.isEmpty || r.lng.isEmpty).count()
)
~~~
2. Fix Missing Coordinates via OpenCage API

~~~scala
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
      } yield GeoHash
        .withCharacterPrecision(lat, lng, 4)
        .toBase32

    withCoords.copy(geohash = geohash)
  }.toList.iterator // materialization

  client.close()
  result
}

println(
  s"NULL coordinates AFTER: " +
    geocodedDs.filter(r => r.lat.isEmpty || r.lng.isEmpty).count()
)
~~~

3. Generate 4-Character Geohash
~~~scala
val geohash =
  for {
    lat <- withCoords.lat
    lng <- withCoords.lng
  } yield GeoHash.withCharacterPrecision(lat, lng, 4).toBase32

withCoords.copy(geohash = geohash)
~~~

4. Join with Weather Data
- Left join on 4-character geohash
- Weather dataset must be pre-aggregated
- Prevents data multiplication
- Safe to rerun (idempotent)
~~~scala
val joinedDf =
  geocodedDs.toDF()
    .join(
      weatherAgg,
      Seq("geohash"),
      "left"
    )
~~~

5. Store Enriched Data (Partitioned Parquet)
~~~scala
joinedDf
  .repartition($"year", $"month")
  .write
  .mode("overwrite")
  .partitionBy("year", "month")
  .parquet("src/main/resources/restaurant_weather")
~~~
