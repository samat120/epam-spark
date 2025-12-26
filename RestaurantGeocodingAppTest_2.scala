import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Dataset

// Допустим, у тебя есть case class Restaurant
case class Restaurant(id: Long, franchise_id: Int, franchise_name: String,
                      restaurant_franchise_id: Int, country: String,
                      city: String, lat: Option[Double], lng: Option[Double])

class RestaurantGeocodingAppTest_2 extends AnyFunSuite {

  // Создаём SparkSession для тестов
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("RestaurantGeocodingAppTest")
    .getOrCreate()

  import spark.implicits._  // ✅ импорт внутри метода/класса для стабильного идентификатора

  // Пример метода геокодирования, который хотим протестировать
  def fakeGeocode(ds: Dataset[Restaurant]): Dataset[Restaurant] = {
    ds.map { r =>
      if (r.lat.isEmpty || r.lng.isEmpty)
        r.copy(lat = Some(1.234), lng = Some(5.678))
      else
        r
    }
  }

  test("Geocoding fills missing coordinates") {
    val input = Seq(
      Restaurant(1, 101, "Savoria", 5001, "US", "Dillon", None, None),
      Restaurant(2, 102, "PizzaX", 5002, "US", "Dallas", Some(32.7767), Some(-96.7970))
    ).toDS()

    val result = fakeGeocode(input).collect()

    // Проверяем, что первый ресторан получил координаты
    assert(result(0).lat.isDefined)
    assert(result(0).lng.isDefined)

    // Проверяем, что второй ресторан остался без изменений
    assert(result(1).lat.contains(32.7767))
    assert(result(1).lng.contains(-96.7970))
  }

}
