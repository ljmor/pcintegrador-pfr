package ec.edu.utpl.computacion.pfr.pi

import com.github.tototoshi.csv.*
import java.io.File
import doobie._
import doobie.implicits._

import cats._
import cats.effect._
import cats.implicits._

import cats.effect.unsafe.implicits.global

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Exporter {
  @main
  def exportFunc() =
    val path2DataFile = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\TallerSem16\\imdb_movie_data_2023.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] =
      reader.allWithHeaders()

    reader.close()

    // println(contentFile)
    // genData2GenreTable(contentFile)
    // genData2ActorTable(contentFile)

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/movie_4_practice",
      user = "root",
      password = "1234",
      logHandler = None
    )

    // generateData2Movie(contentFile)

    generateData2MovieDoobie(contentFile)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())



  def convertDuration2Int(txtDuration: String): Int =
    if(txtDuration.contains("h") && txtDuration.contains("m")) {
      txtDuration
        .trim
        .split("\\s")(0)
        .replaceAll("h", "")
        .toInt * 60
      +
        txtDuration
          .trim
          .split("\\s")(1)
          .replaceAll("m", "")
          .toInt
    } else {
      0
    }

  def defaultValue(text: String): Double =
    if(text.equals("NA")) {
      0
    }  else {
      text.toDouble
    }

  def generateData2Movie(data: List[Map[String, String]]) =
    val sqlInsert = s"INSERT INTO MOVIE(NAME, RATING, VOTES, META_SCORE, PG_RATING, YEAR, DURATION) VALUES('%s', %f, %d, %d, '%s', %d, %d);"
    val movieTuple = data
      .map(
        row => (row("Movie Name").trim,
        defaultValue(row("Rating")),
        defaultValue(row("Votes")).toInt,
        defaultValue(row("Meta Score")).toInt,
        row("PG Rating"),
        row("Year").toInt,
        convertDuration2Int(row("Duration")))
      )
      .map(t7 => sqlInsert.formatLocal(java.util.Locale.US, t7._1, t7._2, t7._3, t7._4, t7._5, t7._6, t7._7))



    println(movieTuple.take(2))

  def generateData2MovieDoobie(data: List[Map[String, String]]) =
    val movieTuple = data
      .map(
        row => (row("Movie Name").trim,
          defaultValue(row("Rating")),
          defaultValue(row("Votes")).toInt,
          defaultValue(row("Meta Score")).toInt,
          row("PG Rating"),
          row("Year").toInt,
          convertDuration2Int(row("Duration")))
      )
      .map(t7 => sql"INSERT INTO MOVIE(NAME, RATING, VOTES, META_SCORE, PG_RATING, YEAR, DURATION) VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5}, ${t7._6} ,${t7._7})".update)


    movieTuple

  def genData2GenreTable(data: List[Map[String, String]]) =
    val insertFormat = s"INSERT INTO GENRE(NAME) VALUES('%s');"
    val genre = data
      .map(_("Genre"))
      .flatMap(_.split(","))
      .filterNot(_ == "NA")
      .map(_.trim)
      .distinct
      .sorted
      .map(name => insertFormat.format(name))


    //println(genre)
    genre.foreach(println)

  def genData2ActorTable(data: List[Map[String, String]]) =
    val insertFormat = s"INSERT INTO ACTOR(NAME) VALUES('%s');"
    val actor = data
      .map(_("Cast"))
      .flatMap(_.split(","))
      .filterNot(_ == "NA")
      .map(_.trim)
      .map(_.replaceAll("'", "\\\\'"))
      .distinct
      .sorted
      .map(name => insertFormat.format(name))


    //println(actor)
    actor.foreach(println)

}
