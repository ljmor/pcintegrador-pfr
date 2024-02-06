package ec.edu.utpl.presencial.computacion.pfr.pintegra
import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.implicits._
import cats.effect.unsafe.implicits.global

case class Actor (id: Int, name: String, lastname: String)
case class Film (id: Int, title: String, releaseYear: Int, actorList: String)
case class FilmCategoty (title: String, name: String)
case class FilmData (id: Int, title: String, releaseYear: Int, actorList: List[Actor], language: String)

object DBDemo {
  @main
  def demo(): Unit =
    println("Demo")

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/sakila",
      user = "root",
      password = "1234",
      logHandler = None
    )

    val result: Option[Actor] = find(1)
      .transact(xa)
      .unsafeRunSync()

    println(result.get)
    println("---------------------------------------------------------------------------------------------------")

    val actorList: List[Actor] = listAllActors()
      .transact(xa)
      .unsafeRunSync()

    actorList.foreach(println)
    println("---------------------------------------------------------------------------------------------------")

    val filmList: List[Film] = listOfFilms()
      .transact(xa)
      .unsafeRunSync()

    filmList.foreach(println)
    println("---------------------------------------------------------------------------------------------------")

    val categotyFilmsList: List[FilmCategoty] = listOfFilmsCategory()
      .transact(xa)
      .unsafeRunSync()

    categotyFilmsList.foreach(println)
    println("---------------------------------------------------------------------------------------------------")
    println("Movie data")

    val movieData = findFilmDataById(2)
      .transact(xa)
      .unsafeRunSync()

    println(movieData.get)


  def find(id: Int): ConnectionIO[Option[Actor]] =
    sql"SELECT a.actor_id, a.first_name, a.last_name FROM actor a WHERE a.actor_id = $id"
      .query[Actor]
      .option

  def listAllActors(): ConnectionIO[List[Actor]] =
    sql"SELECT a.actor_id, a.first_name, a.last_name FROM actor a"
      .query[Actor]
      .to[List]

  def listOfFilms(): ConnectionIO[List[Film]] =
    sql"""
         SELECT f.film_id, f.title, f.release_year, group_concat(CONCAT(a.first_name, ' ', a.last_name)) as actor_list
         FROM film f, film_actor fa, actor a
         WHERE f.film_id = fa.film_id
         AND fa.actor_id = a.actor_id
         GROUP BY f.film_id, f.title;
       """
      .query[Film]
      .to[List]

  def listOfFilmsCategory(): ConnectionIO[List[FilmCategoty]] =
    sql"""
         SELECT f.title, c.name FROM film f INNER JOIN (SELECT fc.film_id, fc.category_id, c2.name
                                                        FROM film_category fc
                                                        INNER JOIN category c2 ON fc.category_id = c2.category_id) c
         ON f.film_id = c.film_id
       """
      .query[FilmCategoty]
      .to[List]


  def findFilmDataById(filmId: Int) =
    def findFilmById(): ConnectionIO[Option[(Int, String, Int, Int)]] =
      sql"""
           |SELECT f.film_id, f.title, f.release_year, f.language_id
           |FROM film f
           |WHERE f.film_id = $filmId
           |""".stripMargin
        .query[(Int, String, Int, Int)]
        .option


    def findLanguageById(languageId: Int): ConnectionIO[String] =
      sql"""
           |SELECT l.name
           |FROM language l
           |WHERE l.language_id = $languageId
           |""".stripMargin
        .query[String]
        .unique

    def findActorsByFilmId(filmId: Int) =
      sql"""
           |SELECT a.actor_id, a.first_name, a.last_name
           |FROM actor a
           |JOIN film_actor fa on a.actor_id = fa.actor_id
           |WHERE fa.film_id = $filmId
           |""".stripMargin
        .query[Actor]
        .to[List]

    val query = for {
      optFilm <- findFilmById()
      language <- optFilm match {
        case Some(film) => findLanguageById(film._4)
        case None => "".pure[ConnectionIO]
      }
      actors <- optFilm match {
        case Some(film) => findActorsByFilmId(film._1)
        case None => List.empty[Actor].pure[ConnectionIO]
      }
      } yield {
        optFilm.map { film =>
          FilmData(film._1, film._2, film._3, actors, language)
        }
      }
      query
}