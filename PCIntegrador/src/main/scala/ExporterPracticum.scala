package ec.edu.utpl.presencial.computacion.pfr.pintegra

// Importaciones de las librerias correspondientes
import com.github.tototoshi.csv.*
import java.io.File
import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.effect.unsafe.implicits.global

// Delimitador de ";" para los campos de los archivos CSV
implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Exporter {
  @main
  def exportFunc() = {
    // Archivo dsAlienacionesXTorneo
    val rutaAlTor = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\Practicum1.1\\Bim02\\DataSets\\dsAlineacionesXTorneo-2.csv"
    val readerAlTor = CSVReader.open(new File(rutaAlTor))
    val alineacionesTorneo: List[Map[String, String]] =
      readerAlTor.allWithHeaders()
    readerAlTor.close()

    // Archivo dsPartidosYGoles
    val rutaParGol = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\Practicum1.1\\Bim02\\DataSets\\dsPartidosYGoles.csv"
    val readerParGol = CSVReader.open(new File(rutaParGol))
    val partidosGoles: List[Map[String, String]] =
      readerParGol.allWithHeaders()
    readerParGol.close()

    // Conexion a la BD
    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/practicum",
      user = "root",
      password = "1234",
      logHandler = None
    )

    // Metodo 01 de inserción de datos para las tablas AwayTeams, Goals, HomeTeams, Matches
    /*
    generateData2AwayTeams(partidosGoles)
    generateData2Goals(partidosGoles)
    generateData2HomeTeams(partidosGoles)
    generateData2Matches(partidosGoles)*/


    // Metodo 02 de inserción de datos para las tablas Players, Squads, Stadiums y Tournaments
    /*
    dataForPlayers(alineacionesTorneo).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    dataForSquads(alineacionesTorneo).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    dataForStadiums(partidosGoles).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    dataForTournaments(partidosGoles).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    */


    // Sentencias SELECT para responder a preguntas
    println("Pregunta01--------------------------------------------------------------------------------------------")
    pregunta01().transact(xa).unsafeRunSync().foreach(println)
    println("Pregunta02--------------------------------------------------------------------------------------------")
    pregunta02().transact(xa).unsafeRunSync().foreach(println)
    println("Pregunta03--------------------------------------------------------------------------------------------")
    pregunta03().transact(xa).unsafeRunSync().foreach(println)
    println("Pregunta04--------------------------------------------------------------------------------------------")
    pregunta04().transact(xa).unsafeRunSync().foreach(println)

  }

  // Metodo para el reemplazo del valor NA por 0 para columnas de tipo entero o real
  def defaultValue(text: String): Double =
    if(text.equals("NA")) {
      0
    }  else {
      text.toDouble
    }

  // Metodo para la insercíón del escape de las comillas simples
  def quoteEscape(text: String) =
    text.replaceAll("'", "\\\\'")

  // Funciones para el 1er método de inserción de datos
  def generateData2AwayTeams(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO awayteams(matches_away_team_id, away_team_name, away_mens_team, away_womens_team, away_region_name) VALUES('%s', '%s', %d, %d, '%s');"
    val awayTeamsTuple =
      data.distinctBy(_("matches_away_team_id"))
        .map(
          row => (row("matches_away_team_id"),
            row("away_team_name"),
            defaultValue(row("away_mens_team")).toInt,
            defaultValue(row("away_womens_team")).toInt,
            row("away_region_name"))
        )
        .sorted
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5))
    awayTeamsTuple.foreach(println)

  }


  def generateData2Goals(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO goals(goals_goal_id, goals_team_id, goals_player_id, goals_player_team_id, goals_minute_label, goals_minute_regulation, goals_minute_stoppage, goals_match_period, goals_own_goal, goals_penalty) VALUES('%s', '%s', '%s', '%s', '%s', %d, %d, '%s', %d, %d);"
    val goalsTuple = data
      .map(
        row => (row("goals_goal_id"),
          row("goals_team_id"),
          row("goals_player_id"),
          row("goals_player_team_id"),
          quoteEscape(row("goals_minute_label")),
          defaultValue(row("goals_minute_regulation")).toInt,
          defaultValue(row("goals_minute_stoppage")).toInt,
          row("goals_match_period"),
          defaultValue(row("goals_own_goal")).toInt,
          defaultValue(row("goals_penalty")).toInt)
      )
      .filterNot(_._1.equals("NA"))
      .sortBy(_._1)
      .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5, name._6, name._7, name._8, name._9, name._10))

    // Se imprime en pantalla en dos partes las instrucciones SQL puesto que la consola no muestra todos los resultados
    // al quererlos imprimir todos a la vez
    goalsTuple.take(goalsTuple.length/2).foreach(println)
    goalsTuple.drop(goalsTuple.length/2).foreach(println)
  }

  def generateData2HomeTeams(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO hometeams(matches_home_team_id, home_team_name, home_mens_team, home_womens_team, home_region_name) VALUES('%s', '%s', %d, %d, '%s');"
    val homeTeamsTuple =
      data.distinctBy(_("matches_home_team_id"))
      .map(
        row => (row("matches_home_team_id"),
          row("home_team_name"),
          defaultValue(row("home_mens_team")).toInt,
          defaultValue(row("home_womens_team")).toInt,
          row("home_region_name"))
      )
      .sorted
      .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5))

    homeTeamsTuple.foreach(println)
  }

  def generateData2Matches(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO matches(matches_match_id, matches_away_team_id, matches_home_team_id, matches_stadium_id, matches_match_date, matches_stage_name, matches_match_time, matches_home_team_score, matches_away_team_score, matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties, matches_result, tournaments_tournament_name, tournaments_year) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, '%s', '%s', '%s');"
    val matchesTuple =
      data.distinctBy(_("matches_match_id"))
      .map(
        row => (row("matches_match_id"),
          row("matches_away_team_id"),
          row("matches_home_team_id"),
          row("matches_stadium_id"),
          row("matches_match_date"),
          row("matches_stage_name"),
          row("matches_match_time"),
          defaultValue(row("matches_home_team_score")).toInt,
          defaultValue(row("matches_away_team_score")).toInt,
          defaultValue(row("matches_extra_time")).toInt,
          defaultValue(row("matches_penalty_shootout")).toInt,
          defaultValue(row("matches_home_team_score_penalties")).toInt,
          defaultValue(row("matches_away_team_score_penalties")).toInt,
          row("matches_result"),
          quoteEscape(row("tournaments_tournament_name")),
          row("tournaments_year")))
        .sortBy(_._1)
        .map(name => sqlInsert.format(name._1, name._2, name._3, name._4, name._5, name._6, name._7, name._8, name._9,
          name._10, name._11, name._12, name._13, name._14, name._15, name._16))

    matchesTuple.foreach(println)

  }

  // Funciones para el 2do método de inserción de datos
  def dataForPlayers(data: List[Map[String, String]]) = {
    val playerTuple =
      data.distinctBy(_("squads_player_id"))
        .map(row =>
          (row("squads_player_id"),
            defaultValue(row("players_female")).toInt,
            row("players_birth_date"),
            row("players_given_name"),
            row("players_family_name")))
        .map(upt =>
          sql"""
          INSERT INTO players (squads_player_id, players_female, players_birth_date, players_given_name, players_family_name)
          VALUES (${upt._1}, ${upt._2}, ${upt._3}, ${upt._4}, ${upt._5});
           """.update)

    playerTuple
  }


  def dataForSquads(data: List[Map[String, String]]) = {
    val squadTuple =
      data
        .map(row =>
          (row("squads_player_id"),
            row("squads_tournament_id"),
            row("squads_team_id"),
            defaultValue(row("players_forward")).toInt,
            defaultValue(row("players_midfielder")).toInt,
            defaultValue(row("players_defender")).toInt,
            defaultValue(row("players_goal_keeper")).toInt,
            row("squads_position_name"),
            defaultValue(row("squads_shirt_number")).toInt))
        .map(upt =>
          sql"""
            INSERT INTO squads (squads_player_id, squads_tournament_id, squads_team_id, players_forward, players_defender, players_midfielder, players_goal_keeper, squads_position_name, squads_shirt_number)
            VALUES (${upt._1}, ${upt._2}, ${upt._3}, ${upt._4}, ${upt._5}, ${upt._6}, ${upt._7}, ${upt._8}, ${upt._9});
             """.update)

    squadTuple
  }


  def dataForStadiums(data: List[Map[String, String]]) = {
    val stadiumTuple =
      data.distinctBy(_("matches_stadium_id"))
        .map(row =>
          (row("matches_stadium_id"),
            row("stadiums_stadium_name"),
            row("stadiums_city_name"),
            row("stadiums_country_name"),
            defaultValue(row("stadiums_stadium_capacity")).toInt))
        .map(upt =>
          sql"""
              INSERT INTO stadiums (matches_stadium_id, stadiums_stadium_name, stadiums_city_name, stadiums_country_name, stadiums_stadium_capacity)
              VALUES (${upt._1}, ${upt._2}, ${upt._3}, ${upt._4}, ${upt._5});
               """.update)

    stadiumTuple
  }


  def dataForTournaments(data: List[Map[String, String]]) = {
    val tournamentTuple =
      data.distinctBy(_("tournaments_tournament_name"))
        .map(row =>
          (row("tournaments_tournament_name"),
            row("tournaments_year"),
            row("tournaments_host_country"),
            row("tournaments_winner"),
            defaultValue(row("tournaments_count_teams")).toInt))
        .map(upt =>
          sql"""
                INSERT INTO tournaments (tournaments_tournament_name, tournaments_year, tournaments_host_country, tournaments_winner, tournaments_count_teams)
                VALUES (${upt._1}, ${upt._2}, ${upt._3}, ${upt._4}, ${upt._5});
                 """.update)

    tournamentTuple
  }

  // Preguntas a responder con SELECT
  // 1. ¿Cuántos jugadores ha tenido cada equipo en total?
  def pregunta01(): ConnectionIO[List[(String, Int)]] = {
    sql"""
         SELECT squads_team_id, COUNT(DISTINCT squads_player_id) AS num_players
         FROM Squads
         GROUP BY squads_team_id;
       """
      .query[(String, Int)]
      .to[List]
  }

  // 2. ¿Cuántos partidos se jugaron en cada torneo?
  def pregunta02(): ConnectionIO[List[(String, Int, Int)]] = {
    sql"""
           SELECT tournaments_tournament_name, tournaments_year, COUNT(*) AS num_matches
           FROM Matches
           GROUP BY tournaments_tournament_name, tournaments_year;
         """
      .query[(String, Int, Int)]
      .to[List]
  }

  // 3. ¿Cuál es el estadio con la capacidad más grande?
  def pregunta03(): ConnectionIO[List[(String, Int)]] = {
    sql"""
             SELECT stadiums_stadium_name, stadiums_stadium_capacity
             FROM Stadiums
             ORDER BY stadiums_stadium_capacity DESC
             LIMIT 1;
           """
      .query[(String, Int)]
      .to[List]
  }

  // 4. ¿Cuál es la cantidad promedio de goles marcados por partido en cada estadio?
  def pregunta04(): ConnectionIO[List[(String, Double)]] = {
    sql"""
               SELECT matches_stadium_id, AVG(matches_home_team_score + matches_away_team_score) AS avg_goals
               FROM Matches
               GROUP BY matches_stadium_id;
             """
      .query[(String, Double)]
      .to[List]
  }
}