package ec.edu.utpl.presencial.computacion.pfr.pintegra

import breeze.plot.{Figure, plot}
import doobie.*
import doobie.implicits.*
import cats.*
import cats.effect.*
import cats.implicits.*
import cats.effect.unsafe.implicits.global
import com.github.tototoshi.csv.*
import doobie.implicits.toSqlInterpolator
import java.io.File
import org.nspl.*
import org.nspl.data.HistogramData
import org.saddle.{Index, Series, Vec}
import org.nspl.awtrenderer.*
import org.nspl.data.*

object GraficasPracticum {

  @main
  def main() = {
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

    // Metodos para data desde CSV
    /*
    golesPartido(partidosGoles)
    capacidadEstadios(partidosGoles)
    golesJugadores(partidosGoles)
     */

    // Métodos para data desde la BD
    /*
    graficaGolesEquipo(golesEquipo().transact(xa).unsafeRunSync())
    graficaEdadGoles(edadGoles().transact(xa).unsafeRunSync())
    graficaEdadPartidos(edadPartidos().transact(xa).unsafeRunSync())
     */

  }

  // Metodo para el reemplazo del valor NA por 0 para columnas de tipo entero o real
  def defaultValue(text: String): Double = {
    if (text.equals("NA")) {
      0
    } else {
      text.toDouble
    }
  }

  // Gráficas a partir de los archivos CSV
  // Histograma de goles marcados ´por el equipo local
  def golesPartido(data: List[Map[String, String]]) = {
    val golesPartidoList =
      data
        .map(_("matches_home_team_score"))
        .filterNot(_.equals("NA"))
        .map(_.toDouble)

    val histograma = xyplot(HistogramData(golesPartidoList, 20) -> bar())(
      par
        .xlab("Goles")
        .ylab("freq.")
        .main("Goles marcados por equipos locales")
    )

    pngToFile(new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\PCIntegrador\\src\\main\\data\\golesPartido.png\\"),
      histograma.build, width = 1000)
  }


  // Histograma de capacidad de los estadios
  def capacidadEstadios(data: List[Map[String, String]]) = {
    val capaEstadiosData =
      data
        .distinctBy(_("matches_stadium_id"))
        .map(_("stadiums_stadium_capacity"))
        .filterNot(_.equals("NA"))
        .map(_.toDouble)

    val histograma = xyplot(HistogramData(capaEstadiosData, 20) -> bar())(
      par
        .xlab("Capacidad")
        .ylab("freq.")
        .main("Capacidad de los estadios")
    )

    pngToFile(new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\PCIntegrador\\src\\main\\data\\capaEstadios.png\\"),
      histograma.build, width = 1000)

  }

  // Gráfico de barras de goles por jugador
  def golesJugadores(data: List[Map[String, String]]) = {
    val golesJugador = data
      .filter(_("tournaments_tournament_name").contains("Men"))
      .map(row => (
        row("goals_player_id"),
        defaultValue(row("goals_minute_regulation")).toInt
      ))
      .groupBy(_._1)
      .map {
        case (jugadorId, goles) =>
          (jugadorId, goles.map(_._2).sum.toDouble)
      }
      .toList
      .sortBy(_._2)(Ordering[Double].reverse)
      .take(20) // Se toman solo los 20 primeros resultados por temas de visibilidad de la gráfica

    graficaGolesJugadores(golesJugador)
  }

  def graficaGolesJugadores (data: List[(String, Double)]): Unit = {

    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("ID Jugador")
        .ylab("Goles")
        .main("Goles por Jugador")
    )

    pngToFile(
      new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\PCIntegrador\\src\\main\\data\\golesJugadores.png\\"),
      barPlot.build,
      5000
    )
  }

  // Gráficas a partir de la BD
  // Gráfico de barras de goles por equipo
    def golesEquipo(): ConnectionIO[List[(String, Double)]] = {
    val golesPorEquipo = sql"""
      SELECT g.goals_team_id, CAST(COUNT(*) AS REAL) AS goles
      FROM goals g
      GROUP BY g.goals_team_id
      ORDER BY goles DESC
      LIMIT 20
   """
    .query[(String, Double)]
    .to[List]

    golesPorEquipo
  }

  def graficaGolesEquipo(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("ID Equipo")
        .ylab("Goles")
        .main("Goles por Equipo")
    )

    pngToFile(
      new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\PCIntegrador\\src\\main\\data\\golesEquipos.png\\"),
      barPlot.build,
      5000
    )
  }

  // Gráfica de dispersión sobre la relación entre la edad de los jugadores (desde 1940) y el número de goles que han marcado en los partidos.
  def edadGoles(): ConnectionIO[List[(Int, Int)]] = {
    sql"""
    SELECT (YEAR(CURRENT_DATE) - YEAR(players_birth_date)) AS edad, COUNT(goals_goal_id) AS total_goles
    FROM players
    INNER JOIN squads ON players.squads_player_id = squads.squads_player_id
    INNER JOIN goals ON squads.squads_player_id = goals.goals_player_id
    WHERE YEAR(players_birth_date) >= 1940
    GROUP BY edad;
  """
      .query[(Int, Int)]
      .to[List]
  }

  def graficaEdadGoles(data: List[(Int, Int)]): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(data.map(_._1), data.map(_._2), '+')
    p.xlabel = "Edad"
    p.ylabel = "Goles"
    f.saveas("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\PCIntegrador\\src\\main\\data\\edadGoles.png\\")
  }

  // Gráfica de dispersión sobre la relación entre la edad de los jugadores (desde 1940) y la cantidad de partidos en los que han participado
  def edadPartidos(): ConnectionIO[List[(Int, Int)]] = {
    sql"""
    SELECT (YEAR(CURRENT_DATE) - YEAR(players_birth_date)) AS edad, COUNT(matches.matches_match_id) AS total_partidos
    FROM players
    INNER JOIN squads ON players.squads_player_id = squads.squads_player_id
    INNER JOIN matches ON squads.squads_team_id = matches.matches_home_team_id OR squads.squads_team_id = matches.matches_away_team_id
    WHERE YEAR(players_birth_date) >= 1940
    GROUP BY edad;
  """
      .query[(Int, Int)]
      .to[List]
  }

  def graficaEdadPartidos(data: List[(Int, Int)]): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(data.map(_._1), data.map(_._2), '+')
    p.xlabel = "Edad"
    p.ylabel = "Partidos Participados"
    f.saveas("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\PCIntegrador\\src\\main\\data\\edadPartidos.png\\")
  }
}
