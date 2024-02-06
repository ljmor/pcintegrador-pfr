package ec.edu.utpl.presencial.computacion.pfr.pintegra

import com.github.tototoshi.csv.*
import java.io.File
import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.effect.unsafe.implicits.global

object StatsPracticum {

  @main
  def stats() = {
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

    println(promedioGoles(partidosGoles))
    println("------------------------------------------------------")
    println(maximoGoles(partidosGoles))
    println("------------------------------------------------------")
    println(frecuenciaGanadores(partidosGoles))
    println("------------------------------------------------------")
    println(modaEstadios(partidosGoles))
    println("------------------------------------------------------")
    println(cantidadJugadoresPorPosicion(alineacionesTorneo))
  }

  def promedioGoles(partidosGoles: List[Map[String, String]]): Double = {
    val totalGoles = partidosGoles.foldLeft(0) { (sum, partido) =>
      val golesHome = partido("matches_home_team_score").toInt
      val golesAway = partido("matches_away_team_score").toInt
      sum + golesHome + golesAway
    }
    totalGoles.toDouble / (partidosGoles.size * 2)
  }

  def maximoGoles(partidosGoles: List[Map[String, String]]): Int = {
    partidosGoles.foldLeft(0) { (max, partido) =>
      val golesHome = partido("matches_home_team_score").toInt
      val golesAway = partido("matches_away_team_score").toInt
      List(max, golesHome + golesAway).max
    }
  }

  def frecuenciaGanadores(partidosGoles: List[Map[String, String]]): Map[String, Int] = {
    partidosGoles.foldLeft(Map[String, Int]()) { (map, partido) =>
      val ganador = if (partido("matches_home_team_score").toInt > partido("matches_away_team_score").toInt)
        partido("home_team_name")
      else
        partido("away_team_name")
      map.updated(ganador, map.getOrElse(ganador, 0) +1
      )
    }
  }

  def modaEstadios(partidosGoles: List[Map[String, String]]): String = {
    val frecuencias = partidosGoles.foldLeft(Map[String, Int]()) { (map, partido) =>
      val estadio = partido("stadiums_stadium_name")
      map.updated(estadio, map.getOrElse(estadio, 0) + 1)
    }
    frecuencias.maxBy(_._2)._1
  }

  def cantidadJugadoresPorPosicion(alineacionesTorneo: List[Map[String, String]]): Map[String, Int] = {
    alineacionesTorneo.foldLeft(Map[String, Int]()) { (map, alineacion) =>
      val posicion = alineacion("squads_position_name")
      map.updated(posicion, map.getOrElse(posicion, 0) + 1
      )
    }
  }
}
