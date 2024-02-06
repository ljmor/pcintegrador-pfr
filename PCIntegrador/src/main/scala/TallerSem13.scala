package ec.edu.utpl.presencial.computacion.pfr.pintegra

import com.github.tototoshi.csv.*
import java.io.File

object TallerSem13 {

  // Archivo partidos y goles
  @main
  def ArcPartidosyGoles() =
    val path2DataFile: String = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\dsPartidosYGoles.csv\\"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    // 1. La capacidad mínima, máxima y el promedio de la capacidad de los estadios en los que se ha jugado algún partido
    // del mundial. La columna que deben utilizar es: stadiums_stadium_capacity.

    val capacidades = contentFile.map(e => (e("stadiums_stadium_name"), e("stadiums_stadium_capacity").toInt)).distinct.map(_._2)
    val CapacidadMin: Int = capacidades.min
    val CapacidadMax: Int = capacidades.max
    val PromedioCapacidad: Double = capacidades.sum.toDouble / capacidades.length

    println(s"Capacidad mínima: ${CapacidadMin}")
    println(s"Capacidad máxima ${CapacidadMax}")
    println(s"Capacidad promedio ${PromedioCapacidad}")

    // 2. ¿Cuál es el minuto más común en el que se han marcado un gol? Su respuesta debe presentar el resultado
    // para los torneos masculinos y otra los torneos femeninos.

    val femTorneos: List[Map[String, String]] = contentFile.filter(_("tournaments_tournament_name").contains("Women's"))
    val minGolFem: Int = femTorneos.map(_("goals_minute_regulation")).filter(e => e != "NA").map(_.toInt)
      .groupBy(identity).maxBy(_._2.length)._1
    println(s"Minuto más común en torneos femeninos: ${minGolFem}")

    val mascTorneos: List[Map[String, String]] = contentFile.filter(_("tournaments_tournament_name").contains("Men's"))
    val minGolMasc: Int = mascTorneos.map(_("goals_minute_regulation")).filter(e => e != "NA").map(_.toInt)
      .groupBy(identity).maxBy(_._2.length)._1
    println(s"Minuto más común en torneos masculinos: ${minGolMasc}")

    // 3. ¿Cuál es el periodo más común en los que se han marcado goles en todos los mundiales? (columna: goals_match_period)

    val PeriodoMasComun: List[String] = contentFile.map(_("goals_match_period")).filter(e => e != "NA")
    val PeriodoMasComunMundiales: String = PeriodoMasComun.groupBy(identity).maxBy(_._2.length)._1

    println(s"Periodo más común: ${PeriodoMasComunMundiales}")

  // Archivo alienaciones por torneo
  @main
  def ArcAlineacionesTorneo() =
    val path2DataFile: String = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\dsAlineacionesXTorneo-2.csv\\"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    // 4. ¿Cuál es el número de camiseta (squads_shirt_number)
    // más común que se utiliza en cada una de las posiciones (squads_position_name)?

    val nrosPosiciones: Map[String, List[(String, String)]] = contentFile
      .map(e => (e.apply("squads_position_name"), e.apply("squads_shirt_number")))
      .filter(e => e._2 != "0")
      .groupBy(e => e._1)

    val masComunes: Map[String, String] = nrosPosiciones.view.mapValues(v =>
      v.map(_._2) // obtener solo números
        .groupBy(identity)
        .maxBy(_._2.size)._1 // número más común
    ).toMap

    println(masComunes)

}

