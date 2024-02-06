package ec.edu.utpl.presencial.computacion.pfr.pintegra

import com.github.tototoshi.csv.*
import java.io.File
import org.nspl.*
import org.nspl.awtrenderer.*
import org.nspl.data.HistogramData
import org.saddle.{Index, Series, Vec}

object TallerSem14 {

  @main
  def Taller() =
    val path2DataFile: String = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\dsPartidosYGoles.csv\\"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    frecuenciaMinutos(contentFile) // Pregunta 01
    datosGrafica(contentFile) // Pregunta 02


  def frecuenciaMinutos(data: List[Map[String, String]]): Unit =
    val listMinGoals: List[Double] = data
      .map(row => row("goals_minute_regulation")).filter(e => e != "NA").map(_.toDouble)

    val histMinGoals = xyplot(HistogramData(listMinGoals, 20) -> bar())(
        par
          .xlab("Goals Minute")
          .ylab("freq.")
          .main("Minutos Goles Marcados")
    )

    pngToFile(new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\pregunta01.png\\"),
      histMinGoals.build, width = 1000)


  def datosGrafica(data: List[Map[String, String]]) =
    val dataGoles = data
      .filter(_("tournaments_tournament_name").contains("Men")) // Filtrar solo aquellas filas que sean un torneo femenino
      // Transformar la lista de Mapas en una lista de tuplas en la que consta el nombre del torneo, el id del partido,
      // el puntaje del equipo local y el puntaje del equipo rival
      .map(row => (
        row("tournaments_tournament_name"),
        row("matches_match_id"),
        row("matches_home_team_score"),
        row("matches_away_team_score")
      ))
      .distinct // Se hace uso de un distinct para eliminar valores duplicados
      // Se mapea la lista de tuplas a una nueva lista de tuplas en las que el primer valor será el nombre del torneo y
      // el segundo la suma de los puntajes de ambos equipos
      .map(t4 => (t4._1, t4._3.toInt + t4._4.toInt))
      // Agrupamos por el nombre del torneo que nos dará un mapa en el que las claves es el nombre del torneo y el valor
      // una lista que aloja las tuplas de (nombre torneo, puntaje)
      .groupBy(_._1)
      // Transformamos el mapa para obtener como clave el nombre del torneo y como valor la sumatoria de todos los
      // puntajes de sus partidas
      .map(t2 => (t2._1, t2._2.map(_._2).sum))
      .toList // Transformamos el mapa a una lista de tuplas
      .sortBy(_._1) // Ordenamos la lista por el nombre de los torneos
    dataGoles
    charBarPlot(dataGoles)

  def charBarPlot(data: List[(String, Int)]): Unit = {

    // Mapea los valores de la lista de entrada a una nueva lista de tuplas,
    // convirtiendo el segundo elemento de cada tupla (que es de tipo Int) a Double.

    val data4Chart: List[(String, Double)] = data
      .map(t2 => (t2._1, t2._2.toDouble))

    // Crea un índice para el gráfico, tomando los primeros cuatro caracteres de
    // cada nombre en la lista y convirtiéndolos en un array.

    val indices = Index(data4Chart.map(value => value._1.substring(0, 4)).toArray)

    // Crea un vector de valores para el gráfico, tomando los valores convertidos
    // a Double de la lista y convirtiéndolos en un array.

    val values = Vec(data4Chart.map(value => value._2).toArray)

    // Crea una serie utilizando el índice y los valores.

    val series = Series(indices, values)
    // Crea un gráfico de barras horizontales utilizando la biblioteca Saddle,
    // estableciendo el tamaño de la fuente del eje x.

    val bar1 = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(1)),

      // Define la paleta de colores y personaliza el gráfico estableciendo la
      // rotación del texto del eje x y el número de marcas en el eje x.

      // Esto establece la paleta de colores del gráfico. En este caso, parece
      // utilizar una paleta que va desde el color rojo al azul. Los números entre
      // paréntesis (86 y 146) podrían representar los extremos del rango de colores
      // en algún espacio de color específico
      // Configura la rotación del texto en el eje x a -77 grados. Esto puede ser
      // útil si los nombres de las categorías en el eje x son largos y la rotación
      // ayuda a que no se superpongan.

      //  Establece el número de marcas (ticks) en el eje x en 0.

      color = RedBlue(40, 210))
      (par
        .xLabelRotation(-77).xNumTicks(0)
        .xlab("Año Torneo")
        .ylab("Goles Marcados")
        .main("Goles Torneos Masculinos")
      )

    pngToFile(new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\pregunta021.png\\")
      , bar1.build, 5000)

  }

}
