package ec.edu.utpl.presencial.computacion.pfr.pintegra
import com.github.tototoshi.csv.*

import java.io.File



object LeerDatos {

  // Archivo partidos y goles
  @main // @main permite que cualquier función sea ejecutable
  def PartidosyGoles() =
    val path2DataFile: String = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\dsPartidosYGoles.csv\\"
    val reader = CSVReader.open(new File(path2DataFile)) // Lector del archivo
    // val contentFile: List[List[String]] = reader.all() // Lee todo como una lista de listas
    val contentFile: List[Map[String, String]] = reader.allWithHeaders() // Lee como una lista de Mapas donde la clave es el nombre de la columna

    reader.close() // Cierra el flujo de datos
    println(contentFile)

    println(s"Filas: ${contentFile.length} y Columnas: ${contentFile(0).keys.size}")

/*
  // Archivo alienaciones por torneo
  @main
  def AlineacionesTorneo() =
    val path2DataFile: String = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\dsAlineacionesXTorneo-2.csv\\"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()

    reader.close()
    println(s"Filas: ${contentFile.length} y Columnas: ${contentFile(0).keys.size}")
*/
}
