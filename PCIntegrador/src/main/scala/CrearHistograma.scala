package ec.edu.utpl.presencial.computacion.pfr.pintegra
import com.github.tototoshi.csv.*
import java.io.File
import org.nspl.*
import org.nspl.awtrenderer.*
import org.nspl.data.HistogramData

object CrearHistograma {

  @main
  def work() =
    val path2DataFile: String = "C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\dsAlineacionesXTorneo-2.csv\\"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()

    reader.close()
    charting(contentFile)

  def charting(data: List[Map[String, String]]): Unit =
    val listNroShirt: List[Double] = data
      .filter(row => row("squads_position_name") == "forward" && row("squads_shirt_number") != "0")
      .map(row => row("squads_shirt_number").toDouble)

    val histForwardShirtNumber = xyplot(HistogramData(listNroShirt, 20) -> bar())(
      par
        .xlab("Shirt number")
        .ylab("freq.")
        .main("Forward shirt number")
    )

    pngToFile(new File("C:\\Users\\ljmor\\Documents\\Universidad\\Ciclo03\\ProgramacionFR\\image.png\\"),
      histForwardShirtNumber.build, width = 1000)

    renderToByteArray(histForwardShirtNumber.build, width = 2000)

}
