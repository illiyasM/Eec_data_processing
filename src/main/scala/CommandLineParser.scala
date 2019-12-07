
import scopt.OptionParser

case class Config(action: String = null,
                  databaseName: String = null,
                  isOverwrite: Boolean = false,
                  execTime: Option[String] = None,
                  inputFileName: Option[String] = None,
                  inputCsvHeader: Option[Boolean] = None
                 )

object CommandLineParser extends App {

  val parser = new OptionParser[Config]("data_processing") {

    //head("data_processing", "1.x")
    opt[String]('d', "databaseName")
      .required()
      .action { (x, c) => c.copy(databaseName = x) }
      .text("Name of the target database")
    cmd("computation")
      .action{(_,c) => c.copy(action = "computation")}
      .children {
        opt[Unit]('o', "isOverwrite")
          .optional()
          .action { (_, c) => c.copy(isOverwrite = false) }
          .text("Overwrite or append calculated data into the target database")
        opt[String]('t', "execTime")
          .optional()
          .action { (x, c) => c.copy(execTime = Option(x)) }
          .text("calculation will be performed one hour before this time period")
      }
    cmd("feeding")
      .action{(_,c) => c.copy(action = "feeding")}
      .children {
        opt[String]('f', "inputFileName")
          .optional()
          .action { (x, c) => c.copy(inputFileName = Option(x)) }
          .text("Name of the input CSV file containing readings")
        opt[Unit]('t', "inputCsvHeader")
          .optional()
          .action { (_, c) => c.copy(inputCsvHeader = Option(true)) }
          .text("Define the presence of a header in the input data")
      }
  }

  parser.parse(args, Config()) match {
    case Some(conf) => EecDataProcessing.Main(conf)
    case _          =>
      throw new IllegalArgumentException("The arguments entered are incorrect. End of execution.")
      System.exit(1)
  }
}
