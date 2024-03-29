name         := "EEC_data_processing"
version      := "1.0"
scalaVersion := "2.11.0"

libraryDependencies +=  "com.github.scopt"  %   "scopt_2.11"           %  "3.6.0"
libraryDependencies +=  "org.scalatest"     %%  "scalatest"            %  "3.1.0"        %  Test
libraryDependencies +=  "mysql"             %   "mysql-connector-java" %  "8.0.15"
libraryDependencies +=  "org.apache.spark"  %%  "spark-sql"            %  "2.4.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}



