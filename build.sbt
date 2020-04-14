import sbt._
import Keys._

name := "DPI_Recommender"

version := "0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

//scalaVersion := "2.10.5" // Configurar de acuerdo a la versión de scala. La 2.10.5 es la que se encuentra en el cluster de Telefonica (ejecutar util.Properties.versionString en spark-shell).

//libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.10" % "1.6.2","org.apache.spark" % "spark-mllib_2.10" % "1.6.2") // Configurar de acuerdo a la versión de scala. La 2.10.5 es la que se encuentra en el cluster de Telefonica

scalaVersion := "2.11.12" // Configurar de acuerdo a la versión de scala. La 2.11.12 es la que se encuentra en el cluster de Telefonica (ejecutar util.Properties.versionString en spark-shell).

//libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.10" % "2.3.2.3.1.4.0-315","org.apache.spark" % "spark-mllib_2.10" % "2.3.2.3.1.4.0-315") // Configurar de acuerdo a la versión de scala. La 2.10.5 es la que se encuentra en el cluster de Telefonica

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.0.0"
)
