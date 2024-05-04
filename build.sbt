name := "Etri Project"
version := "1.0"
scalaVersion := "2.11.12"
val sparkVersion = "2.3.1"
libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.3.1",
	"org.scala-lang" % "scala-compiler" % "2.11.12",
	"com.google.guava" % "guava" % "11.0.2",
        "commons-net" % "commons-net" % "2.2",
	"com.google.code.findbugs" % "jsr305" % "1.3.9"	,
	"org.rogach" %% "scallop" % "3.1.3"
)

resolvers += "MavenRepository" at "https://mvnrepository.com/"
autoScalaLibrary := false

