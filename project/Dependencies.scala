import sbt._

object Dependencies {

  val Kafka = Seq(Libs.`kafka-streams-scala`, Libs.`avro4s`, Libs.`embedded-kafka` % Test)

}
