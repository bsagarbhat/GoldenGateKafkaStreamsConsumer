import java.util.Properties

import com.lightbend.kafka.scala.streams.{DefaultSerdes, StreamsBuilderS}
import com.sksamuel.avro4s.AvroInputStream
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters._
import scala.collection.mutable

object Consumer extends App {
  val genericAvroSchema = new Schema.Parser().parse("{\n  \"type\" : \"record\",\n  \"name\" : \"generic_wrapper\",\n  \"namespace\" : \"oracle.goldengate\",\n  \"fields\" : [ {\n    \"name\" : \"table_name\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"schema_fingerprint\",\n    \"type\" : \"long\"\n  }, {\n    \"name\" : \"payload\",\n    \"type\" : \"bytes\"\n  } ]\n}")
  val ggschema = new Schema.Parser().parse("{\n  \"type\" : \"record\",\n  \"name\" : \"COUNTRIES\",\n  \"namespace\" : \"ORCLPDB1.HR\",\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\",\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, {\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"COUNTRY_ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"COUNTRY_ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"COUNTRY_NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"COUNTRY_NAME_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"REGION_ID\",\n        \"type\" : [ \"null\", \"double\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"REGION_ID_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}")
  val config = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties
  }

  private val builder = new StreamsBuilderS
  implicit val consumed = Consumed.`with`(DefaultSerdes.byteArraySerde, DefaultSerdes.byteArraySerde)
  builder.stream("ora-ogg-avro")
    .map((k, v) => {
      println("key is " + new String(k))
      (k, recordToString(v))
    })
    .print(Printed.toSysOut())

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

  def recordToString(record: Array[Byte]): String = {
    val genericWrapper = AvroInputStream.binary[GenericWrapper](record).iterator.toList.head

    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](ggschema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(genericWrapper.payload, null)
    val tableRow: GenericRecord = reader.read(null, decoder)
    val genericRecord = readRecordFromGoldenGate(tableRow)
    val fields: mutable.Buffer[Field] = getAllFieldsInSchema

    val missingFields: List[Field] = fields.filter(field => field.name().endsWith("_isMissing")).toList
    val columnsWithValues = missingFields.filter(p => !genericRecord.get(p.name()).asInstanceOf[Boolean])

    val colums = columnsWithValues.map(field => {
      val lastIndex = field.name().lastIndexOf("_isMissing")
      val name = field.name().substring(0, lastIndex)
      val value = genericRecord.get(name)
      if (value != null) Column(name, value.toString) else Column(name, null)
    })
    println(s"columns are $colums")
    tableRow.toString
  }

  private def getAllFieldsInSchema = {
    val schema: Schema = ggschema.getField("after").schema().getTypes.asScala.find(_.getName.equals("columns")).get
    val fields = schema.getFields.asScala
    fields
  }

  def readRecordFromGoldenGate(tableRow: GenericRecord): GenericRecord = {
    tableRow.get("op_type").toString match {
      case "D" => tableRow.get("before").asInstanceOf[GenericRecord]
      case _ => tableRow.get("after").asInstanceOf[GenericRecord]
      case "T" => throw new RuntimeException("cannot handle truncate")
    }
  }

}
