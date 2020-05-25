package com.softsapiens.experiments

import java.net.URI

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

object Write2S3 extends App {
  val uri = new URI("s3a://<bucket>/<key>")
  val key = ???
  val secret = ???
  val sessionToken = ???
  val credentialsProvider = ???

  val config = new Configuration()
  config.set("fs.s3a.access.key", key)
  config.set("fs.s3a.secret.key", secret)

  val schema: Schema = ???

  val writer = AvroParquetWriter.builder[GenericRecord](new Path(uri)).withConf(config).withSchema(schema).build()

  writer.write(???)
  writer.close()
}
