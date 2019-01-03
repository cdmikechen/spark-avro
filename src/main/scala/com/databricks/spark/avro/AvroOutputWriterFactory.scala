/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.avro

import org.apache.avro.Schema
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private[avro] class AvroOutputWriterFactory(
    schema: StructType,
    avroSchemaAsJsonString: String,
    recordName: String,
    recordNamespace: String) extends OutputWriterFactory {

  private lazy val avroSchema = new Schema.Parser().parse(avroSchemaAsJsonString)

  override def getFileExtension(context: TaskAttemptContext): String = {
    ".avro"
  }

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    new AvroOutputWriter(path, context, schema, avroSchema, recordName, recordNamespace)
  }
}
