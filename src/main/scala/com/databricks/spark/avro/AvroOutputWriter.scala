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

import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util.HashMap

import scala.collection.immutable.Map

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[avro] class AvroOutputWriter(
                                      path: String,
                                      context: TaskAttemptContext,
                                      schema: StructType,
                                      avroSchema: Schema,
                                      recordName: String,
                                      recordNamespace: String) extends OutputWriter {

  // The input rows will never be null.
  private lazy val serializer = new AvroSerializer(schema, avroSchema, nullable = false)

//  /**
//    * Overrides the couple of methods responsible for generating the output streams / files so
//    * that the data can be correctly partitioned
//    */
//  private val recordWriter: RecordWriter[AvroKey[GenericRecord], NullWritable] =
//    new AvroKeyOutputFormat[GenericRecord]() {
//
//      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
//        new Path(path)
//      }
//
//      @throws(classOf[IOException])
//      override def getAvroFileOutputStream(c: TaskAttemptContext): OutputStream = {
//        val path = getDefaultWorkFile(context, ".avro")
//        path.getFileSystem(context.getConfiguration).create(path)
//      }
//
//    }.getRecordWriter(context)

//  private lazy val converter = createConverterToAvro(schema, recordName,
//    SchemaNsNaming.fromName(recordNamespace))
  // copy of the old conversion logic after api change in SPARK-19085
//  private lazy val internalRowConverter =
//    CatalystTypeConverters.createToScalaConverter(schema).asInstanceOf[InternalRow => Row]

  /**
    * Overrides the couple of methods responsible for generating the output streams / files so
    * that the data can be correctly partitioned
    */
  private val recordWriter: RecordWriter[AvroKey[GenericRecord], NullWritable] =
    new AvroKeyOutputFormat[GenericRecord]() {

      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }

      @throws(classOf[IOException])
      override def getAvroFileOutputStream(c: TaskAttemptContext): OutputStream = {
        val path = getDefaultWorkFile(context, ".avro")
        path.getFileSystem(context.getConfiguration).create(path)
      }

    }.getRecordWriter(context)

  // this is the new api in spark 2.2+
  def write(row: InternalRow): Unit = {
//    write(internalRowConverter(row))
    val key = new AvroKey(serializer.serialize(row).asInstanceOf[GenericRecord])
    recordWriter.write(key, NullWritable.get())
  }

  // api in spark 2.0 - 2.1
//  def write(row: Row): Unit = {
//    val key = new AvroKey(converter(row).asInstanceOf[GenericRecord])
//    recordWriter.write(key, NullWritable.get())
//  }

  override def close(): Unit = recordWriter.close(context)

  /**
    * This function constructs converter function for a given sparkSQL datatype. This is used in
    * writing Avro records out to disk
    */
//  private def createConverterToAvro(
//                                     dataType: DataType,
//                                     structName: String,
//                                     recordNamespace: SchemaNsNaming): (Any) => Any = {
//    dataType match {
//      case BinaryType => (item: Any) => item match {
//        case null => null
//        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
//      }
//      case ByteType | ShortType | IntegerType | LongType |
//           FloatType | DoubleType | StringType | BooleanType => identity
//      case d: DecimalType => (item: Any) =>
//        Option(item).map { i =>
//          val bigDecimalValue = i.asInstanceOf[java.math.BigDecimal]
//          val unscaledValue = bigDecimalValue.unscaledValue()
//          ByteBuffer.wrap(unscaledValue.toByteArray)
//        }.orNull
//      case TimestampType => (item: Any) =>
//        Option(item).map(_.asInstanceOf[Timestamp].getTime).orNull
//      case DateType => (item: Any) =>
//        Option(item).map(_.asInstanceOf[Date].toLocalDate.toEpochDay.toInt).orNull
//      case ArrayType(elementType, _) =>
//        val elementConverter = createConverterToAvro(
//          elementType,
//          structName,
//          recordNamespace.arrayFieldNaming(structName, elementType))
//        (item: Any) => {
//          if (item == null) {
//            null
//          } else {
//            val sourceArray = item.asInstanceOf[Seq[Any]]
//            val sourceArraySize = sourceArray.size
//            val targetArray = new Array[Any](sourceArraySize)
//            var idx = 0
//            while (idx < sourceArraySize) {
//              targetArray(idx) = elementConverter(sourceArray(idx))
//              idx += 1
//            }
//            targetArray
//          }
//        }
//      case MapType(StringType, valueType, _) =>
//        val valueConverter = createConverterToAvro(
//          valueType,
//          structName,
//          recordNamespace.mapFieldNaming(structName, valueType))
//        (item: Any) => {
//          if (item == null) {
//            null
//          } else {
//            val javaMap = new HashMap[String, Any]()
//            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
//              javaMap.put(key, valueConverter(value))
//            }
//            javaMap
//          }
//        }
//      case structType: StructType =>
//        val builder = SchemaBuilder.record(structName).namespace(recordNamespace.currentNamespace)
//        val schema: Schema = SchemaConverters.convertStructToAvro(
//          structType, builder, recordNamespace)
//        val fieldConverters = structType.fields.map(field =>
//          createConverterToAvro(
//            field.dataType,
//            field.name,
//            recordNamespace.structFieldNaming(field.name)))
//        (item: Any) => {
//          if (item == null) {
//            null
//          } else {
//            val record = new Record(schema)
//            val convertersIterator = fieldConverters.iterator
//            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
//            val rowIterator = item.asInstanceOf[Row].toSeq.iterator
//
//            while (convertersIterator.hasNext) {
//              val converter = convertersIterator.next()
//              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
//            }
//            record
//          }
//        }
//    }
//  }
}