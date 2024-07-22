/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.csv;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.math.BigDecimal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvIOParse}. */
@RunWith(JUnit4.class)
public class CsvIOParseTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();
  private static final String[] ALL_PRIMITIVE_DATA_TYPES_HEADER =
      new String[] {"aBoolean", "aDecimal", "aDouble", "aFloat", "anInteger", "aLong", "aString"};

  @Test
  public void isSerializable() {
    SerializableUtils.ensureSerializable(CsvIOParse.class);
  }

  @Test
  public void parsesToPOJOs() {
    SchemaAwareJavaBeans.AllPrimitiveDataTypes want =
        allPrimitiveDataTypes(true, BigDecimal.valueOf(1.0), 2.0d, 3.0f, 4, 5L, "foo");
    CSVFormat csvFormat = csvFormat();
    String header =
        String.join(String.valueOf(csvFormat.getDelimiter()), ALL_PRIMITIVE_DATA_TYPES_HEADER);
    String comment = csvFormat.getCommentMarker() + " This is a comment";
    PCollection<String> records =
        pipeline.apply(Create.of(comment, header, "true,1.0,2.0,3.0,4,5,foo"));
    CsvIOParse<SchemaAwareJavaBeans.AllPrimitiveDataTypes> underTest =
        new CsvIOParse<>(configurationBuilder(csvFormat));
    PCollection<SchemaAwareJavaBeans.AllPrimitiveDataTypes> got = records.apply(underTest);
    PAssert.that(got).containsInAnyOrder(want);
    pipeline.run();
  }

  @Test
  public void parsesToRows() {
    Schema schema = schema();
    Row want =
        Row.withSchema(schema)
            .withFieldValues(
                ImmutableMap.of(
                    "aBoolean",
                    true,
                    "aDecimal",
                    BigDecimal.valueOf(1.0),
                    "aDouble",
                    2.0,
                    "aFloat",
                    3.0f,
                    "anInteger",
                    4,
                    "aLong",
                    5L,
                    "aString",
                    "foo"))
            .build();
    CSVFormat csvFormat = csvFormat();
    String header =
        String.join(String.valueOf(csvFormat.getDelimiter()), ALL_PRIMITIVE_DATA_TYPES_HEADER);
    String comment = csvFormat.getCommentMarker() + " This is a comment";
    PCollection<String> records =
        pipeline.apply(Create.of(comment, header, "true,1.0,2.0,3.0,4,5,foo"));
    RowCoder rowCoder = RowCoder.of(schema);
    CsvIOParse<Row> underTest =
        new CsvIOParse<>(
            configurationBuilder(csvFormat(), schema, rowCoder.getFromRowFunction(), rowCoder));
    PCollection<Row> got = records.apply(underTest);
    PAssert.that(got).containsInAnyOrder(want);
    pipeline.run();
  }

  @Test
  public void testNullString() {
    SchemaAwareJavaBeans.AllPrimitiveDataTypes want =
        allPrimitiveDataTypes(true, BigDecimal.valueOf(1.0), 2.0d, 3.0f, 4, 5L, "foo");
    CSVFormat csvFormat = csvFormat().withNullString("foo");
    String header =
        String.join(String.valueOf(csvFormat.getDelimiter()), ALL_PRIMITIVE_DATA_TYPES_HEADER);
    PCollection<String> records = pipeline.apply(Create.of(header, "true,1.0,2.0,3.0,4,5,foo"));
    CsvIOParse<SchemaAwareJavaBeans.AllPrimitiveDataTypes> underTest =
        new CsvIOParse<>(configurationBuilder(csvFormat));
    PCollection<SchemaAwareJavaBeans.AllPrimitiveDataTypes> got = records.apply(underTest);
    PAssert.that(got).containsInAnyOrder(want);
    pipeline.run();
  }

  @Test
  public void testEmptyString() {
    pipeline.run();
  }

  @Test
  public void testDeadLetterQueue() {
    pipeline.run();
  }

  private static CsvIOParseConfiguration.Builder<SchemaAwareJavaBeans.AllPrimitiveDataTypes>
      configurationBuilder(CSVFormat csvFormat) {
    SerializableFunction<Row, SchemaAwareJavaBeans.AllPrimitiveDataTypes> fromRowFn =
        checkStateNotNull(allPrimitiveDataTypesFromRowFn());
    Schema schema = checkStateNotNull(ALL_PRIMITIVE_DATA_TYPES_SCHEMA);
    Coder<SchemaAwareJavaBeans.AllPrimitiveDataTypes> coder =
        SchemaCoder.of(
            schema,
            ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR,
            checkStateNotNull(allPrimitiveDataTypesToRowFn()),
            fromRowFn);
    return configurationBuilder(csvFormat, schema, fromRowFn, coder);
  }

  private static <T> CsvIOParseConfiguration.Builder<T> configurationBuilder(
      CSVFormat csvFormat, Schema schema, SerializableFunction<Row, T> fromRowFn, Coder<T> coder) {
    CsvIOParseConfiguration.Builder<T> builder = CsvIOParseConfiguration.builder();
    builder.setCsvFormat(csvFormat).setSchema(schema).setFromRowFn(fromRowFn).setCoder(coder);
    return builder;
  }

  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader(ALL_PRIMITIVE_DATA_TYPES_HEADER)
        .withCommentMarker('#');
  }

  private static Schema schema() {
    return Schema.builder()
        .addBooleanField("aBoolean")
        .addDecimalField("aDecimal")
        .addDoubleField("aDouble")
        .addFloatField("aFloat")
        .addInt32Field("anInteger")
        .addInt64Field("aLong")
        .addStringField("aString")
        .build();
  }
}
