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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * CsvIO master class that takes an input of {@link PCollection<String>} and outputs custom type
 * {@link PCollection<T>}.
 */
class CsvIOParse<T> extends PTransform<PCollection<String>, PCollection<T>> {

  /** Stores required parameters for parsing. */
  private final CsvIOParseConfiguration.Builder<T> configBuilder;

  private final TupleTag<T> outputTag = new TupleTag<T>() {};

  private final TupleTag<BadRecord> errorTag = new TupleTag<BadRecord>() {};

  CsvIOParse(CsvIOParseConfiguration.Builder<T> configBuilder) {
    this.configBuilder = configBuilder;
  }

  /** Parses to custom type not specified under {@link Schema.FieldType}. */
  // TODO(https://github.com/apache/beam/issues/31875): Implement method.
  void withCustomRecordParsing() {}

  @Override
  public PCollection<T> expand(PCollection<String> input) {
    CsvIOParseConfiguration<T> config = configBuilder.build();
    try (ErrorHandler.BadRecordErrorHandler<PCollection<BadRecord>> errorHandler =
        input.getPipeline().registerBadRecordErrorHandler(config.getErrorHandler())) {
      PCollection<List<String>> records =
          input.apply(new CsvIOStringToCsvRecord(config.getCsvFormat()));
      ParseFn parseFn = new ParseFn(config);
      PCollectionTuple pct =
          records.apply(ParDo.of(parseFn).withOutputTags(outputTag, TupleTagList.of(errorTag)));
      errorHandler.addErrorCollection(pct.get(errorTag));
      return pct.get(outputTag).setCoder(config.getCoder());
    }
  }

  /** Parses CsvRecord values into custom user Schema-mapped types. */
  private class ParseFn extends DoFn<List<String>, T> {
    private final CsvIOParseConfiguration<T> configuration;
    private final Map<Integer, Schema.Field> fieldPositionMap;

    private ParseFn(CsvIOParseConfiguration<T> configuration) {
      this.configuration = configuration;
      fieldPositionMap =
          CsvIOParseHelpers.mapFieldPositions(
              configuration.getCsvFormat(), configuration.getSchema());
    }

    @ProcessElement
    public void process(@Element List<String> record, MultiOutputReceiver receiver) {
      try {
        Map<String, Object> fieldNamesAndValues = new HashMap<>();
        for (Map.Entry<Integer, Schema.Field> entry : fieldPositionMap.entrySet()) {
          int index = entry.getKey();
          Schema.Field field = entry.getValue();
          String cell = record.get(index);
          Object value = parseCell(cell, entry.getValue());
          fieldNamesAndValues.put(field.getName(), value);
        }
        Row row =
            Row.withSchema(configuration.getSchema()).withFieldValues(fieldNamesAndValues).build();
        receiver.get(outputTag).output(configuration.getFromRowFn().apply(row));
      } catch (RuntimeException e) {
        receiver
            .get(errorTag)
            .output(
                BadRecord.builder()
                    .setFailure(BadRecord.Failure.builder().setDescription("bad record").setException(e.getMessage()).build())
                    .setRecord(
                        BadRecord.Record.builder()
                            .addCoderAndEncodedRecord(ListCoder.of(StringUtf8Coder.of()), record)
                            .build())
                    .build());
        throw e;
      }
    }

    /** Parses cell to emit the value, as well as potential errors with filename. */
    Object parseCell(String cell, Schema.Field field) {
      Map<String, SerializableFunction<String, Object>> customProcessingMap =
          configuration.getCustomProcessingMap();
      if (cell == null && !field.getType().getNullable()) {
        throw new IllegalArgumentException(
            "Required schema field " + field.getName() + " has null value");
      } else if (cell == null && field.getType().getNullable()) {
        return cell;
      }
      if (!customProcessingMap.containsKey(field.getName())) {
        return CsvIOParseHelpers.parseCell(cell, field);
      }
      return customProcessingMap.get(field.getName()).apply(cell);
    }
  }
}
