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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores parameters needed for CSV record parsing. */
@AutoValue
abstract class CsvIOParseConfiguration<T> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CsvIOParseConfiguration.class);

  static <T> Builder<T> builder() {
    return new AutoValue_CsvIOParseConfiguration.Builder<>();
  }

  /** The expected {@link CSVFormat} of the parsed CSV record. */
  abstract CSVFormat getCsvFormat();

  /** The expected {@link Schema} of the target type. */
  abstract Schema getSchema();

  /** A map of the {@link Schema.Field#getName()} to the custom CSV processing lambda. */
  abstract Map<String, SerializableFunction<String, Object>> getCustomProcessingMap();

  /** A {@link SerializableFunction} from {@link Row} to custom Schema-mapped type. */
  abstract SerializableFunction<Row, T> getFromRowFn();

  /** The expected {@link Coder} for with the output type. */
  abstract Coder<T> getCoder();

  abstract PTransform<PCollection<BadRecord>, PCollection<BadRecord>> getErrorHandler();

  @AutoValue.Builder
  abstract static class Builder<T> implements Serializable {
    abstract Builder<T> setCsvFormat(CSVFormat csvFormat);

    abstract Builder<T> setSchema(Schema schema);

    abstract Builder<T> setCustomProcessingMap(
        Map<String, SerializableFunction<String, Object>> customProcessingMap);

    abstract Optional<Map<String, SerializableFunction<String, Object>>> getCustomProcessingMap();

    abstract Builder<T> setFromRowFn(SerializableFunction<Row, T> fromRowFn);

    abstract Builder<T> setCoder(Coder<T> coder);

    abstract Builder<T> setErrorHandler(
        PTransform<PCollection<BadRecord>, PCollection<BadRecord>> errorHandler);

    abstract Optional<PTransform<PCollection<BadRecord>, PCollection<BadRecord>>> getErrorHandler();

    abstract CsvIOParseConfiguration<T> autoBuild();

    final CsvIOParseConfiguration<T> build() {
      if (!getCustomProcessingMap().isPresent()) {
        setCustomProcessingMap(new HashMap<>());
      }
      if (!getErrorHandler().isPresent()) {
        setErrorHandler(new BadRecordOutput());
      }
      return autoBuild();
    }
  }

  private static class BadRecordOutput
      extends PTransform<PCollection<BadRecord>, PCollection<BadRecord>> {

    @Override
    public PCollection<BadRecord> expand(PCollection<BadRecord> input) {
      return input.apply(ParDo.of(new BadRecordTransformFn()));
    }

    private static class BadRecordTransformFn extends DoFn<BadRecord, BadRecord> {

      @ProcessElement
      public void process(@Element BadRecord input, OutputReceiver<BadRecord> receiver) {
        LOG.error("{}: {}", Instant.now(), input);
        receiver.output(input);
      }
    }
  }
}
