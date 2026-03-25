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
package org.apache.beam.io.debezium;

import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.testing.testcontainers.MongoDbContainer;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class DebeziumIOMongoDbConnectorIT implements Serializable {

  @ClassRule
  public static final MongoDbContainer MONGO_DB_CONTAINER =
      MongoDbContainer.node()
          .imageName(DockerImageName.parse("quay.io/debezium/example-mongodb:3.1.3.Final"))
          .name(DebeziumIOMongoDbConnectorIT.class.getName())
          .replicaSet("rs0")
          .skipDockerDesktopLogWarning(true)
          .build()
          .withExposedPorts(27017);

  /**
   * Debezium - MongoDB connector Test.
   *
   * <p>Tests that connector can actually connect to the database
   */
  @Test
  public void testDebeziumIOMongoDb() {
    MONGO_DB_CONTAINER.start();

    String host = MONGO_DB_CONTAINER.getHost();
    String port = MONGO_DB_CONTAINER.getMappedPort(27017).toString();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> unused =
        p.apply(
                DebeziumIO.<String>read()
                    .withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                            .withConnectorClass(MongoDbConnector.class)
                            .withHostName(host)
                            .withPort(port)
                            .withUsername("debezium")
                            .withPassword("dbz")
                            .withConnectionProperty("topic.prefix", "mongo-prefix")
                            .withConnectionProperty("database.include.list", "inventory")
                            .withConnectionProperty(
                                "collection.include.list", "inventory.customers")
                    )
                    .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                    .withMaxNumberOfRecords(30)
                    .withCoder(StringUtf8Coder.of()))
            .apply(
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        System.out.println(c.element());
                        c.output(c.element());
                      }
                    }));

    // TODO: Insert test data using MongoDB driver and add expected PAssert results
    // PAssert.that(results).satisfies(...);

    p.run().waitUntilFinish();

    MONGO_DB_CONTAINER.stop();
  }
}
