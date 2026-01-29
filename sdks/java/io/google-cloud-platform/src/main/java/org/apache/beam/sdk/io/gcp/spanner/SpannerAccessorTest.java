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
package org.apache.beam.sdk.io.gcp.spanner;

// Option 1: Run this class in IntelliJ
// Option 2: Run with a fat jar:
//   ./gradlew :sdks:java:io:google-cloud-platform:expansion-service:shadowJar
//   java -cp \
// sdks/java/io/google-cloud-platform/expansion-service/build/libs/beam-sdks-java-io-google-cloud-platform-expansion-service-2.72.0-SNAPSHOT.jar org.apache.beam.sdk.io.gcp.spanner.SpannerAccessorTest
public class SpannerAccessorTest {
  public static void main(String[] args) {
    SpannerConfig config =
        SpannerConfig.create()
            .withProjectId("cloud-teleport-testing")
            .withInstanceId("beam-test")
            .withDatabaseId("test-271");
    SpannerAccessor accessor = SpannerAccessor.getOrCreate(config);
  }
}
