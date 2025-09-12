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
package org.apache.beam.sdk.expansion.service;

import java.net.URL;

public class CheckClass {
  public static void main(String[] argv) {
    String resourceName =
        "META-INF/maven/org.apache.avro/avro/pom.properties"; // Replace with your resource path

    ClassLoader classLoader = CheckClass.class.getClassLoader();
    if (classLoader != null) {
      URL resourceUrl = classLoader.getResource(resourceName);
      if (resourceUrl != null) {
        System.out.println(
            "Resource '" + resourceName + "' found at: " + resourceUrl.toExternalForm());
      } else {
        System.out.println("Resource '" + resourceName + "' not found.");
      }
    } else {
      System.out.println("ClassLoader not available.");
    }
  }
}
