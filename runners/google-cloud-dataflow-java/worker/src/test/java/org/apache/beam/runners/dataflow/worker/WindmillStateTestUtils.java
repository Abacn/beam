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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertFalse;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** Static helpers for testing Windmill state. */
public class WindmillStateTestUtils {
  /**
   * Assert that no field (including compiler-generated fields) within {@code obj} point back to a
   * non-null instance of (a subclass of) {@code clazz}.
   */
  public static void assertNoReference(Object obj, Class<?> clazz) throws Exception {
    assertNoReference(obj, clazz, new ArrayList<String>(), new HashSet<Object>());
  }

  public static void assertNoReference(
      Object obj, Class<?> clazz, ArrayList<String> path, HashSet<Object> visited)
      throws Exception {
    if (obj == null || visited.contains(obj)) {
      return;
    }
    Class<?> thisClazz = obj.getClass();
    assertFalse(
        "Found invalid path " + path + " back to " + clazz.getName(),
        clazz.isAssignableFrom(thisClazz));

    visited.add(obj);
    if (obj instanceof Object[]) {
      Object[] arr = (Object[]) obj;
      for (int i = 0; i < arr.length; i++) {
        try {
          path.add(thisClazz.getName() + "[" + i + "]");
          assertNoReference(arr[i], clazz, path, visited);
        } finally {
          path.remove(path.size() - 1);
        }
      }
    } else if (obj instanceof AtomicReferenceArray) {
      AtomicReferenceArray<?> arr = (AtomicReferenceArray<?>) obj;
      for (int i = 0; i < arr.length(); i++) {
        try {
          path.add(thisClazz.getName() + "[" + i + "]");
          assertNoReference(arr.get(i), clazz, path, visited);
        } finally {
          path.remove(path.size() - 1);
        }
      }
    } else if (obj instanceof java.util.concurrent.ConcurrentLinkedQueue) {
      int i = 0;
      for (Object o : (ConcurrentLinkedQueue<?>) obj) {
        try {
          path.add(thisClazz.getName() + "[" + i + "]");
          assertNoReference(o, clazz, path, visited);
        } finally {
          path.remove(path.size() - 1);
        }
        ++i;
      }
    } else if ((obj instanceof Number)
        || (obj instanceof String)
        || (obj instanceof Boolean)
        || (obj instanceof Enum)
        || (obj instanceof java.util.concurrent.locks.Lock)) {
      // Skip primitives
      return;
    } else {
      System.out.println("Checking " + thisClazz.getName());
      Class<?> currClazz = thisClazz;
      while (currClazz != null) {
        for (Field f : currClazz.getDeclaredFields()) {
          if (Modifier.isStatic(f.getModifiers())) {
            continue;
          }

          boolean accessible = f.isAccessible();
          try {
            path.add(thisClazz.getName() + "#" + f.getName());
            f.setAccessible(true);
            assertNoReference(f.get(obj), clazz, path, visited);
          } finally {
            path.remove(path.size() - 1);
            f.setAccessible(accessible);
          }
        }
        currClazz = currClazz.getSuperclass();
      }
    }
  }
}
