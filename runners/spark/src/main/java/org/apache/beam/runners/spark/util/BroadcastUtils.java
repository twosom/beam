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
package org.apache.beam.runners.spark.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.broadcast.BroadcastManager;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Utility class for creating broadcast variables in Apache Spark. Handles compatibility with
 * different Spark versions.
 */
public class BroadcastUtils {
  public static final String NEW_BROADCAST_METHOD_NAME = "newBroadcast";
  private static @MonotonicNonNull BroadcastManager instance;

  @VisibleForTesting static @MonotonicNonNull Method newBroadcastMethod;

  /**
   * Gets the singleton instance of the Spark BroadcastManager.
   *
   * @return {@link BroadcastManager} instance
   */
  private static BroadcastManager getInstance() {
    if (instance == null) {
      instance = SparkEnv.get().broadcastManager();
    }
    return instance;
  }

  /**
   * Creates a new broadcast variable with the value provided. This method keeps both serialized and
   * unserialized versions of the broadcast object.
   *
   * @param <BroadcastT> the type of the broadcast value
   * @param value the value to broadcast
   * @return a Broadcast object containing the value
   */
  public static <BroadcastT> Broadcast<BroadcastT> newBroadcast(BroadcastT value) {
    return newBroadcast(value, false);
  }

  /**
   * Creates a new broadcast variable with the value provided. This method uses reflection to invoke
   * the appropriate newBroadcast method on the BroadcastManager based on the Spark version being
   * used. Reflection is necessary because the method signature changed after <a
   * href="https://issues.apache.org/jira/browse/SPARK-39983">SPARK-39983</a>
   *
   * <p>Original signature (with 2 parameters in Scala, 3 in Java including {@link
   * scala.reflect.ClassTag}) used in Spark versions before 3.4.0:
   *
   * <pre>{@code
   * def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T]
   * }</pre>
   *
   * <p>Updated signature (with 3 parameters in Scala, 4 in Java including {@link
   * scala.reflect.ClassTag}) introduced in Spark 3.4.0 and later:
   *
   * <pre>{@code
   * def newBroadcast[T: ClassTag](
   *   value_ : T,
   *   isLocal: Boolean,
   *   serializedOnly: Boolean = false): Broadcast[T]
   * }</pre>
   *
   * @param <BroadcastT> the type of the broadcast value
   * @param value the value to broadcast
   * @param serializedOnly if {@code true}, only keeps the serialized version of the broadcast
   *     object, which can significantly reduce memory usage on the driver for large objects. Note
   *     that this parameter is ignored in earlier Spark versions (before 3.4.0) that don't support
   *     it.
   * @return a Broadcast object containing the value
   */
  @SuppressWarnings({"unchecked", "nullness", "rawtypes"})
  public static <BroadcastT> Broadcast<BroadcastT> newBroadcast(
      BroadcastT value, boolean serializedOnly) {
    final BroadcastManager broadcastManager = getInstance();
    final Method newBroadcastMethod = getNewBroadcastMethod();

    try {
      switch (newBroadcastMethod.getParameterCount()) {
          // Earlier Spark versions (before 3.4.0) with 3 parameters (including ClassTag)
        case 3:
          return (Broadcast)
              newBroadcastMethod.invoke(
                  broadcastManager, value, isLocal(), JavaSparkContext$.MODULE$.fakeClassTag());
        case 4:
          // Newer Spark versions (3.4.0 and later) with 4 parameters (including serializedOnly and
          // ClassTag)
          return (Broadcast)
              newBroadcastMethod.invoke(
                  broadcastManager,
                  value,
                  isLocal(),
                  serializedOnly,
                  JavaSparkContext$.MODULE$.fakeClassTag());
        default:
          throw new RuntimeException(
              String.format(
                  "Unsupported parameter count %d for method '%s'. Expected 3 or 4 parameters.",
                  newBroadcastMethod.getParameterCount(), NEW_BROADCAST_METHOD_NAME));
      }

    } catch (InvocationTargetException e) {
      throw new RuntimeException(
          String.format(
              "Error invoking method '%s': %s",
              NEW_BROADCAST_METHOD_NAME, e.getTargetException().getMessage()),
          e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          String.format("Cannot access method '%s': %s", NEW_BROADCAST_METHOD_NAME, e.getMessage()),
          e);
    }
  }

  private static @NonNull Method getNewBroadcastMethod() {
    if (newBroadcastMethod == null) {
      final Optional<Method> newBroadcastMethodOptional =
          Arrays.stream(BroadcastManager.class.getDeclaredMethods())
              .filter(m -> m.getName().equals(NEW_BROADCAST_METHOD_NAME))
              .findFirst();

      checkState(
          newBroadcastMethodOptional.isPresent(),
          "Cannot find method '%s' in BroadcastManager class. This may indicate an incompatible Spark version.",
          NEW_BROADCAST_METHOD_NAME);
      newBroadcastMethod = newBroadcastMethodOptional.get();
    }

    return newBroadcastMethod;
  }

  /**
   * Determines if the Spark application is running in local mode.
   *
   * @return true if running in local mode, false otherwise
   */
  private static boolean isLocal() {
    return org.apache.spark.util.Utils.isLocalMaster(SparkEnv.get().conf());
  }
}
