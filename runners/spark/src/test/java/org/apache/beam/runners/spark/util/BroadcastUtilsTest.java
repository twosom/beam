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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Method;
import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.VersionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class BroadcastUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastUtilsTest.class);
  public static final int AFTER_SIGNATURE_CHANGED = 4;
  public static final int BEFORE_SIGNATURE_CHANGED = 3;
  @ClassRule public static SparkContextRule contextRule = new SparkContextRule();

  @Test
  public void createBroadcastTest() {
    // Get the current Spark version from the context
    final String currentVersion = contextRule.getSparkContext().version();
    LOG.debug("current spark version : {}", currentVersion);

    // Create a broadcast variable with a test string value
    final Broadcast<String> broadcastVariable = BroadcastUtils.newBroadcast("test-value");

    // Verify the broadcast variable is properly created and contains the expected value
    assertNotNull(broadcastVariable.value());
    assertEquals("test-value", broadcastVariable.value());

    // Get the reflection Method object that BroadcastUtils uses internally
    final Method newBroadcastMethod = BroadcastUtils.newBroadcastMethod;
    assertNotNull(newBroadcastMethod);

    // Check if the method signature matches what we expect based on the Spark version
    // Spark 3.4.0+ uses 4 parameters, earlier versions use 3 parameters
    if (this.isSignatureChangedVersion(currentVersion)) {
      assertEquals(AFTER_SIGNATURE_CHANGED, newBroadcastMethod.getParameterCount());
    } else {
      assertEquals(BEFORE_SIGNATURE_CHANGED, newBroadcastMethod.getParameterCount());
    }
  }

  /**
   * Determines if the current Spark version has the changed method signature (Spark 3.4.0 and later
   * changed the BroadcastManager.newBroadcast method).
   *
   * @param currentVersion the Spark version string
   * @return true if using Spark 3.4.0 or later, false otherwise
   */
  private boolean isSignatureChangedVersion(String currentVersion) {
    final Tuple2<Integer, Integer> parsedVersion = this.getCurrentSparkVersion(currentVersion);
    return parsedVersion._1() >= 3 && parsedVersion._2() >= 4;
  }

  /**
   * Parses the Spark version string into a tuple of (major, minor) version numbers Uses Spark's
   * built-in VersionUtils to handle the parsing.
   *
   * @param currentVersion the Spark version string
   * @return Tuple2 containing (major, minor) version numbers
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Tuple2<Integer, Integer> getCurrentSparkVersion(String currentVersion) {
    return (Tuple2<Integer, Integer>) (Tuple2) VersionUtils.majorMinorVersion(currentVersion);
  }
}
