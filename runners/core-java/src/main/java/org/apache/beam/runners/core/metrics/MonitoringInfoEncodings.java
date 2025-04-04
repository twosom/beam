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
package org.apache.beam.runners.core.metrics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrie;
import org.apache.beam.model.pipeline.v1.MetricsApi.HistogramValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;

/** A set of functions used to encode and decode common monitoring info types. */
public class MonitoringInfoEncodings {
  private static final Coder<Long> VARINT_CODER = VarLongCoder.of();
  private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();
  private static final IterableCoder<String> STRING_SET_CODER =
      IterableCoder.of(StringUtf8Coder.of());

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#DISTRIBUTION_INT64_TYPE}. */
  public static ByteString encodeInt64Distribution(DistributionData data) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(data.count(), output);
      VARINT_CODER.encode(data.sum(), output);
      VARINT_CODER.encode(data.min(), output);
      VARINT_CODER.encode(data.max(), output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#DISTRIBUTION_INT64_TYPE}. */
  public static DistributionData decodeInt64Distribution(ByteString payload) {
    InputStream input = payload.newInput();
    try {
      long count = VARINT_CODER.decode(input);
      long sum = VARINT_CODER.decode(input);
      long min = VARINT_CODER.decode(input);
      long max = VARINT_CODER.decode(input);
      return DistributionData.create(sum, count, min, max);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#DISTRIBUTION_DOUBLE_TYPE}. */
  // TODO(BEAM-4374): Implement decodeDoubleDistribution(...)
  public static ByteString encodeDoubleDistribution(
      long count, double sum, double min, double max) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(count, output);
      DOUBLE_CODER.encode(sum, output);
      DOUBLE_CODER.encode(min, output);
      DOUBLE_CODER.encode(max, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#LATEST_INT64_TYPE}. */
  public static ByteString encodeInt64Gauge(GaugeData data) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(data.timestamp().getMillis(), output);
      VARINT_CODER.encode(data.value(), output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#LATEST_INT64_TYPE}. */
  public static GaugeData decodeInt64Gauge(ByteString payload) {
    InputStream input = payload.newInput();
    try {
      Instant timestamp = new Instant(VARINT_CODER.decode(input));
      return GaugeData.create(VARINT_CODER.decode(input), timestamp);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#SET_STRING_TYPE}. */
  public static ByteString encodeStringSet(StringSetData data) {
    try (ByteStringOutputStream output = new ByteStringOutputStream()) {
      STRING_SET_CODER.encode(data.stringSet(), output);
      return output.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#SET_STRING_TYPE}. */
  public static StringSetData decodeStringSet(ByteString payload) {
    try (InputStream input = payload.newInput()) {
      Set<String> elements = Sets.newHashSet(STRING_SET_CODER.decode(input));
      return StringSetData.create(elements);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#BOUNDED_TRIE_TYPE}. */
  public static ByteString encodeBoundedTrie(BoundedTrieData data) {
    return data.toProto().toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#BOUNDED_TRIE_TYPE}. */
  public static BoundedTrieData decodeBoundedTrie(ByteString payload) {
    try {
      return BoundedTrieData.fromProto(BoundedTrie.parseFrom(payload));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#SUM_INT64_TYPE}. */
  public static ByteString encodeInt64Counter(long value) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(value, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#SUM_INT64_TYPE}. */
  public static long decodeInt64Counter(ByteString payload) {
    try {
      return VarLongCoder.of().decode(payload.newInput());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#SUM_DOUBLE_TYPE}. */
  public static ByteString encodeDoubleCounter(double value) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      DOUBLE_CODER.encode(value, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#SUM_DOUBLE_TYPE}. */
  public static double decodeDoubleCounter(ByteString payload) {
    try {
      return DOUBLE_CODER.decode(payload.newInput());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#HISTOGRAM}. */
  public static ByteString encodeInt64Histogram(HistogramData inputHistogram) {
    return inputHistogram.toProto().toByteString();
  }

  /** Decodes to {@link MonitoringInfoConstants.TypeUrns#HISTOGRAM}. */
  public static HistogramData decodeInt64Histogram(ByteString payload) {
    try {
      return new HistogramData(HistogramValue.parseFrom(payload));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
