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
package org.apache.beam.sdk.io.mqtt;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;

@AutoService(SchemaTransformProvider.class)
public class MqttReadWithMetadataSchemaTransformProvider
    extends TypedSchemaTransformProvider<ReadConfiguration> {

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:mqtt_read_with_metadata:v1";
  }

  @Override
  protected SchemaTransform from(ReadConfiguration configuration) {
    return new MqttReadWithMetadataSchemaTransform(configuration);
  }

  private static class MqttReadWithMetadataSchemaTransform extends SchemaTransform {
    private final ReadConfiguration config;

    private MqttReadWithMetadataSchemaTransform(ReadConfiguration config) {
      this.config = config;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Preconditions.checkState(
          input.getAll().isEmpty(),
          "Expected zero input PCollections for this source, but found: %",
          input.getAll().keySet());

      MqttIO.Read<MqttRecord> readWithMetadataTransform =
          MqttIO.readWithMetadata()
              .withConnectionConfiguration(config.getConnectionConfiguration());

      Long maxRecords = config.getMaxNumRecords();
      Long maxReadTime = config.getMaxReadTimeSeconds();
      if (maxRecords != null) {
        readWithMetadataTransform = readWithMetadataTransform.withMaxNumRecords(maxRecords);
      }
      if (maxReadTime != null) {
        readWithMetadataTransform =
            readWithMetadataTransform.withMaxReadTime(Duration.standardSeconds(maxReadTime));
      }

      Schema outputSchema;
      SerializableFunction<MqttRecord, Row> toRowFn;
      try {
        outputSchema = input.getPipeline().getSchemaRegistry().getSchema(MqttRecord.class);
        toRowFn = input.getPipeline().getSchemaRegistry().getToRowFunction(MqttRecord.class);
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }

      final PCollection<Row> outputRows =
          input
              .getPipeline()
              .apply(readWithMetadataTransform)
              .apply("Wrap in Beam Rows", MapElements.into(TypeDescriptors.rows()).via(toRowFn))
              .setRowSchema(outputSchema);

      return PCollectionRowTuple.of("output", outputRows);
    }
  }
}
