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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;

@AutoService(SchemaTransformProvider.class)
public class MqttReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<ReadConfiguration> {

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:mqtt_read:v1";
  }

  @Override
  protected SchemaTransform from(ReadConfiguration configuration) {
    return new MqttReadSchemaTransform(configuration);
  }

  private static class MqttReadSchemaTransform extends SchemaTransform {
    private final ReadConfiguration config;

    MqttReadSchemaTransform(ReadConfiguration configuration) {
      this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Preconditions.checkState(
          input.getAll().isEmpty(),
          "Expected zero input PCollections for this source, but found: %",
          input.getAll().keySet());

      MqttIO.Read<byte[]> readTransform =
          MqttIO.read().withConnectionConfiguration(config.getConnectionConfiguration());

      Long maxRecords = config.getMaxNumRecords();
      Long maxReadTime = config.getMaxReadTimeSeconds();
      if (maxRecords != null) {
        readTransform = readTransform.withMaxNumRecords(maxRecords);
      }
      if (maxReadTime != null) {
        readTransform = readTransform.withMaxReadTime(Duration.standardSeconds(maxReadTime));
      }

      Schema outputSchema = Schema.builder().addByteArrayField("bytes").build();

      PCollection<Row> outputRows =
          input
              .getPipeline()
              .apply(readTransform)
              .apply(
                  "Wrap in Beam Rows",
                  ParDo.of(
                      new DoFn<byte[], Row>() {
                        @ProcessElement
                        public void processElement(
                            @Element byte[] data, OutputReceiver<Row> outputReceiver) {
                          outputReceiver.output(
                              Row.withSchema(outputSchema).addValue(data).build());
                        }
                      }))
              .setRowSchema(outputSchema);

      return PCollectionRowTuple.of("output", outputRows);
    }
  }
}
