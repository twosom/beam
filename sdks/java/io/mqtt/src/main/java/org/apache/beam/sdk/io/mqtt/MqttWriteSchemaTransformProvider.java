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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

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

@AutoService(SchemaTransformProvider.class)
public class MqttWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<WriteConfiguration> {

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:mqtt_write:v1";
  }

  @Override
  protected SchemaTransform from(WriteConfiguration configuration) {
    return new MqttWriteSchemaTransform(configuration);
  }

  private static class MqttWriteSchemaTransform extends SchemaTransform {
    private final WriteConfiguration config;

    MqttWriteSchemaTransform(WriteConfiguration configuration) {
      this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> inputRows = input.getSinglePCollection();

      // TODO validate payloadFn, topicFn?
      checkState(
          inputRows.getSchema().getFieldCount() == 1
              && inputRows.getSchema().getField(0).getType().equals(Schema.FieldType.BYTES),
          "Expected only one Schema field containing bytes, but instead received: %s",
          inputRows.getSchema());

      MqttIO.Write<byte[]> writeTransform =
          MqttIO.write().withConnectionConfiguration(config.getConnectionConfiguration());
      Boolean retained = config.getRetained();
      if (retained != null) {
        writeTransform = writeTransform.withRetained(retained);
      }

      inputRows
          .apply(
              "Extract bytes",
              ParDo.of(
                  new DoFn<Row, byte[]>() {
                    @ProcessElement
                    public void processElement(
                        @Element Row row, OutputReceiver<byte[]> outputReceiver) {
                      outputReceiver.output(
                          org.apache.beam.sdk.util.Preconditions.checkStateNotNull(
                              row.getBytes(0)));
                    }
                  }))
          .apply(writeTransform);

      return PCollectionRowTuple.empty(inputRows.getPipeline());
    }
  }
}
