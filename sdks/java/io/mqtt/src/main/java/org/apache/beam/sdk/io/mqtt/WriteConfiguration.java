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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.mqtt.MqttIO.ConnectionConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class WriteConfiguration implements Serializable {

  public static Builder builder() {
    return new AutoValue_WriteConfiguration.Builder();
  }

  @SchemaFieldDescription("Configuration options to set up the MQTT connection.")
  public abstract ConnectionConfiguration getConnectionConfiguration();

  @SchemaFieldDescription(
      "Whether or not the publish message should be retained by the messaging engine. "
          + "When a subscriber connects, it gets the latest retained message. "
          + "Defaults to `False`, which will clear the retained message from the server.")
  @Nullable
  public abstract Boolean getRetained();

  // TODO schema field description
  @Nullable
  public abstract SerializableFunction<Row, byte[]> getPayloadFn();

  // TODO schema field description
  @Nullable
  public abstract SerializableFunction<Row, String> getTopicFn();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTopicFn(SerializableFunction<Row, String> topicFn);

    public abstract Builder setPayloadFn(SerializableFunction<Row, byte[]> payloadFn);

    public abstract Builder setConnectionConfiguration(ConnectionConfiguration config);

    public abstract Builder setRetained(Boolean retained);

    public abstract WriteConfiguration build();
  }
}
