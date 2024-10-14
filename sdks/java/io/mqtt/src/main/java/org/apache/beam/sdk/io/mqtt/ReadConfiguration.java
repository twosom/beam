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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ReadConfiguration implements Serializable {
  public static Builder builder() {
    return new AutoValue_ReadConfiguration.Builder();
  }

  @SchemaFieldDescription("Configuration options to set up the MQTT connection.")
  public abstract MqttIO.ConnectionConfiguration getConnectionConfiguration();

  @SchemaFieldDescription(
      "The max number of records to receive. Setting this will result in a bounded PCollection.")
  @Nullable
  public abstract Long getMaxNumRecords();

  @SchemaFieldDescription(
      "The maximum time for this source to read messages. Setting this will result in a bounded PCollection.")
  @Nullable
  public abstract Long getMaxReadTimeSeconds();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setConnectionConfiguration(
        MqttIO.ConnectionConfiguration connectionConfiguration);

    public abstract Builder setMaxNumRecords(Long maxNumRecords);

    public abstract Builder setMaxReadTimeSeconds(Long maxReadTimeSeconds);

    public abstract ReadConfiguration build();
  }
}
