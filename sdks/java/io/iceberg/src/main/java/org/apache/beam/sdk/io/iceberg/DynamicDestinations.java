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
package org.apache.beam.sdk.io.iceberg;

import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.iceberg.catalog.TableIdentifier;

public interface DynamicDestinations extends Serializable {

  Schema getDataSchema();

  Row getData(Row element);

  IcebergDestination instantiateDestination(String destination);

  String getTableStringIdentifier(ValueInSingleWindow<Row> element);

  static DynamicDestinations singleTable(TableIdentifier tableId, Schema inputSchema) {
    return new OneTableDynamicDestinations(tableId, inputSchema);
  }
}
