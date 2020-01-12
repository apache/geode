/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc.internal.cli;

public final class MappingConstants {
  public static final String REGION_NAME = "region";
  public static final String PDX_NAME = "pdx-name";
  public static final String TABLE_NAME = "table";
  public static final String DATA_SOURCE_NAME = "data-source";
  public static final String SYNCHRONOUS_NAME = "synchronous";
  public static final String ID_NAME = "id";
  public static final String SPECIFIED_ID_NAME = "id-user-specified";
  public static final String SCHEMA_NAME = "schema";
  public static final String CATALOG_NAME = "catalog";
  public static final String GROUP_NAME = "groups";
  public static final String PDX_CLASS_FILE = "pdx-class-file";

  public static final String THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION =
      "There is no jdbc mapping on proxy region.";

  private MappingConstants() {}
}
