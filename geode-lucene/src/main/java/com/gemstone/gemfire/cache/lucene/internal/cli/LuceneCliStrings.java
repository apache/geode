/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.cli;

public class LuceneCliStrings {
  //List Lucene Index commands
  public static final String LUCENE_LIST_INDEX = "list lucene indexes";
  public static final String LUCENE_LIST_INDEX__HELP = "Display the list of Lucene indexes created for all members.";
  public static final String LUCENE_LIST_INDEX__ERROR_MESSAGE = "An error occurred while collecting all Lucene Index information across the Geode cluster: %1$s";
  public static final String LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE = "No Lucene Indexes Found";

  public static final String LUCENE_CREATE_INDEX = "create lucene index";
  public static final String LUCENE_CREATE_INDEX__HELP = "Create an Lucene index that can be used when executing queries.";
  public static final String LUCENE_CREATE_INDEX__NAME = "name";
  public static final String LUCENE_CREATE_INDEX__NAME__HELP = "Name of the Lucene index to create.";
  public static final String LUCENE_CREATE_INDEX__REGION = "region";
  public static final String LUCENE_CREATE_INDEX__REGION_HELP = "Name/Path of the region where the Lucene index is created on.";
  public static final String LUCENE_CREATE_INDEX__FIELD = "field";
  public static final String LUCENE_CREATE_INDEX__FIELD_HELP = "fields on the region values on which are stored in the Lucene index.";
  public static final String LUCENE_CREATE_INDEX__ANALYZER = "analyzer";
  public static final String LUCENE_CREATE_INDEX__ANALYZER_HELP = "Type of the index. Valid values are: range, key and hash.";///

  public static final String LUCENE_CREATE_INDEX__GROUP = "group";
  public static final String LUCENE_CREATE_INDEX__GROUP__HELP = "Group of members in which the lucene index will be created.";

  public static final String CREATE_INDEX__INVALID__INDEX__TYPE__MESSAGE = "Invalid index type,value must be one of the following: range, key or hash.";
  public static final String CREATE_INDEX__SUCCESS__MSG = "Index successfully created with following details";
  public static final String CREATE_INDEX__FAILURE__MSG = "Failed to create index \"{0}\" due to following reasons";
  public static final String CREATE_INDEX__ERROR__MSG = "Exception \"{0}\" occured on following members";
  public static final String CREATE_INDEX__NAME__CONFLICT = "Index \"{0}\" already exists.  "
    + "Create failed due to duplicate name.";
  public static final String CREATE_INDEX__INDEX__EXISTS = "Index \"{0}\" already exists.  "
    + "Create failed due to duplicate definition.";
  public static final String CREATE_INDEX__INVALID__EXPRESSION = "Invalid indexed expression : \"{0}\"";
  public static final String CREATE_INDEX__INVALID__REGIONPATH = "Region not found : \"{0}\"";
  public static final String CREATE_INDEX__NAME__MSG = "Name       : {0}";
  public static final String CREATE_INDEX__EXPRESSION__MSG = "Expression : {0}";
  public static final String CREATE_INDEX__REGIONPATH__MSG = "RegionPath : {0}";
  public static final String CREATE_INDEX__TYPE__MSG = "Type       : {0}";
  public static final String CREATE_INDEX__MEMBER__MSG = "Members which contain the index";
  public static final String CREATE_INDEX__NUMBER__AND__MEMBER = "{0}. {1}";
  public static final String CREATE_INDEX__EXCEPTION__OCCURRED__ON = "Occurred on following members";
  public static final String CREATE_INDEX__INVALID__INDEX__NAME = "Invalid index name";
}
