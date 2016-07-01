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
  public static final String LUCENE_LIST_INDEX = "list lucene index";
  public static final String LUCENE_LIST_INDEX__HELP = "Display the list of lucene indexes created for all members.";
  public static final String LUCENE_LIST_INDEX__ERROR_MESSAGE = "An error occurred while collecting all lucene index information across the Geode cluster: %1$s";
  public static final String LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE = "No lucene indexes Found";

  public static final String LUCENE_CREATE_INDEX = "create lucene index";
  public static final String LUCENE_CREATE_INDEX__HELP = "Create a lucene index that can be used to execute queries.";
  public static final String LUCENE_CREATE_INDEX__NAME = "name";
  public static final String LUCENE_CREATE_INDEX__NAME__HELP = "Name of the lucene index to create.";
  public static final String LUCENE_CREATE_INDEX__REGION = "region";
  public static final String LUCENE_CREATE_INDEX__REGION_HELP = "Name/Path of the region where the lucene index is created on.";
  public static final String LUCENE_CREATE_INDEX__FIELD = "field";
  public static final String LUCENE_CREATE_INDEX__FIELD_HELP = "fields on the region values which are stored in the lucene index.";
  public static final String LUCENE_CREATE_INDEX__ANALYZER = "analyzer";
  public static final String LUCENE_CREATE_INDEX__ANALYZER_HELP = "Type of the analyzer for each field.";
  public static final String LUCENE_CREATE_INDEX__GROUP = "group";
  public static final String LUCENE_CREATE_INDEX__GROUP__HELP = "Group of members in which the lucene index will be created.";

  public static final String CREATE_INDEX__SUCCESS__MSG = "Index successfully created with following details";
  public static final String CREATE_INDEX__FAILURE__MSG = "Failed to create index \"{0}\" due to following reasons";
  public static final String CREATE_INDEX__NAME__MSG = "Name       : {0}";
  public static final String CREATE_INDEX__REGIONPATH__MSG = "RegionPath : {0}";
  public static final String CREATE_INDEX__MEMBER__MSG = "Members which contain the index";
  public static final String CREATE_INDEX__NUMBER__AND__MEMBER = "{0}. {1}";
  public static final String CREATE_INDEX__EXCEPTION__OCCURRED__ON = "Occurred on following members";
}
