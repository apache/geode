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
package org.apache.geode.cache.lucene.internal.cli;

public class LuceneCliStrings {
  //Common parameters/options
  public static final String LUCENE__INDEX_NAME = "name";
  public static final String LUCENE__REGION_PATH = "region";

  //List lucene index commands
  public static final String LUCENE_LIST_INDEX = "list lucene indexes";
  public static final String LUCENE_LIST_INDEX__HELP = "Display the list of lucene indexes created for all members.";
  public static final String LUCENE_LIST_INDEX__ERROR_MESSAGE = "An error occurred while collecting all lucene index information across the Geode cluster: %1$s";
  public static final String LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE = "No lucene indexes found";
  public static final String LUCENE_LIST_INDEX__STATS = "with-stats";
  public static final String LUCENE_LIST_INDEX__STATS__HELP = "Display lucene index stats";

  //Create lucene index commands 
  public static final String LUCENE_CREATE_INDEX = "create lucene index";
  public static final String LUCENE_CREATE_INDEX__HELP = "Create a lucene index that can be used to execute queries.";
  public static final String LUCENE_CREATE_INDEX__NAME__HELP = "Name of the lucene index to create.";
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
  
  //Describe lucene index commands
  public static final String LUCENE_DESCRIBE_INDEX = "describe lucene index";
  public static final String LUCENE_DESCRIBE_INDEX__HELP = "Display the describe of lucene indexes created for all members.";
  public static final String LUCENE_DESCRIBE_INDEX__ERROR_MESSAGE = "An error occurred while collecting lucene index information across the Geode cluster: %1$s";
  public static final String LUCENE_DESCRIBE_INDEX__NAME__HELP = "Name of the lucene index to describe.";
  public static final String LUCENE_DESCRIBE_INDEX__REGION_HELP = "Name/Path of the region where the lucene index to be described exists.";

  
  //Search lucene index commands
  public static final String LUCENE_SEARCH_INDEX = "search lucene";
  public static final String LUCENE_SEARCH_INDEX__HELP = "Search lucene index";
  public static final String LUCENE_SEARCH_INDEX__ERROR_MESSAGE = "An error occurred while searching lucene index across the Geode cluster: %1$s";
  public static final String LUCENE_SEARCH_INDEX__NAME__HELP = "Name of the lucene index to search.";
  public static final String LUCENE_SEARCH_INDEX__REGION_HELP = "Name/Path of the region where the lucene index exists.";
  public static final String LUCENE_SEARCH_INDEX__QUERY_STRING="queryStrings";
  public static final String LUCENE_SEARCH_INDEX__LIMIT="limit";
  public static final String LUCENE_SEARCH_INDEX__LIMIT__HELP="Number of search results needed";
  public static final String LUCENE_SEARCH_INDEX__QUERY_STRING__HELP="Query string to search the lucene index";
  public static final String LUCENE_SEARCH_INDEX__DEFAULT_FIELD="defaultField";
  public static final String LUCENE_SEARCH_INDEX__DEFAULT_FIELD__HELP="Default field to search in";
  public static final String LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE="No results";
  public static final String LUCENE_SEARCH_INDEX__PAGE_SIZE="pageSize";
  public static final String LUCENE_SEARCH_INDEX__PAGE_SIZE__HELP="Number of results to be returned in a page";
  public static final String LUCENE_SEARCH_INDEX__KEYSONLY="keys-only";
  public static final String LUCENE_SEARCH_INDEX__KEYSONLY__HELP="Return only keys of search results.";

}
