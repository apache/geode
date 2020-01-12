/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.cli;

public class LuceneCliStrings {
  // Common parameters/options
  public static final String LUCENE__INDEX_NAME = "name";
  public static final String LUCENE__REGION_PATH = "region";

  // List lucene index commands
  public static final String LUCENE_LIST_INDEX = "list lucene indexes";
  public static final String LUCENE_LIST_INDEX__HELP =
      "Display the list of lucene indexes created for all members.";
  public static final String LUCENE_LIST_INDEX__ERROR_MESSAGE =
      "An error occurred while collecting all lucene index information across the Geode cluster: %1$s";
  public static final String LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE =
      "No lucene indexes found";
  public static final String LUCENE_LIST_INDEX__STATS = "with-stats";
  public static final String LUCENE_LIST_INDEX__STATS__HELP = "Display lucene index stats";

  // Create lucene index commands
  public static final String LUCENE_CREATE_INDEX = "create lucene index";
  public static final String LUCENE_CREATE_INDEX__HELP =
      "Create a lucene index that can be used to execute queries.";
  public static final String LUCENE_CREATE_INDEX__NAME__HELP =
      "Name of the lucene index to create.";
  public static final String LUCENE_CREATE_INDEX__REGION_HELP =
      "Name/Path of the region on which to create the lucene index.";
  public static final String LUCENE_CREATE_INDEX__FIELD = "field";
  public static final String LUCENE_CREATE_INDEX__FIELD_HELP =
      "Fields on the region values which are stored in the lucene index.\nUse __REGION_VALUE_FIELD if the entire region value should be indexed.\n__REGION_VALUE_FIELD is valid only if the region values are strings or numbers.";
  public static final String LUCENE_CREATE_INDEX__ANALYZER = "analyzer";
  public static final String LUCENE_CREATE_INDEX__ANALYZER_HELP =
      "Type of the analyzer for each field.\nUse the case sensitive keyword DEFAULT or leave an analyzer blank to use the default standard analyzer.";
  public static final String LUCENE_CREATE_INDEX__SERIALIZER = "serializer";
  public static final String LUCENE_CREATE_INDEX__SERIALIZER_HELP =
      "Fully qualified class name of the LuceneSerializer to use with this index.\n";

  // Describe lucene index commands
  public static final String LUCENE_DESCRIBE_INDEX = "describe lucene index";
  public static final String LUCENE_DESCRIBE_INDEX__HELP =
      "Display the description of lucene indexes created for all members.";
  public static final String LUCENE_DESCRIBE_INDEX__ERROR_MESSAGE =
      "An error occurred while collecting lucene index information across the Geode cluster: %1$s";
  public static final String LUCENE_DESCRIBE_INDEX__NAME__HELP =
      "Name of the lucene index to describe.";
  public static final String LUCENE_DESCRIBE_INDEX__REGION_HELP =
      "Name/Path of the region defining the lucene index to be described.";

  // Search lucene index commands
  public static final String LUCENE_SEARCH_INDEX = "search lucene";
  public static final String LUCENE_SEARCH_INDEX__HELP = "Search lucene index";
  public static final String LUCENE_SEARCH_INDEX__ERROR_MESSAGE =
      "An error occurred while searching lucene index across the Geode cluster: %1$s";
  public static final String LUCENE_SEARCH_INDEX__NAME__HELP =
      "Name of the lucene index to search.";
  public static final String LUCENE_SEARCH_INDEX__REGION_HELP =
      "Name/Path of the region defining the lucene index to be searched.";
  public static final String LUCENE_SEARCH_INDEX__QUERY_STRING = "queryString";
  public static final String LUCENE_SEARCH_INDEX__QUERY_STRINGS = "queryStrings";
  public static final String LUCENE_SEARCH_INDEX__LIMIT = "limit";
  public static final String LUCENE_SEARCH_INDEX__LIMIT__HELP = "Number of search results needed";
  public static final String LUCENE_SEARCH_INDEX__QUERY_STRING__HELP =
      "Query string to search the lucene index";
  public static final String LUCENE_SEARCH_INDEX__DEFAULT_FIELD = "defaultField";
  public static final String LUCENE_SEARCH_INDEX__DEFAULT_FIELD__HELP =
      "Default field to search in";
  public static final String LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE = "No results";
  public static final String LUCENE_SEARCH_INDEX__KEYSONLY = "keys-only";
  public static final String LUCENE_SEARCH_INDEX__KEYSONLY__HELP =
      "Return only keys of search results.";

  // Destroy lucene index command
  public static final String LUCENE_DESTROY_INDEX = "destroy lucene index";
  public static final String LUCENE_DESTROY_INDEX__HELP = "Destroy the lucene index.";
  public static final String LUCENE_DESTROY_INDEX__EXCEPTION_MESSAGE =
      "An unexpected exception occurred while destroying lucene index:";
  public static final String LUCENE_DESTROY_INDEX__NAME__HELP =
      "Name of the lucene index to destroy.";
  public static final String LUCENE_DESTROY_INDEX__REGION_HELP =
      "Name of the region defining the lucene index to be destroyed.";
  public static final String LUCENE_DESTROY_INDEX__MSG__REGION_CANNOT_BE_EMPTY =
      "Region cannot be empty.";
  public static final String LUCENE_DESTROY_INDEX__MSG__INDEX_CANNOT_BE_EMPTY =
      "Index cannot be empty.";
  public static final String LUCENE_DESTROY_INDEX__MSG__COULDNOT_FIND_MEMBERS_FOR_REGION_0 =
      "Could not find any members defining region {0}.";
  public static final String LUCENE_DESTROY_INDEX__MSG__COULD_NOT_FIND__MEMBERS_GREATER_THAN_VERSION_0 =
      "Could not find any members greater than or equal to version {0}.";
  public static final String LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0 =
      "Successfully destroyed all lucene indexes from region {0}";
  public static final String LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1 =
      "Successfully destroyed lucene index {0} from region {1}";
}
