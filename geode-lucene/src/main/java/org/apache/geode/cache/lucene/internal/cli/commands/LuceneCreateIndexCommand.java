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
package org.apache.geode.cache.lucene.internal.cli.commands;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneFunctionSerializable;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.exceptions.UserErrorException;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

@SuppressWarnings("unused")
public class LuceneCreateIndexCommand extends LuceneCommandBase {

  private static final LuceneCreateIndexFunction createIndexFunction =
      new LuceneCreateIndexFunction();

  /**
   * Backward-compatible overload for Java callers and unit tests without vector field parameters.
   */
  public ResultModel createIndex(final String indexName, final String regionPath,
      final String[] fields, final String[] analyzers, final String serializer)
      throws CommandResultException {
    return createIndex(indexName, regionPath, fields, analyzers, serializer, null, null, null,
        null);
  }

  /**
   * On the server, we also verify the resource operation permissions CLUSTER:WRITE:DISK
   */
  @ShellMethod(value = LuceneCliStrings.LUCENE_CREATE_INDEX__HELP,
      key = LuceneCliStrings.LUCENE_CREATE_INDEX)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  public ResultModel createIndex(
      @ShellOption(value = LuceneCliStrings.LUCENE__INDEX_NAME,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__NAME__HELP) final String indexName,

      @ShellOption(value = LuceneCliStrings.LUCENE__REGION_PATH,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__REGION_HELP) final String regionPath,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD_HELP) final String[] fields,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER_HELP) final String[] analyzers,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER_HELP) final String serializer,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_FIELD,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_FIELD_HELP) final String[] vectorFields,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_DIM,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_DIM_HELP) final String[] vectorDims,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_SIMILARITY,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_SIMILARITY_HELP) final String[] vectorSimilarities,

      @ShellOption(value = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_SQ8,
          defaultValue = ShellOption.NULL,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_SQ8_HELP) final String[] vectorSq8Flags)
      throws CommandResultException {

    // Every lucene index potentially writes to disk.
    authorize(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET);

    final InternalCache cache = (InternalCache) getCache();

    // trim fields for any leading trailing spaces.
    String[] trimmedFields = fields == null
        ? new String[0]
        : Arrays.stream(fields).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
    Map<String, VectorIndexMetadata> parsedVectorFields =
        parseVectorFields(vectorFields, vectorDims, vectorSimilarities, vectorSq8Flags);

    if (trimmedFields.length == 0 && parsedVectorFields.isEmpty()) {
      throw new UserErrorException(
          "At least one --field or --vector-field must be specified when creating a Lucene index.");
    }
    for (String textField : trimmedFields) {
      if (parsedVectorFields.containsKey(textField)) {
        throw new UserErrorException(
            "Field '" + textField + "' cannot be specified in both --field and --vector-field.");
      }
    }

    LuceneIndexInfo indexInfo = new LuceneIndexInfo(indexName, regionPath, trimmedFields, analyzers,
        serializer, parsedVectorFields);

    final ResultCollector<?, ?> rc =
        executeFunctionOnAllMembers(createIndexFunction, indexInfo);
    final List<CliFunctionResult> funcResults = (List<CliFunctionResult>) rc.getResult();
    final XmlEntity xmlEntity = funcResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
    final ResultModel result = new ResultModel();
    final TabularResultModel tabularResult = result.addTable("lucene-indexes");
    for (final CliFunctionResult cliFunctionResult : funcResults) {
      tabularResult.accumulate("Member", cliFunctionResult.getMemberIdOrName());

      if (cliFunctionResult.isSuccessful()) {
        tabularResult.accumulate("Status", "Successfully created lucene index");
      } else {
        tabularResult.accumulate("Status", "Failed: " + cliFunctionResult.getMessage());
      }
    }

    // if at least one member returns with successful deletion, we will need to update cc
    InternalConfigurationPersistenceService configurationPersistenceService =
        getConfigurationPersistenceService();
    if (xmlEntity != null) {
      if (configurationPersistenceService == null) {
        result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        configurationPersistenceService.addXmlEntity(xmlEntity, null);
      }
    }

    return result;
  }

  private Map<String, VectorIndexMetadata> parseVectorFields(String[] vectorFields,
      String[] vectorDims, String[] vectorSimilarities, String[] vectorSq8Flags) {
    if (vectorFields == null || vectorFields.length == 0) {
      if ((vectorDims != null && vectorDims.length > 0)
          || (vectorSimilarities != null && vectorSimilarities.length > 0)
          || (vectorSq8Flags != null && vectorSq8Flags.length > 0)) {
        throw new UserErrorException(
            "--vector-dim, --vector-similarity, and --vector-sq8 require --vector-field.");
      }
      return Collections.emptyMap();
    }

    if (vectorDims == null || vectorDims.length != vectorFields.length) {
      throw new UserErrorException(
          "--vector-dim must be specified with the same number of entries as --vector-field.");
    }

    if (vectorSimilarities != null && vectorSimilarities.length > 0
        && vectorSimilarities.length != 1 && vectorSimilarities.length != vectorFields.length) {
      throw new UserErrorException(
          "--vector-similarity must have either 1 value or match the length of --vector-field.");
    }

    if (vectorSq8Flags != null && vectorSq8Flags.length > 0
        && vectorSq8Flags.length != 1 && vectorSq8Flags.length != vectorFields.length) {
      throw new UserErrorException(
          "--vector-sq8 must have either 1 value or match the length of --vector-field.");
    }

    Map<String, VectorIndexMetadata> result = new LinkedHashMap<>();
    for (int i = 0; i < vectorFields.length; i++) {
      String fieldName = vectorFields[i] == null ? "" : vectorFields[i].trim();
      int dim;
      try {
        dim = Integer.parseInt(vectorDims[i].trim());
      } catch (NumberFormatException | NullPointerException e) {
        throw new UserErrorException(
            "Invalid --vector-dim value '" + vectorDims[i] + "' for vector field '" + fieldName
                + "': expected a positive integer.");
      }

      VectorSimilarityFunction similarity = VectorSimilarityFunction.COSINE;
      if (vectorSimilarities != null && vectorSimilarities.length > 0) {
        String rawSim =
            vectorSimilarities.length == 1 ? vectorSimilarities[0] : vectorSimilarities[i];
        try {
          similarity = VectorSimilarityFunction.valueOf(rawSim.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
          throw new UserErrorException("Invalid --vector-similarity value '" + rawSim
              + "'. Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN.");
        }
      }

      boolean sq8 = false;
      if (vectorSq8Flags != null && vectorSq8Flags.length > 0) {
        String rawSq8 = vectorSq8Flags.length == 1 ? vectorSq8Flags[0] : vectorSq8Flags[i];
        sq8 = parseStrictBoolean(rawSq8, LuceneCliStrings.LUCENE_CREATE_INDEX__VECTOR_SQ8);
      }

      try {
        VectorIndexMetadata metadata = new VectorIndexMetadata(fieldName, dim, similarity, sq8);
        if (result.containsKey(metadata.getFieldName())) {
          throw new UserErrorException(
              "Duplicate --vector-field name: '" + metadata.getFieldName() + "'.");
        }
        result.put(metadata.getFieldName(), metadata);
      } catch (IllegalArgumentException e) {
        throw new UserErrorException(e.getMessage(), e);
      }
    }
    return result;
  }

  private boolean parseStrictBoolean(String raw, String optionName) {
    if (raw == null) {
      throw new UserErrorException(
          "Invalid --" + optionName + " value: null (expected true/false)");
    }
    String trimmed = raw.trim().toLowerCase(Locale.ROOT);
    if ("true".equals(trimmed)) {
      return true;
    }
    if ("false".equals(trimmed)) {
      return false;
    }
    throw new UserErrorException(
        "Invalid --" + optionName + " value '" + raw + "' (expected true or false).");
  }

  protected ResultCollector<?, ?> executeFunctionOnAllMembers(Function function,
      final LuceneFunctionSerializable functionArguments)
      throws IllegalArgumentException {
    Set<DistributedMember> targetMembers = getAllNormalMembers();
    return executeFunction(function, functionArguments, targetMembers);
  }

  @ShellMethodAvailability(LuceneCliStrings.LUCENE_CREATE_INDEX)
  public boolean indexCommandsAvailable() {
    return super.indexCommandsAvailable();
  }
}
