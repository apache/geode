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

package org.apache.geode.management.internal.cli.commands;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.cache.configuration.StringType;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.security.ResourcePermission;

public class ConfigurePDXCommand extends InternalGfshCommand {

  protected ReflectionBasedAutoSerializer createReflectionBasedAutoSerializer(
      boolean checkPortability, String[] patterns) {
    return new ReflectionBasedAutoSerializer(checkPortability, patterns);
  }

  @CliCommand(value = CliStrings.CONFIGURE_PDX, help = CliStrings.CONFIGURE_PDX__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION,
      interceptor = "org.apache.geode.management.internal.cli.commands.ConfigurePDXCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result configurePDX(
      @CliOption(key = CliStrings.CONFIGURE_PDX__READ__SERIALIZED,
          unspecifiedDefaultValue = "false",
          help = CliStrings.CONFIGURE_PDX__READ__SERIALIZED__HELP) Boolean readSerialized,
      @CliOption(key = CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS,
          unspecifiedDefaultValue = "false",
          help = CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS__HELP) Boolean ignoreUnreadFields,
      @CliOption(key = CliStrings.CONFIGURE_PDX__DISKSTORE, specifiedDefaultValue = "DEFAULT",
          help = CliStrings.CONFIGURE_PDX__DISKSTORE__HELP) String diskStore,
      @CliOption(key = CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES,
          help = CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES__HELP) String[] nonPortableClassesPatterns,
      @CliOption(key = CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES,
          help = CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES__HELP) String[] portableClassesPatterns) {

    Result result;
    ReflectionBasedAutoSerializer autoSerializer = null;

    if (getConfigurationPersistenceService() == null) {
      return ResultBuilder
          .createUserErrorResult("Configure pdx failed because cluster configuration is disabled.");
    }

    InfoResultData infoResultData = ResultBuilder.createInfoResultData();

    if (!getAllNormalMembers().isEmpty()) {
      infoResultData.addLine(CliStrings.CONFIGURE_PDX__NORMAL__MEMBERS__WARNING);
    }

    // Set persistent and the disk-store
    if (diskStore != null) {
      infoResultData.addLine(CliStrings.CONFIGURE_PDX__PERSISTENT + " = true");
      infoResultData.addLine(CliStrings.CONFIGURE_PDX__DISKSTORE + " = " + diskStore);
    } else {
      infoResultData.addLine(CliStrings.CONFIGURE_PDX__PERSISTENT + " = false");
    }

    infoResultData.addLine(CliStrings.CONFIGURE_PDX__READ__SERIALIZED + " = " + readSerialized);
    infoResultData
        .addLine(CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS + " = " + ignoreUnreadFields);

    // Auto Serializer Configuration
    if (portableClassesPatterns != null) {
      autoSerializer = createReflectionBasedAutoSerializer(true, portableClassesPatterns);
      infoResultData.addLine("PDX Serializer = " + autoSerializer.getClass().getName());
      infoResultData.addLine("Portable Classes = " + Arrays.toString(portableClassesPatterns));
    }

    if (nonPortableClassesPatterns != null) {
      autoSerializer = createReflectionBasedAutoSerializer(false, nonPortableClassesPatterns);
      infoResultData.addLine("PDX Serializer = " + autoSerializer.getClass().getName());
      infoResultData
          .addLine("Non Portable Classes = " + Arrays.toString(nonPortableClassesPatterns));
    }

    result = ResultBuilder.buildResult(infoResultData);
    ReflectionBasedAutoSerializer finalAutoSerializer = autoSerializer;
    getConfigurationPersistenceService()
        .updateCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG, config -> {
          if (config.getPdx() == null) {
            config.setPdx(new PdxType());
          }
          config.getPdx().setReadSerialized(readSerialized);
          config.getPdx().setIgnoreUnreadFields(ignoreUnreadFields);
          config.getPdx().setDiskStoreName(diskStore);
          config.getPdx().setPersistent(diskStore != null);

          if (portableClassesPatterns != null || nonPortableClassesPatterns != null) {
            PdxType.PdxSerializer pdxSerializer = new PdxType.PdxSerializer();
            pdxSerializer.setClassName(ReflectionBasedAutoSerializer.class.getName());

            List<ParameterType> parameters =
                finalAutoSerializer.getConfig().entrySet().stream().map(entry -> {
                  ParameterType parameterType = new ParameterType();
                  parameterType.setName((String) entry.getKey());
                  parameterType.setString(new StringType((String) entry.getValue()));
                  return parameterType;
                }).collect(Collectors.toList());
            pdxSerializer.getParameter().addAll(parameters);

            config.getPdx().setPdxSerializer(pdxSerializer);
          }
          return config;
        });
    return result;
  }

  /**
   * Interceptor to validate command parameters.
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String[] portableClassesPatterns = (String[]) parseResult
          .getParamValue(CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES);
      String[] nonPortableClassesPatterns =
          (String[]) parseResult.getParamValue(CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES);

      if ((nonPortableClassesPatterns != null && nonPortableClassesPatterns.length > 0)
          && (portableClassesPatterns != null && portableClassesPatterns.length > 0)) {
        return ResultBuilder.createUserErrorResult(CliStrings.CONFIGURE_PDX__ERROR__MESSAGE);
      }

      return ResultBuilder.createInfoResult("");
    }
  }
}
