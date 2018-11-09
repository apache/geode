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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.security.ResourcePermission;

public class ConfigurePDXCommand extends SingleGfshCommand {

  protected ReflectionBasedAutoSerializer createReflectionBasedAutoSerializer(
      boolean checkPortability, String[] patterns) {
    return new ReflectionBasedAutoSerializer(checkPortability, patterns);
  }

  @CliCommand(value = CliStrings.CONFIGURE_PDX, help = CliStrings.CONFIGURE_PDX__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION,
      interceptor = "org.apache.geode.management.internal.cli.commands.ConfigurePDXCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel configurePDX(
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

    if (getConfigurationPersistenceService() == null) {
      return ResultModel
          .createError("Configure pdx failed because cluster configuration is disabled.");
    }

    ResultModel result = new ResultModel();
    InfoResultModel infoSection = result.addInfo();

    if (!getAllNormalMembers().isEmpty()) {
      infoSection.addLine(CliStrings.CONFIGURE_PDX__NORMAL__MEMBERS__WARNING);
    }

    PdxType pdxType = new PdxType();
    pdxType.setIgnoreUnreadFields(ignoreUnreadFields);
    pdxType.setReadSerialized(readSerialized);
    infoSection.addLine(CliStrings.CONFIGURE_PDX__READ__SERIALIZED + " = " + readSerialized);
    infoSection
        .addLine(CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS + " = " + ignoreUnreadFields);

    pdxType.setDiskStoreName(diskStore);
    pdxType.setPersistent(diskStore != null);

    if (diskStore != null) {
      infoSection.addLine(CliStrings.CONFIGURE_PDX__PERSISTENT + " = true");
      infoSection.addLine(CliStrings.CONFIGURE_PDX__DISKSTORE + " = " + diskStore);
    } else {
      infoSection.addLine(CliStrings.CONFIGURE_PDX__PERSISTENT + " = false");
    }

    ReflectionBasedAutoSerializer autoSerializer = null;
    if (portableClassesPatterns != null) {
      autoSerializer = createReflectionBasedAutoSerializer(true, portableClassesPatterns);
      infoSection.addLine("PDX Serializer = " + autoSerializer.getClass().getName());
      infoSection.addLine("Portable Classes = " + Arrays.toString(portableClassesPatterns));
    } else if (nonPortableClassesPatterns != null) {
      autoSerializer = createReflectionBasedAutoSerializer(false, nonPortableClassesPatterns);
      infoSection.addLine("PDX Serializer = " + autoSerializer.getClass().getName());
      infoSection.addLine("Non Portable Classes = " + Arrays.toString(nonPortableClassesPatterns));
    }
    if (autoSerializer != null) {
      pdxType.setPdxSerializer(new DeclarableType(ReflectionBasedAutoSerializer.class.getName(),
          autoSerializer.getConfig()));
    }

    result.setConfigObject(pdxType);
    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    config.setPdx((PdxType) configObject);
    return true;
  }

  /**
   * Interceptor to validate command parameters.
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {

    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String[] portableClassesPatterns = (String[]) parseResult
          .getParamValue(CliStrings.CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES);
      String[] nonPortableClassesPatterns =
          (String[]) parseResult.getParamValue(CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES);

      if ((nonPortableClassesPatterns != null && nonPortableClassesPatterns.length > 0)
          && (portableClassesPatterns != null && portableClassesPatterns.length > 0)) {
        return ResultModel.createError(CliStrings.CONFIGURE_PDX__ERROR__MESSAGE);
      }
      return ResultModel.createInfo("");
    }
  }
}
