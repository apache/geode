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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

import joptsimple.internal.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

;

/**
 * Commands for the cluster configuration
 */
@SuppressWarnings("unused")
public class ExportClusterConfigurationCommand extends InternalGfshCommand {
  private static Logger logger = LogService.getLogger();
  public static final String XML_FILE = "xml-file";

  @CliCommand(value = {CliStrings.EXPORT_SHARED_CONFIG},
      help = CliStrings.EXPORT_SHARED_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportClusterConfigurationCommand$ExportInterceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public ResultModel exportSharedConfig(
      @CliOption(key = GROUP,
          specifiedDefaultValue = ConfigurationPersistenceService.CLUSTER_CONFIG,
          unspecifiedDefaultValue = ConfigurationPersistenceService.CLUSTER_CONFIG) String group,
      @CliOption(key = XML_FILE) String xmlFile,
      @CliOption(key = CliStrings.EXPORT_SHARED_CONFIG__FILE,
          help = CliStrings.EXPORT_SHARED_CONFIG__FILE__HELP) String zipFileName)
      throws IOException {

    if (!isSharedConfigurationRunning()) {
      return ResultModel.createError("Cluster configuration service is not running.");
    }

    ResultModel result = new ResultModel();
    if (zipFileName != null) {
      Path tempDir = Files.createTempDirectory("temp");
      Path exportedDir = tempDir.resolve("cluster_config");
      Path zipFile = tempDir.resolve(FilenameUtils.getName(zipFileName));
      InternalConfigurationPersistenceService sc = getConfigurationPersistenceService();
      try {
        for (Configuration config : sc.getEntireConfiguration().values()) {
          sc.writeConfigToFile(config, exportedDir.toFile());
        }
        ZipUtils.zipDirectory(exportedDir, zipFile);
        result.addFile(zipFile.toFile(), FileResultModel.FILE_TYPE_BINARY);
      } catch (Exception e) {
        logger.error("unable to export configuration.", e);
      } finally {
        FileUtils.deleteQuietly(tempDir.toFile());
      }
    } else {
      Configuration configuration = getConfigurationPersistenceService().getConfiguration(group);
      if (configuration == null) {
        return ResultModel.createError("No cluster configuration for '" + group + "'.");
      }

      String cacheXmlContent = configuration.getCacheXmlContent();
      if (cacheXmlContent != null) {
        InfoResultModel xmlSection = result.addInfo("xml");
        xmlSection.setHeader(configuration.getCacheXmlFileName() + ": ");
        xmlSection.addLine(cacheXmlContent);
      }

      Properties gemfireProperties = configuration.getGemfireProperties();
      if (gemfireProperties.size() > 0) {
        DataResultModel propertySection = result.addData("properties");
        propertySection.setHeader("Properties: ");
        propertySection.addData(gemfireProperties);
      }

      Set<String> jarNames = configuration.getJarNames();
      if (jarNames.size() > 0) {
        InfoResultModel jarSection = result.addInfo("jars");
        jarSection.setHeader("Jars: ");
        jarSection.addLine(Strings.join(jarNames, ", "));
      }
    }

    return result;
  }

  /**
   * Interceptor used by gfsh to intercept execution of export shared config command at "shell".
   */
  public static class ExportInterceptor extends AbstractCliAroundInterceptor {
    private String saveDirString;
    private static final Logger logger = LogService.getLogger();

    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String zip = parseResult.getParamValueAsString(CliStrings.EXPORT_SHARED_CONFIG__FILE);
      String xmlFile = parseResult.getParamValueAsString(XML_FILE);
      String group = parseResult.getParamValueAsString(GROUP);

      if (group != null && group.contains(",")) {
        return ResultModel.createError("Only a single group name is supported.");
      }

      if (zip != null && xmlFile != null) {
        return ResultModel.createError("Zip file and xml File can't both be specified.");
      }

      if (zip != null && !group.equals(ConfigurationPersistenceService.CLUSTER_CONFIG)) {
        return ResultModel.createError("zip file can not be exported with a specific group.");
      }

      if (zip != null && !zip.endsWith(".zip")) {
        return ResultModel
            .createError(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".zip"));
      }

      String exportedFile = (zip != null) ? zip : xmlFile;
      if (exportedFile != null) {
        // make sure the file does not exist so that we don't overwrite some existing file
        File file = new File(exportedFile).getAbsoluteFile();
        if (file.exists()) {
          String message = file.getAbsolutePath() + " already exists. Overwrite it? ";
          if (readYesNo(message, Response.YES) == Response.NO) {
            return ResultModel.createError("Aborted. " + exportedFile + "already exists.");
          }
        }
      }

      return ResultModel.createInfo("");
    }

    @Override
    public ResultModel postExecution(GfshParseResult parseResult, ResultModel result, Path tempFile)
        throws IOException {
      if (result.getStatus() == Result.Status.ERROR) {
        return result;
      }
      String xmlFile = parseResult.getParamValueAsString(XML_FILE);
      String zipFile = parseResult.getParamValueAsString(CliStrings.EXPORT_SHARED_CONFIG__FILE);
      String group = parseResult.getParamValueAsString(GROUP);
      // save the result to the file
      if (xmlFile != null) {
        InfoResultModel xmlSection = result.getInfoSection("xml");
        if (xmlSection == null) {
          InfoResultModel info = result.addInfo("info");
          info.addLine(String.format("xml content is empty. %s is not created.", xmlFile));
        } else {
          File file = new File(xmlFile).getAbsoluteFile();
          FileUtils.write(file, Strings.join(xmlSection.getContent(), System.lineSeparator()),
              Charset.defaultCharset());
          xmlSection.removeLine(0);
          xmlSection.addLine("xml content exported to " + file.getAbsolutePath());
        }
      } else if (zipFile != null) {
        // delete the existing file since at this point, user is OK to replace the old zip.
        File file = new File(zipFile).getAbsoluteFile();
        if (file.exists()) {
          FileUtils.deleteQuietly(file);
        }
        result.saveFileTo(file.getParentFile());
      }
      return result;
    }
  }
}
