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

import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.xml.sax.SAXException;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.functions.GetRegionNamesFunction;
import org.apache.geode.management.internal.configuration.functions.RecreateCacheFunction;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * Commands for the cluster configuration
 */
@SuppressWarnings("unused")
public class ImportClusterConfigurationCommand extends GfshCommand {
  public static final Logger logger = LogService.getLogger();
  public static final String XML_FILE = "xml-file";
  public static final String ACTION = "action";
  public static final String ACTION_HELP =
      "What to do with the running servers if any. APPLY would try to apply the configuration to the empty servers. STAGE would leave the running servers alone.";

  public enum Action {
    APPLY, STAGE
  }

  @CliCommand(value = {CliStrings.IMPORT_SHARED_CONFIG},
      help = CliStrings.IMPORT_SHARED_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ImportClusterConfigurationCommand$ImportInterceptor",
      isFileUploaded = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public ResultModel importSharedConfig(
      @CliOption(key = CliStrings.GROUP,
          specifiedDefaultValue = ConfigurationPersistenceService.CLUSTER_CONFIG,
          unspecifiedDefaultValue = ConfigurationPersistenceService.CLUSTER_CONFIG) String group,
      @CliOption(key = XML_FILE) String xmlFile,
      @CliOption(key = ACTION, help = ACTION_HELP, unspecifiedDefaultValue = "APPLY") Action action,
      @CliOption(key = {CliStrings.IMPORT_SHARED_CONFIG__ZIP},
          help = CliStrings.IMPORT_SHARED_CONFIG__ZIP__HELP) String zip)
      throws IOException, TransformerException, SAXException, ParserConfigurationException {

    if (!isSharedConfigurationRunning()) {
      return ResultModel.createError("Cluster configuration service is not running.");
    }

    InternalConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    Set<DistributedMember> servers = findMembers(group);
    File file = getUploadedFile();

    ResultModel result = new ResultModel();
    InfoResultModel infoSection = result.addInfo(ResultModel.INFO_SECTION);
    ccService.lockSharedConfiguration();
    try {
      if (action == Action.APPLY && servers.size() > 0) {
        // make sure the servers are vanilla servers, users hasn't done anything on them.
        // server might belong to multiple group, so we can't just check one group's xml is null,
        // has to make sure
        // all group's xml are null
        if (ccService.hasXmlConfiguration()) {
          return ResultModel.createError("Can not configure servers that are already configured.");
        }
        // if no existing cluster configuration, to be safe, further check to see if running
        // servers has regions already defined
        Set<String> regionsOnServers = servers.stream().map(this::getRegionNamesOnServer)
            .flatMap(Collection::stream).collect(toSet());

        if (!regionsOnServers.isEmpty()) {
          return ResultModel.createError("Can not configure servers with existing regions: "
              + String.join(",", regionsOnServers));
        }
      }

      // backup the old config
      backupTheOldConfig(ccService);

      if (zip != null) {
        Path tempDir = Files.createTempDirectory("config");
        ZipUtils.unzip(file.getAbsolutePath(), tempDir.toAbsolutePath().toString());
        // load it from the disk
        ccService.loadSharedConfigurationFromDir(tempDir.toFile());
        FileUtils.deleteQuietly(tempDir.toFile());
        infoSection.addLine("Cluster configuration successfully imported.");
      } else {
        // update the xml in the cluster configuration service
        Configuration configuration = ccService.getConfiguration(group);
        if (configuration == null) {
          configuration = new Configuration(group);
        }
        configuration.setCacheXmlFile(file);
        ccService.setConfiguration(group, configuration);
        logger.info(
            configuration.getConfigName() + "xml content: \n" + configuration.getCacheXmlContent());
        infoSection.addLine(
            "Successfully set the '" + group + "' configuration to the content of " + xmlFile);
      }
    } finally {
      FileUtils.deleteQuietly(file);
      ccService.unlockSharedConfiguration();
    }

    if (servers.size() > 0) {
      if (action == Action.APPLY) {
        List<CliFunctionResult> functionResults =
            executeAndGetFunctionResult(new RecreateCacheFunction(), null, servers);
        TabularResultModel tableSection =
            result.addTableAndSetStatus(ResultModel.MEMBER_STATUS_SECTION, functionResults, false,
                true);
        tableSection.setHeader("Configure the servers in '" + group + "' group: ");
      } else {
        infoSection.addLine("Existing servers are not affected with this configuration change.");
      }
    }
    return result;
  }

  void backupTheOldConfig(InternalConfigurationPersistenceService ccService) throws IOException {
    String backupDir = "cluster_config_" + new SimpleDateFormat("yyyyMMddhhmm").format(new Date())
        + '.' + System.nanoTime();
    File backDirFile = ccService.getClusterConfigDirPath().getParent().resolve(backupDir).toFile();
    for (Configuration config : ccService.getEntireConfiguration().values()) {
      ccService.writeConfigToFile(config, backDirFile);
    }
  }

  File getUploadedFile() {
    List<String> filePathFromShell = CommandExecutionContext.getFilePathFromShell();
    return new File(filePathFromShell.get(0));
  }

  Set<DistributedMember> findMembers(String group) {
    Set<DistributedMember> serversInGroup;
    if (ConfigurationPersistenceService.CLUSTER_CONFIG.equals(group)) {
      serversInGroup = getAllNormalMembers();
    } else {
      serversInGroup = findMembers(new String[] {group}, null);
    }
    return serversInGroup;
  }

  private Set<String> getRegionNamesOnServer(DistributedMember server) {
    ResultCollector<?, ?> rc = executeFunction(new GetRegionNamesFunction(), null, server);
    @SuppressWarnings("unchecked")
    List<Set<String>> results = (List<Set<String>>) rc.getResult();

    return results.get(0);
  }

  public static class ImportInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String zip = parseResult.getParamValueAsString(CliStrings.IMPORT_SHARED_CONFIG__ZIP);
      String xmlFile = parseResult.getParamValueAsString(XML_FILE);
      String group = parseResult.getParamValueAsString(CliStrings.GROUP);

      if (group != null && group.contains(",")) {
        return ResultModel.createError("Only a single group name is supported.");
      }

      if (zip == null && xmlFile == null) {
        return ResultModel.createError("Either a zip file or a xml file is required.");
      }

      if (zip != null && xmlFile != null) {
        return ResultModel.createError("Zip file and xml File can't both be specified.");
      }


      if (zip != null) {
        if (!group.equals(ConfigurationPersistenceService.CLUSTER_CONFIG)) {
          return ResultModel.createError("zip file can not be imported with a specific group.");
        }

        if (!zip.endsWith(CliStrings.ZIP_FILE_EXTENSION)) {
          return ResultModel.createError("Invalid file type. The file extension must be .zip");
        }
      }

      if (xmlFile != null) {
        if (!xmlFile.endsWith(".xml")) {
          return ResultModel.createError("Invalid file type. The file extension must be .xml.");
        }
      }

      String file = (zip != null) ? zip : xmlFile;
      File importedFile = new File(file).getAbsoluteFile();
      if (!importedFile.exists()) {
        return ResultModel.createError("'" + file + "' not found.");
      }

      String message = "This command will replace the existing cluster configuration, if any, "
          + "The old configuration will be backed up in the working directory.\n\n" + "Continue? ";

      if (readYesNo(message, Response.YES) == Response.NO) {
        return ResultModel.createError("Aborted import of " + file + ".");
      }

      Action action = (Action) parseResult.getParamValue(ACTION);
      if (action == Action.STAGE) {
        message =
            "The configuration you are trying to import should NOT have any conflict with the configuration"
                + "of existing running servers if any, otherwise you may not be able to start new servers. "
                + "\nIt is also expected that you would restart the servers with the old configuration after new servers have come up."
                + "\n\nContinue? ";
        if (readYesNo(message, Response.YES) == Response.NO) {
          return ResultModel.createError("Aborted import of " + xmlFile + ".");
        }
      }

      ResultModel result = new ResultModel();
      result.addFile(importedFile, FileResultModel.FILE_TYPE_FILE);
      return result;
    }
  }

}
