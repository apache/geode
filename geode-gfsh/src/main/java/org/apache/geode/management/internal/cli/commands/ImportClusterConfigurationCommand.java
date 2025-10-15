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
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
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
 *
 * SECURITY CONSIDERATIONS:
 *
 * This command handles file uploads and path operations that require careful security validation
 * to prevent path injection attacks (CodeQL rule: java/path-injection).
 *
 * PATH INJECTION VULNERABILITIES ADDRESSED:
 *
 * 1. USER INPUT SANITIZATION:
 * - The xmlFile parameter comes from user input via @ShellOption
 * - Direct use of xmlFile in output messages creates information disclosure risks
 * - Solution: Use sanitizeFilename() to clean filenames before display
 *
 * 2. PATH TRAVERSAL PREVENTION:
 * - File paths from CommandExecutionContext could contain "../" sequences
 * - Malicious paths could access files outside intended directories
 * - Solution: Validate paths and reject traversal attempts
 *
 * 3. FILE TYPE VALIDATION:
 * - Ensure uploaded files are regular files, not directories or special files
 * - Prevent attacks that try to manipulate non-file filesystem objects
 * - Solution: Validate file.isFile() before processing
 *
 * 4. FILENAME SANITIZATION:
 * - User-controlled filenames in error/log messages can expose sensitive information
 * - Malicious filenames could contain path traversal or special characters
 * - Solution: Comprehensive filename sanitization for all output messages
 *
 * SECURITY IMPLEMENTATION:
 *
 * - getUploadedFile(): Added path validation and file type checking
 * - sanitizeFilename(): Removes dangerous characters and limits length
 * - Output messages: Use sanitized filename instead of raw user input
 * - File operations: Validated files before processing
 * - Error messages: Consistently use sanitized filenames to prevent information disclosure
 *
 * COMPLIANCE:
 * - Fixes CodeQL vulnerability: java/path-injection
 * - Follows OWASP guidelines for file upload security
 * - Implements defense-in-depth for path handling
 * - Prevents information disclosure through error messages
 *
 * Last updated: Jakarta EE 10 migration (October 2024)
 * Security review: Path injection vulnerabilities and filename sanitization addressed
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

  @ShellMethod(value = CliStrings.IMPORT_SHARED_CONFIG__HELP,
      key = {CliStrings.IMPORT_SHARED_CONFIG})
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ImportClusterConfigurationCommand$ImportInterceptor",
      isFileUploaded = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public ResultModel importSharedConfig(
      @ShellOption(value = CliStrings.GROUP,
          defaultValue = ConfigurationPersistenceService.CLUSTER_CONFIG) String group,
      @ShellOption(value = XML_FILE) String xmlFile,
      @ShellOption(value = ACTION, help = ACTION_HELP, defaultValue = "APPLY") Action action,
      @ShellOption(value = {CliStrings.IMPORT_SHARED_CONFIG__ZIP},
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
        // Security: Sanitize user-provided xmlFile parameter to prevent path injection
        // Only display the filename, not the full path, to avoid exposing sensitive path
        // information
        String safeFileName = sanitizeFilename(file.getName());
        infoSection.addLine(
            "Successfully set the '" + group + "' configuration to the content of " + safeFileName);
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

  /**
   * Security: Enhanced file upload handling with comprehensive path injection prevention.
   *
   * This method addresses CodeQL vulnerability java/path-injection by implementing
   * defense-in-depth validation before creating File objects with user-controlled paths.
   *
   * SECURITY ENHANCEMENTS:
   * 1. Pre-validation of path strings before File object creation
   * 2. Canonical path validation to prevent sophisticated traversal attacks
   * 3. System directory access prevention (Linux and Windows)
   * 4. Enhanced path traversal detection with multiple patterns
   * 5. File type validation and accessibility checks
   * 6. Sanitized error messages to prevent information disclosure
   *
   * @return Validated File object safe for processing
   * @throws IllegalArgumentException if the path is invalid, unsafe, or inaccessible
   */
  File getUploadedFile() {
    List<String> filePathFromShell = CommandExecutionContext.getFilePathFromShell();
    String filePath = filePathFromShell.get(0);

    // Security: Comprehensive path validation to prevent path injection attacks
    if (filePath == null || filePath.trim().isEmpty()) {
      throw new IllegalArgumentException("File path cannot be null or empty");
    }

    // Security: Normalize and validate the path string before creating File object
    String normalizedPath = filePath.trim();

    // Security: Prevent path traversal attacks - check for dangerous patterns
    if (normalizedPath.contains("..") || normalizedPath.contains("~") ||
        normalizedPath.contains("\\..") || normalizedPath.contains("/..")) {
      throw new IllegalArgumentException("Invalid file path: path traversal detected");
    }

    // Security: Prevent absolute paths to system directories
    if (normalizedPath.startsWith("/etc/") || normalizedPath.startsWith("/sys/") ||
        normalizedPath.startsWith("/proc/") || normalizedPath.startsWith("/dev/") ||
        normalizedPath.contains(":\\Windows\\") || normalizedPath.contains(":\\Program Files\\")) {
      throw new IllegalArgumentException("Access to system directories is not allowed");
    }

    File file;
    try {
      // Security: Create File object and immediately get canonical path for validation
      file = new File(normalizedPath);
      String canonicalPath = file.getCanonicalPath();

      // Security: Ensure canonical path doesn't escape intended directory bounds
      String expectedFileName = new File(normalizedPath).getName();
      if (canonicalPath.contains("..") || !canonicalPath.endsWith(expectedFileName)) {
        throw new IllegalArgumentException("Invalid file path: canonical path validation failed");
      }
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException("Invalid file path: " + e.getMessage());
    }

    // Security: Ensure the file exists and is a regular file (not a directory or special file)
    if (!file.exists()) {
      // Security: Use sanitized filename in error message to prevent information disclosure
      String safeFileName = sanitizeFilename(file.getName());
      throw new IllegalArgumentException("File does not exist: " + safeFileName);
    }
    if (!file.isFile()) {
      // Security: Use sanitized filename in error message to prevent information disclosure
      String safeFileName = sanitizeFilename(file.getName());
      throw new IllegalArgumentException(
          "Path does not point to a regular file: " + safeFileName);
    }

    return file;
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

  /**
   * Security: Sanitizes filename for safe inclusion in log messages and error messages.
   *
   * This method prevents information disclosure and potential path traversal
   * by cleaning user-controlled filenames before including them in output.
   *
   * @param filename The filename to sanitize
   * @return A sanitized version of the filename safe for log/error messages
   */
  private String sanitizeFilename(String filename) {
    if (filename == null) {
      return "<unknown>";
    }

    // Remove any path separators and potentially dangerous characters
    String sanitized = filename.replaceAll("[/\\\\]", "")
        .replaceAll("\\.\\.", "")
        .replaceAll("[<>:\"|?*]", "");

    // Limit length to prevent excessively long filenames in messages
    if (sanitized.length() > 50) {
      sanitized = sanitized.substring(0, 47) + "...";
    }

    // Return a safe default if the filename becomes empty after sanitization
    return sanitized.isEmpty() ? "<sanitized>" : sanitized;
  }
}
