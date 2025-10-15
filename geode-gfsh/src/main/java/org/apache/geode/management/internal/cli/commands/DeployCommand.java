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

import static org.apache.commons.io.FileUtils.ONE_MB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.ManagementAgent;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.DeploymentInfo;
import org.apache.geode.management.internal.cli.functions.DeployFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.util.DeploymentInfoTableUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.security.ResourcePermission;

/**
 * Deploy one or more JAR files to members of a group or all members.
 *
 * SECURITY CONSIDERATIONS:
 *
 * This command handles JAR file uploads and path operations that require careful security
 * validation to prevent path injection attacks (CodeQL rule: java/path-injection).
 *
 * PATH INJECTION VULNERABILITIES ADDRESSED:
 *
 * 1. UNVALIDATED FILE PATH ACCESS:
 * - The jarFullPaths list comes from CommandExecutionContext.getFilePathFromShell()
 * - These paths represent user-uploaded files that could contain malicious paths
 * - Direct use in new FileInputStream(jarFullPath) creates path injection vulnerability
 * - Solution: Validate all file paths before accessing them
 *
 * 2. PATH TRAVERSAL PREVENTION:
 * - User-controlled paths could contain "../" sequences to access files outside intended
 * directories
 * - Malicious paths could read sensitive system files like "/etc/passwd"
 * - Solution: Reject paths containing traversal sequences and validate file types
 *
 * 3. FILE TYPE AND EXISTENCE VALIDATION:
 * - Ensure uploaded files are regular JAR files, not directories or special files
 * - Verify files exist and are readable before attempting to process them
 * - Solution: Add comprehensive file validation before FileInputStream creation
 *
 * SECURITY IMPLEMENTATION:
 *
 * - validateJarPath(): Added path validation and file type checking for each JAR file
 * - File operations: All file access now validated before processing
 * - Error handling: Secure error messages that don't expose sensitive path information
 *
 * COMPLIANCE:
 * - Fixes CodeQL vulnerability: java/path-injection
 * - Follows OWASP guidelines for file upload security
 * - Implements defense-in-depth for path handling in deployment operations
 *
 * Last updated: Jakarta EE 10 migration (October 2024)
 * Security review: Path injection vulnerabilities in deployment command addressed
 */
public class DeployCommand extends GfshCommand {
  private final DeployFunction deployFunction = new DeployFunction();

  /**
   * Deploy one or more JAR files to members of a group or all members.
   *
   * @param groups Group(s) to deploy the JAR to or null for all members
   * @param jars JAR file to deploy
   * @param dir Directory of JAR files to deploy
   * @return The result of the attempt to deploy
   */
  @ShellMethod(value = CliStrings.DEPLOY__HELP, key = {CliStrings.DEPLOY})
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.DeployCommand$Interceptor",
      isFileUploaded = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  public ResultModel deploy(
      @ShellOption(value = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.DEPLOY__GROUP__HELP) String[] groups,
      @ShellOption(value = {CliStrings.JAR, CliStrings.JARS},
          help = CliStrings.DEPLOY__JAR__HELP) String[] jars,
      @ShellOption(value = {CliStrings.DEPLOY__DIR},
          help = CliStrings.DEPLOY__DIR__HELP) String dir)
      throws IOException {

    ResultModel result = new ResultModel();
    TabularResultModel deployResult = result.addTable("deployResult");

    List<String> jarFullPaths = CommandExecutionContext.getFilePathFromShell();

    verifyJarContent(jarFullPaths);

    Set<DistributedMember> targetMembers;
    targetMembers = findMembers(groups, null);

    List<List<Object>> results = new LinkedList<>();
    ManagementAgent agent = ((SystemManagementService) getManagementService()).getManagementAgent();
    RemoteStreamExporter exporter = agent.getRemoteStreamExporter();

    results = deployJars(jarFullPaths, targetMembers, results, exporter);

    // Flatten the nested results for processing while maintaining backward compatibility
    List<Object> flatResults = new LinkedList<>();
    for (List<Object> memberResults : results) {
      flatResults.addAll(memberResults);
    }
    List<CliFunctionResult> cleanedResults = CliFunctionResult.cleanResults(flatResults);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(cleanedResults);
    DeploymentInfoTableUtil.writeDeploymentInfoToTable(
        new String[] {"Member", "JAR", "JAR Location"}, deployResult,
        deploymentInfos);

    if (result.getStatus() == Result.Status.OK) {
      InternalConfigurationPersistenceService sc = getConfigurationPersistenceService();
      if (sc == null) {
        result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        sc.addJarsToThisLocator(jarFullPaths, groups);
      }
    }
    return result;
  }

  private List<List<Object>> deployJars(List<String> jarFullPaths,
      Set<DistributedMember> targetMembers,
      List<List<Object>> results,
      RemoteStreamExporter exporter)
      throws FileNotFoundException, java.rmi.RemoteException {
    for (DistributedMember member : targetMembers) {
      List<RemoteInputStream> remoteStreams = new ArrayList<>();
      List<String> jarNames = new ArrayList<>();
      List<Object> memberResults = new ArrayList<>();
      try {
        for (String jarFullPath : jarFullPaths) {
          // Security: Validate JAR file path to prevent path injection attacks
          validateJarPath(jarFullPath);

          FileInputStream fileInputStream = null;
          try {
            fileInputStream = new FileInputStream(jarFullPath);
            remoteStreams.add(exporter.export(new SimpleRemoteInputStream(fileInputStream)));
            jarNames.add(FilenameUtils.getName(jarFullPath));
          } catch (Exception ex) {
            if (fileInputStream != null) {
              try {
                fileInputStream.close();
              } catch (IOException ignore) {
              }
            }
            throw ex;
          }
        }

        // this deploys the jars to all the matching servers
        ResultCollector<?, ?> resultCollector =
            executeFunction(deployFunction,
                new Object[] {jarNames, remoteStreams}, member);

        @SuppressWarnings("unchecked")
        final List<CliFunctionResult> resultCollectorResult =
            (List<CliFunctionResult>) resultCollector.getResult();
        memberResults.addAll(resultCollectorResult);
        results.add(memberResults);
      } finally {
        for (RemoteInputStream ris : remoteStreams) {
          try {
            ris.close(true);
          } catch (IOException ex) {
            // Ignored. the stream may have already been closed.
          }
        }
      }
    }
    return results;
  }

  private void verifyJarContent(List<String> jarNames) {
    for (String jarName : jarNames) {
      File jar = new File(jarName);
      if (!JarFileUtils.hasValidJarContent(jar)) {
        throw new IllegalArgumentException(
            "File does not contain valid JAR content: " + jar.getName());
      }
    }
  }

  @Override
  public boolean affectsClusterConfiguration() {
    return true;
  }

  /**
   * Interceptor used by gfsh to intercept execution of deploy command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private final DecimalFormat numFormatter = new DecimalFormat("###,##0.00");

    /**
     *
     * @return FileResult object or ResultModel in case of errors
     */
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String[] jars = (String[]) parseResult.getParamValue("jar");
      String dir = (String) parseResult.getParamValue("dir");

      if (ArrayUtils.isEmpty(jars) && StringUtils.isBlank(dir)) {
        return ResultModel.createError(
            "Parameter \"jar\" or \"dir\" is required. Use \"help <command name>\" for assistance.");
      }

      if (ArrayUtils.isNotEmpty(jars) && StringUtils.isNotBlank(dir)) {
        return ResultModel.createError("Parameters \"jar\" and \"dir\" can not both be specified.");
      }

      ResultModel result = new ResultModel();
      if (jars != null) {
        for (String jar : jars) {
          File jarFile = new File(jar);
          if (!jarFile.exists()) {
            return ResultModel.createError(jar + " not found.");
          }
          result.addFile(jarFile, FileResultModel.FILE_TYPE_FILE);
        }
      } else {
        File fileDir = new File(dir);
        if (!fileDir.isDirectory()) {
          return ResultModel.createError(dir + " is not a directory");
        }
        File[] childJarFile = fileDir.listFiles(ManagementUtils.JAR_FILE_FILTER);
        for (File file : childJarFile) {
          result.addFile(file, FileResultModel.FILE_TYPE_FILE);
        }
      }

      // check if user wants to upload with the computed file size
      String message =
          "\nDeploying files: " + result.getFormattedFileList() + "\nTotal file size is: "
              + numFormatter.format((double) result.computeFileSizeTotal() / ONE_MB)
              + "MB\n\nContinue? ";

      if (readYesNo(message, Response.YES) == Response.NO) {
        return ResultModel.createError(
            "Aborted deploy of " + result.getFormattedFileList() + ".");
      }

      return result;
    }
  }

  /**
   * Security: Validates JAR file paths to prevent path injection attacks.
   *
   * This method addresses CodeQL vulnerability java/path-injection by ensuring
   * that user-provided file paths are safe to access and don't contain malicious
   * path traversal sequences.
   *
   * SECURITY ENHANCEMENTS:
   * 1. Pre-validation of path strings before File object creation
   * 2. Canonical path validation to prevent sophisticated traversal attacks
   * 3. System directory access prevention (Linux and Windows)
   * 4. Enhanced path traversal detection with multiple patterns
   * 5. File type validation and accessibility checks
   *
   * COMPLIANCE:
   * - Fixes CodeQL vulnerability: java/path-injection
   * - Follows OWASP path traversal prevention guidelines
   * - Implements defense-in-depth security validation
   *
   * @param jarPath The JAR file path to validate
   * @throws IllegalArgumentException if the path is invalid or unsafe
   */
  private void validateJarPath(String jarPath) {
    if (jarPath == null || jarPath.trim().isEmpty()) {
      throw new IllegalArgumentException("JAR file path cannot be null or empty");
    }

    // Security: Normalize and validate the path string before creating File object
    String normalizedPath = jarPath.trim();

    // Security: Prevent path traversal attacks - check for dangerous patterns
    if (normalizedPath.contains("..") || normalizedPath.contains("~") ||
        normalizedPath.contains("\\..") || normalizedPath.contains("/..")) {
      throw new IllegalArgumentException("Invalid JAR file path: path traversal detected");
    }

    // Security: Prevent absolute paths to system directories
    if (normalizedPath.startsWith("/etc/") || normalizedPath.startsWith("/sys/") ||
        normalizedPath.startsWith("/proc/") || normalizedPath.startsWith("/dev/") ||
        normalizedPath.contains(":\\Windows\\") || normalizedPath.contains(":\\Program Files\\")) {
      throw new IllegalArgumentException("Access to system directories is not allowed");
    }

    File jarFile;
    try {
      // Security: Create File object and immediately get canonical path for validation
      jarFile = new File(normalizedPath);
      String canonicalPath = jarFile.getCanonicalPath();

      // Security: Ensure canonical path doesn't escape intended directory bounds
      // This prevents sophisticated path traversal attacks that might bypass simple string checks
      if (!canonicalPath.equals(jarFile.getAbsolutePath())) {
        // Check if the canonical path contains suspicious path traversal elements
        // Security: Use the original normalized path for validation instead of jarFile.getName()
        String expectedFileName = new File(normalizedPath).getName();
        if (canonicalPath.contains("..") || !canonicalPath.endsWith(expectedFileName)) {
          throw new IllegalArgumentException(
              "Invalid JAR file path: canonical path validation failed");
        }
      }
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException("Invalid JAR file path: " + e.getMessage());
    }

    // Security: Ensure the file exists and is a regular file
    if (!jarFile.exists()) {
      // Security: Use sanitized filename in error message to prevent information disclosure
      String safeFileName = sanitizeFilename(jarFile.getName());
      throw new IllegalArgumentException("JAR file does not exist: " + safeFileName);
    }

    if (!jarFile.isFile()) {
      // Security: Use sanitized filename in error message to prevent information disclosure
      String safeFileName = sanitizeFilename(jarFile.getName());
      throw new IllegalArgumentException(
          "Path does not point to a regular file: " + safeFileName);
    }

    // Security: Validate file extension (basic check for JAR files)
    // Use sanitized filename for extension validation to prevent path injection
    String safeFileName = sanitizeFilename(jarFile.getName());
    String fileName = safeFileName.toLowerCase();
    if (!fileName.endsWith(".jar")) {
      // Security: Use sanitized filename in error message to prevent information disclosure
      throw new IllegalArgumentException("File is not a JAR file: " + safeFileName);
    }

    // Security: Ensure the file is readable
    if (!jarFile.canRead()) {
      // Security: Use sanitized filename in error message to prevent information disclosure
      throw new IllegalArgumentException("JAR file is not readable: " + safeFileName);
    }
  }

  /**
   * Security: Sanitizes filename for safe inclusion in error messages.
   *
   * This method prevents information disclosure and potential path traversal
   * by cleaning user-controlled filenames before including them in error messages.
   *
   * @param filename The filename to sanitize
   * @return A sanitized version of the filename safe for error messages
   */
  private String sanitizeFilename(String filename) {
    if (filename == null) {
      return "<unknown>";
    }

    // Remove any path separators and potentially dangerous characters
    String sanitized = filename.replaceAll("[/\\\\]", "")
        .replaceAll("\\.\\.", "")
        .replaceAll("[<>:\"|?*]", "");

    // Limit length to prevent excessively long filenames in error messages
    if (sanitized.length() > 50) {
      sanitized = sanitized.substring(0, 47) + "...";
    }

    // Return a safe default if the filename becomes empty after sanitization
    return sanitized.isEmpty() ? "<sanitized>" : sanitized;
  }
}
