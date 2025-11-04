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
package org.apache.geode.management.internal.cli.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * Secure path resolver for file deployment operations.
 *
 * <p>
 * Prevents path traversal attacks (CWE-22) by validating and normalizing file paths before they
 * are used in file system operations.
 *
 * <p>
 * <b>Security Features:</b>
 * <ul>
 * <li>Canonical path resolution (resolves symlinks and relative paths)</li>
 * <li>Path traversal detection (blocks ../, ~, etc.)</li>
 * <li>System directory blacklist (prevents access to /etc, /sys, etc.)</li>
 * <li>Symbolic link detection and resolution</li>
 * <li>File type validation</li>
 * <li>Base directory containment checks</li>
 * </ul>
 *
 * <p>
 * <b>Usage Example:</b>
 *
 * <pre>
 * SecurePathResolver resolver = new SecurePathResolver(null);
 * Path validatedPath = resolver.resolveSecurePath(userInput, true, true);
 * File safeFile = validatedPath.toFile();
 * </pre>
 *
 * @since Geode 2.0 (Jakarta EE 10 migration - GEODE-10466)
 */
public class SecurePathResolver {

  // Blacklisted system directories (Linux/Unix)
  private static final Set<String> SYSTEM_DIRECTORIES = new HashSet<>();
  static {
    SYSTEM_DIRECTORIES.add("/etc");
    SYSTEM_DIRECTORIES.add("/sys");
    SYSTEM_DIRECTORIES.add("/proc");
    SYSTEM_DIRECTORIES.add("/dev");
    SYSTEM_DIRECTORIES.add("/boot");
    SYSTEM_DIRECTORIES.add("/root");
  }

  // Blacklisted system directories (Windows)
  private static final Set<String> WINDOWS_SYSTEM_DIRECTORIES = new HashSet<>();
  static {
    WINDOWS_SYSTEM_DIRECTORIES.add(":\\Windows\\System32");
    WINDOWS_SYSTEM_DIRECTORIES.add(":\\Windows\\SysWOW64");
    WINDOWS_SYSTEM_DIRECTORIES.add(":\\Program Files\\");
    WINDOWS_SYSTEM_DIRECTORIES.add(":\\Program Files (x86)\\");
  }

  private final Path baseDirectory;

  /**
   * Creates a secure path resolver with optional base directory constraint.
   *
   * @param baseDirectory The base directory to restrict operations to (null = no restriction)
   */
  public SecurePathResolver(Path baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  /**
   * Resolves and validates a file path for safe file system access.
   *
   * <p>
   * This method performs comprehensive security checks to prevent path traversal attacks (CWE-22,
   * CodeQL java/path-injection):
   *
   * <ol>
   * <li>Path normalization and canonicalization</li>
   * <li>Path traversal detection (../, ~, symlinks)</li>
   * <li>System directory access prevention</li>
   * <li>Base directory containment verification</li>
   * <li>File existence and type validation</li>
   * </ol>
   *
   * @param userProvidedPath The path provided by user (potentially malicious)
   * @param mustExist Whether the file must exist (true) or can be new (false)
   * @param mustBeFile Whether the path must point to a regular file
   * @return A validated, canonical Path object safe for file operations
   * @throws SecurityException if the path is invalid or poses security risk
   */
  public Path resolveSecurePath(String userProvidedPath, boolean mustExist, boolean mustBeFile)
      throws SecurityException {

    // Step 1: Basic null/empty validation
    if (userProvidedPath == null || userProvidedPath.trim().isEmpty()) {
      throw new SecurityException("Path cannot be null or empty");
    }

    String normalizedInput = userProvidedPath.trim();

    // Step 2: Check for obvious path traversal patterns
    if (normalizedInput.contains("..") || normalizedInput.contains("~")) {
      throw new SecurityException(
          "Path traversal patterns detected: " + sanitizePath(normalizedInput));
    }

    // Step 3: Convert to Path object and validate
    Path userPath;
    try {
      userPath = Paths.get(normalizedInput);
    } catch (InvalidPathException e) {
      throw new SecurityException("Invalid path syntax: " + sanitizePath(normalizedInput), e);
    }

    // Step 4: Get absolute path and resolve against base directory if provided
    Path resolvedPath;
    if (baseDirectory != null) {
      // Resolve relative paths against base directory
      resolvedPath = baseDirectory.resolve(userPath).normalize();
    } else {
      resolvedPath = userPath.toAbsolutePath().normalize();
    }

    // Step 5: Check normalized path for additional traversal patterns
    // This catches cases where ".." appears in the normalized form
    String normalizedStr = resolvedPath.normalize().toString();
    if (normalizedStr.contains("..")) {
      throw new SecurityException(
          "Path traversal detected in normalized path: " + sanitizePath(normalizedInput));
    }

    // Step 6: Check for system directory access BEFORE checking existence
    // This prevents information disclosure about system files
    String resolvedString = resolvedPath.toString();
    for (String sysDir : SYSTEM_DIRECTORIES) {
      if (resolvedString.startsWith(sysDir)) {
        throw new SecurityException("Access to system directory denied: " + sysDir);
      }
    }

    // Step 7: Check for system directory access (Windows)
    for (String winDir : WINDOWS_SYSTEM_DIRECTORIES) {
      if (resolvedString.contains(winDir)) {
        throw new SecurityException("Access to system directory denied");
      }
    }

    // Step 8: Get canonical path (resolves symlinks)
    Path canonicalPath;
    try {
      // toRealPath() throws if file doesn't exist and NOFOLLOW_LINKS not set
      if (Files.exists(resolvedPath)) {
        canonicalPath = resolvedPath.toRealPath();
      } else {
        if (mustExist) {
          throw new SecurityException(
              "Path does not exist: " + sanitizePath(normalizedInput));
        }
        // If file doesn't need to exist, use normalized path
        canonicalPath = resolvedPath;
      }
    } catch (IOException e) {
      throw new SecurityException(
          "Cannot resolve canonical path: " + sanitizePath(normalizedInput), e);
    }

    // Step 9: Verify base directory containment
    if (baseDirectory != null) {
      Path canonicalBase;
      try {
        canonicalBase = baseDirectory.toRealPath();
      } catch (IOException e) {
        throw new SecurityException("Base directory not accessible", e);
      }

      if (!canonicalPath.startsWith(canonicalBase)) {
        throw new SecurityException(
            "Path escapes base directory: " + sanitizePath(normalizedInput));
      }
    }

    // Step 10: Validate file existence and type
    if (mustExist) {
      if (!Files.exists(canonicalPath)) {
        throw new SecurityException("File does not exist: " + sanitizePath(normalizedInput));
      }

      if (mustBeFile && !Files.isRegularFile(canonicalPath)) {
        throw new SecurityException(
            "Path does not point to a regular file: " + sanitizePath(normalizedInput));
      }
    }

    return canonicalPath;
  }

  /**
   * Sanitizes a path for safe inclusion in error messages.
   *
   * <p>
   * Prevents information disclosure attacks by removing sensitive path information and limiting
   * output length.
   *
   * @param path The path to sanitize
   * @return A safe version for error messages
   */
  private String sanitizePath(String path) {
    if (path == null) {
      return "<null>";
    }

    try {
      // Get just the filename, not full path
      Path p = Paths.get(path);
      String filename = p.getFileName() != null ? p.getFileName().toString() : path;

      // Remove dangerous characters
      String sanitized = filename.replaceAll("[<>:\"|?*]", "_");

      // Limit length
      if (sanitized.length() > 50) {
        sanitized = sanitized.substring(0, 47) + "...";
      }

      return sanitized;
    } catch (InvalidPathException e) {
      return "<invalid-path>";
    }
  }
}
