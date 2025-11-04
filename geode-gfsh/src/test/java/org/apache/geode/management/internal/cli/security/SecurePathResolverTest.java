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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link SecurePathResolver}.
 *
 * Tests comprehensive path traversal attack prevention including: - Path traversal patterns (../,
 * ~)
 * - System directory access attempts - Symbolic link resolution - Base directory containment -
 * Canonical path validation
 */
public class SecurePathResolverTest {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private SecurePathResolver resolver;
  private Path testFile;

  @Before
  public void setup() throws IOException {
    resolver = new SecurePathResolver(null);
    testFile = tempDir.newFile("test.jar").toPath();
  }

  @After
  public void cleanup() {
    // TemporaryFolder rule handles cleanup
  }

  @Test
  public void testValidPath() {
    Path resolved = resolver.resolveSecurePath(testFile.toString(), true, true);
    assertThat(resolved).exists();
    assertThat(Files.isRegularFile(resolved)).isTrue();
  }

  @Test
  public void testPathTraversalWithDoubleDot() {
    assertThatThrownBy(() -> resolver.resolveSecurePath("../etc/passwd", true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("traversal");
  }

  @Test
  public void testPathTraversalWithTilde() {
    assertThatThrownBy(() -> resolver.resolveSecurePath("~/../../etc/passwd", true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("traversal");
  }

  @Test
  public void testNullPath() {
    assertThatThrownBy(() -> resolver.resolveSecurePath(null, true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("null or empty");
  }

  @Test
  public void testEmptyPath() {
    assertThatThrownBy(() -> resolver.resolveSecurePath("   ", true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("null or empty");
  }

  @Test
  public void testSystemDirectoryAccessEtc() {
    assertThatThrownBy(() -> resolver.resolveSecurePath("/etc/shadow", true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("system directory");
  }

  @Test
  public void testSystemDirectoryAccessSys() {
    assertThatThrownBy(() -> resolver.resolveSecurePath("/sys/kernel/config", true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("system directory");
  }

  @Test
  public void testSystemDirectoryAccessProc() {
    assertThatThrownBy(() -> resolver.resolveSecurePath("/proc/cpuinfo", true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("system directory");
  }

  @Test
  public void testNonExistentFileMustExist() {
    assertThatThrownBy(
        () -> resolver.resolveSecurePath("/tmp/nonexistent-file-12345.jar", true, true))
            .isInstanceOf(SecurityException.class)
            .hasMessageContaining("does not exist");
  }

  @Test
  public void testNonExistentFileAllowed() {
    Path resolved = resolver.resolveSecurePath("/tmp/newfile-12345.jar", false, false);
    assertThat(resolved).isNotNull();
  }

  @Test
  public void testDirectoryWhenFileMustBe() throws IOException {
    Path dir = tempDir.newFolder("testdir").toPath();

    assertThatThrownBy(() -> resolver.resolveSecurePath(dir.toString(), true, true))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("regular file");
  }

  @Test
  public void testBaseDirContainment() throws IOException {
    Path baseDir = tempDir.newFolder("base").toPath();
    Path fileInBase = Files.createFile(baseDir.resolve("allowed.jar"));

    SecurePathResolver restrictedResolver = new SecurePathResolver(baseDir);

    // Should succeed - file is within base directory
    Path resolved = restrictedResolver.resolveSecurePath(fileInBase.toString(), true, true);
    assertThat(resolved).startsWith(baseDir);
  }

  @Test
  public void testBaseDirEscape() throws IOException {
    Path baseDir = tempDir.newFolder("base").toPath();
    Path fileOutsideBase = tempDir.newFile("outside.jar").toPath();

    SecurePathResolver restrictedResolver = new SecurePathResolver(baseDir);

    // Should fail - file is outside base directory
    assertThatThrownBy(
        () -> restrictedResolver.resolveSecurePath(fileOutsideBase.toString(), true, true))
            .isInstanceOf(SecurityException.class)
            .hasMessageContaining("escapes base directory");
  }

  @Test
  public void testSymlinkResolution() throws IOException {
    // Only run this test on systems that support symlinks
    if (!Files.getFileStore(tempDir.getRoot().toPath()).supportsFileAttributeView("posix")) {
      return; // Skip on Windows
    }

    Path actualFile = tempDir.newFile("actual.jar").toPath();
    Path symlinkPath = tempDir.getRoot().toPath().resolve("symlink.jar");

    try {
      Files.createSymbolicLink(symlinkPath, actualFile);

      Path resolved = resolver.resolveSecurePath(symlinkPath.toString(), true, true);

      // Should resolve to the actual file (canonical path)
      assertThat(resolved).isEqualTo(actualFile.toRealPath());
    } catch (UnsupportedOperationException e) {
      // Skip test if symlinks not supported
    }
  }

  @Test
  public void testRelativePathResolution() throws IOException {
    Path file = tempDir.newFile("relative.jar").toPath();

    // Use absolute path since relative paths are resolved against current working directory
    // which we cannot easily change in a test
    Path resolved = resolver.resolveSecurePath(file.toString(), true, true);
    assertThat(resolved.getFileName().toString()).isEqualTo("relative.jar");
  }

  @Test
  public void testMultiplePathTraversalAttempts() {
    String[] maliciousPaths = {
        "../../../etc/passwd",
        "..\\..\\..\\Windows\\System32\\config\\sam",
        "~/../../etc/shadow",
        "....//....//etc/passwd",
        "..%2F..%2Fetc%2Fpasswd"
    };

    for (String path : maliciousPaths) {
      assertThatThrownBy(() -> resolver.resolveSecurePath(path, true, true))
          .isInstanceOf(SecurityException.class)
          .as("Should block path: " + path);
    }
  }

  @Test
  public void testValidAbsolutePath() {
    Path resolved = resolver.resolveSecurePath(testFile.toAbsolutePath().toString(), true, true);
    assertThat(resolved).exists();
  }

  @Test
  public void testInvalidPathSyntax() {
    // Test with null bytes (invalid on most file systems)
    assertThatThrownBy(() -> resolver.resolveSecurePath("file\0name.jar", true, true))
        .isInstanceOf(SecurityException.class);
  }
}
