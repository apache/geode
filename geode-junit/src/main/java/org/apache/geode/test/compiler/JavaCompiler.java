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
package org.apache.geode.test.compiler;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import javax.tools.ToolProvider;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;


public class JavaCompiler {
  private Path tempDir;
  private String classpath;

  public JavaCompiler() throws IOException {
    this.tempDir = Files.createTempDirectory("javaCompiler");
    tempDir.toFile().deleteOnExit();
    this.classpath = System.getProperty("java.class.path");
  }

  public void addToClasspath(File jarFile) {
    classpath += File.pathSeparator + jarFile.getAbsolutePath();
  }

  public List<CompiledSourceCode> compile(File... sourceFiles) throws IOException {
    String[] sourceFileContents =
        Arrays.stream(sourceFiles).map(this::readFileToString).toArray(String[]::new);

    return compile(sourceFileContents);
  }

  public List<CompiledSourceCode> compile(String... sourceFileContents) throws IOException {
    UncompiledSourceCode[] uncompiledSourceCodes = Arrays.stream(sourceFileContents)
        .map(UncompiledSourceCode::fromSourceCode).toArray(UncompiledSourceCode[]::new);

    return compile(uncompiledSourceCodes);
  }

  public List<CompiledSourceCode> compile(UncompiledSourceCode... uncompiledSources)
      throws IOException {
    File temporarySourcesDirectory = createSubdirectory(tempDir, "sources");
    File temporaryClassesDirectory = createSubdirectory(tempDir, "classes");

    List<String> options =
        Stream.of("-d", temporaryClassesDirectory.getAbsolutePath(), "-classpath", classpath)
            .collect(toList());

    try {
      for (UncompiledSourceCode sourceCode : uncompiledSources) {
        File sourceFile =
            new File(temporarySourcesDirectory, sourceCode.getSimpleClassName() + ".java");
        FileUtils.writeStringToFile(sourceFile, sourceCode.getSourceCode(), Charsets.UTF_8);
        options.add(sourceFile.getAbsolutePath());
      }

      int exitCode = ToolProvider.getSystemJavaCompiler().run(System.in, System.out, System.err,
          options.toArray(new String[] {}));

      if (exitCode != 0) {
        throw new RuntimeException(
            "Unable to compile the given source code. See System.err for details.");
      }

      List<CompiledSourceCode> compiledSourceCodes = new ArrayList<>();
      addCompiledClasses(compiledSourceCodes, "", temporaryClassesDirectory);
      return compiledSourceCodes;
    } finally {
      FileUtils.deleteDirectory(temporaryClassesDirectory);
    }
  }

  private static void addCompiledClasses(List<CompiledSourceCode> ret, String pkgName, File dir)
      throws IOException {
    for (File file : dir.listFiles()) {
      String filename = file.getName();

      if (file.isDirectory()) {
        String qname = pkgName + filename + ".";
        addCompiledClasses(ret, qname, file);
      } else if (filename.endsWith(".class")) {
        String qname = pkgName + filename.substring(0, filename.length() - 6);
        ret.add(new CompiledSourceCode(qname, FileUtils.readFileToByteArray(file)));
      } else {
        System.err.println("Unexpected file : " + file.getAbsolutePath());
      }
    }
  }

  private File createSubdirectory(Path parent, String directoryName) {
    File subdirectory = parent.resolve(directoryName).toFile();
    if (!subdirectory.exists()) {
      subdirectory.mkdirs();
    }

    if (!subdirectory.exists() || !subdirectory.isDirectory()) {
      throw new IllegalArgumentException("Invalid directory" + subdirectory.getAbsolutePath());
    }

    return subdirectory;
  }

  private String readFileToString(File file) {
    try {
      return FileUtils.readFileToString(file, Charsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
