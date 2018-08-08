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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.apache.commons.io.IOUtils;

/**
 * Test framework utility class to programmatically create classes, JARs and ClassLoaders that
 * include the classes.
 *
 * @since GemFire 7.0
 */
@SuppressWarnings("serial")
public class ClassBuilder implements Serializable {

  private String classPath = System.getProperty("java.class.path");

  /**
   * Write a JAR with an empty class using the given class name. The className may have a package
   * separated by /. For example: my/package/myclass
   *
   * @param className Name of the class to create
   * @param outputFile Where to write the JAR file
   * @throws IOException If there is a problem creating the output stream for the JAR file.
   */
  public void writeJarFromName(final String className, final File outputFile) throws IOException {
    writeJarFromContent(className, "public class " + className + "{}", outputFile);
  }

  /**
   * Write a JAR with a class of the given name with the provided content. The className may have a
   * package separated by /. For example: my/package/myclass
   *
   * @param className Name of the class to create
   * @param content Content of the created class
   * @param outputFile Where to write the JAR file
   * @throws IOException If there is a problem writing the JAR file.
   */
  public void writeJarFromContent(final String className, final String content,
      final File outputFile) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
    writeJarFromContent(className, content, fileOutputStream);
    fileOutputStream.close();
  }

  /**
   * Create a JAR with an empty class using the given class name. The className may have a package
   * separated by /. For example: my/package/myclass
   *
   * @param className Name of the class to create
   * @return The JAR file contents
   * @throws IOException If there is a problem creating the output stream for the JAR file.
   */
  public byte[] createJarFromName(final String className) throws IOException {
    return createJarFromClassContent(className, "public class " + className + "{}");
  }

  /**
   * Create a JAR using the given file contents and with the given file name.
   *
   * @param fileName Name of the file to create
   * @param content Content of the created file
   * @return The JAR file content
   * @throws IOException If there is a problem creating the output stream for the JAR file.
   */
  public byte[] createJarFromFileContent(final String fileName, final String content)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    JarOutputStream jarOutputStream = new JarOutputStream(byteArrayOutputStream);

    JarEntry entry = new JarEntry(fileName);
    entry.setTime(System.currentTimeMillis());
    jarOutputStream.putNextEntry(entry);
    jarOutputStream.write(content.getBytes());
    jarOutputStream.closeEntry();

    jarOutputStream.close();
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Create a JAR using the given class contents and with the given class name.
   *
   * @param className Name of the class to create
   * @param content Content of the created class
   * @return The JAR file content
   * @throws IOException If there is a problem creating the output stream for the JAR file.
   */
  public byte[] createJarFromClassContent(final String className, final String content)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    writeJarFromContent(className, content, byteArrayOutputStream);
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Write a JAR with a class of the given name with the provided content. The className may have a
   * package separated by /. For example: my/package/myclass
   *
   * @param className Name of the class to create
   * @param content Content of the created class
   * @param outStream Stream to write the JAR to
   * @throws IOException If there is a problem creating the output stream for the JAR file.
   */
  public void writeJarFromContent(final String className, final String content,
      final OutputStream outStream) throws IOException {

    byte[] bytes = compileClass(className, content);

    createJar(className, outStream, bytes);
    return;
  }

  private void createJar(String className, OutputStream outStream, byte[] bytes)
      throws IOException {
    JarOutputStream jarOutputStream = new JarOutputStream(outStream);

    // Add the class file to the JAR file
    String formattedName = className;
    if (!formattedName.endsWith(".class")) {
      formattedName = formattedName.concat(".class");
    }

    JarEntry entry = new JarEntry(formattedName);
    entry.setTime(System.currentTimeMillis());
    jarOutputStream.putNextEntry(entry);
    jarOutputStream.write(bytes);
    jarOutputStream.closeEntry();

    jarOutputStream.close();
  }

  public void writeJarFromClass(Class clazz, File jar) throws IOException {
    String className = clazz.getName();
    String classAsPath = className.replace('.', '/') + ".class";
    InputStream stream = clazz.getClassLoader().getResourceAsStream(classAsPath);
    byte[] bytes = IOUtils.toByteArray(stream);
    try (FileOutputStream out = new FileOutputStream(jar)) {
      createJar(classAsPath, out, bytes);
    }

  }

  /**
   * Creates a ClassLoader that contains an empty class with the given name using the given content.
   * The className may have a package separated by /. For example: my/package/myclass
   *
   * @param className Name of the class to create
   * @param content Content of the created class
   * @return The class loader
   * @throws IOException If there's a problem creating the output stream used to generate the class
   */
  public ClassLoader createClassLoaderFromContent(final String className, final String content)
      throws IOException {
    byte[] classDefinition = compileClass(className, content);
    SingleClassClassLoader classLoader = new SingleClassClassLoader();
    classLoader.addClass(className, classDefinition);
    return classLoader;
  }

  /**
   * Compile the provided class. The className may have a package separated by /. For example:
   * my/package/myclass
   *
   * @param className Name of the class to compile.
   * @param classCode Plain text contents of the class
   * @return The byte contents of the compiled class.
   */
  public byte[] compileClass(final String className, final String classCode) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    OutputStreamJavaFileManager<JavaFileManager> fileManager =
        new OutputStreamJavaFileManager<JavaFileManager>(
            javaCompiler.getStandardFileManager(null, null, null), byteArrayOutputStream);

    List<JavaFileObject> fileObjects = new ArrayList<JavaFileObject>();
    fileObjects.add(new JavaSourceFromString(className, classCode));

    List<String> options = Arrays.asList("-classpath", this.classPath);
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
    if (!javaCompiler.getTask(null, fileManager, diagnostics, options, null, fileObjects).call()) {
      StringBuilder errorMsg = new StringBuilder();
      for (Diagnostic d : diagnostics.getDiagnostics()) {
        String err = String.format("Compilation error: Line %d - %s%n", d.getLineNumber(),
            d.getMessage(null));
        errorMsg.append(err);
        System.err.print(err);
      }
      throw new IOException(errorMsg.toString());
    }
    return byteArrayOutputStream.toByteArray();
  }

  /***
   * Add to the ClassPath used when compiling.
   *
   * @param path Path to add
   * @return The complete, new ClassPath
   */
  public String addToClassPath(final String path) {
    this.classPath += (System.getProperty("path.separator") + path);
    return this.classPath;
  }

  /**
   * Get the current ClassPath used when compiling.
   *
   * @return The ClassPath used when compiling.
   */
  public String getClassPath() {
    return this.classPath;
  }

  private class JavaSourceFromString extends SimpleJavaFileObject {
    final String code;

    JavaSourceFromString(final String name, final String code) {
      super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
      this.code = code;
    }

    @Override
    public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
      return this.code;
    }
  }

  private class OutputStreamSimpleFileObject extends SimpleJavaFileObject {
    private OutputStream outputStream;

    protected OutputStreamSimpleFileObject(final URI uri, final JavaFileObject.Kind kind,
        final OutputStream outputStream) {
      super(uri, kind);
      this.outputStream = outputStream;
    }

    @Override
    public OutputStream openOutputStream() {
      return this.outputStream;
    }
  }

  private class OutputStreamJavaFileManager<M extends JavaFileManager>
      extends ForwardingJavaFileManager<M> {
    private OutputStream outputStream;

    protected OutputStreamJavaFileManager(final M fileManager, final OutputStream outputStream) {
      super(fileManager);
      this.outputStream = outputStream;
    }

    @Override
    public JavaFileObject getJavaFileForOutput(final JavaFileManager.Location location,
        final String className, final JavaFileObject.Kind kind, final FileObject sibling) {
      return new OutputStreamSimpleFileObject(new File(className).toURI(), kind, outputStream);
    }
  }

  public class SingleClassClassLoader extends ClassLoader {
    public Class addClass(final String className, final byte[] definition) {
      return super.defineClass(className, definition, 0, definition.length);
    }
  }
}
