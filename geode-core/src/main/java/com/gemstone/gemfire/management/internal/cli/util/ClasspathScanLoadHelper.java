/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.management.internal.cli.CliUtil;

/**
 * Utility class to scan class-path & load classes.
 * 
 * 
 * @since GemFire 7.0
 */
public class ClasspathScanLoadHelper {
  private static final String CLASSFILE_EXTENSION = ".class";

  public static Set<Class<?>> loadAndGet(String commandPackageName,
                              Class<?> requiredInterfaceToLoad,
                              boolean onlyInstantiable) throws ClassNotFoundException, IOException {
    
    Set<Class<?>> classSet  = new HashSet<Class<?>>();
    Class<?>      classes[] = getClasses(commandPackageName);

    for (int i = 0; i < classes.length; i++) {
      if (implementsType(classes[i], requiredInterfaceToLoad)) {
        if (onlyInstantiable) {
          if (isInstantiable(classes[i])) {
            classSet.add(classes[i]);
          }
        } else {
          classSet.add(classes[i]);
        }
      }
    }
    
    return classSet;
  }
  
  public static boolean isInstantiable(Class<?> klass) {
    int modifiers = klass.getModifiers();
    
    boolean isInstantiable = !Modifier.isAbstract(modifiers) && 
                             !Modifier.isInterface(modifiers) && 
                             Modifier.isPublic(modifiers); 
    
    return isInstantiable;
  }

  private static boolean implementsType(Class<?> typeToCheck, Class<?> requiredInterface) {
    if(requiredInterface.isAssignableFrom(typeToCheck)){
      return true;
    }else{
      return false;
    }
  }

  public static Class<?>[] getClasses(String packageName)
      throws ClassNotFoundException, IOException {
    String packagePath = packageName.replace('.', '/');

    List<File> dirs = new ArrayList<File>();

    Enumeration<URL> resources = ClassPathLoader.getLatest().getResources(packagePath);
    List<Class<?>> classesList = new ArrayList<Class<?>>();
    
    while (resources.hasMoreElements()) {
      URL packageUrl = resources.nextElement();
      
      String actualPackagePath = packageUrl.getPath();
      int jarIndex = actualPackagePath.indexOf(".jar!");
      if (jarIndex != -1) { // resource appears to be in a jar
        String jarPath = actualPackagePath.substring(0, jarIndex + ".jar".length());

        if (jarPath.startsWith("file:")) {
          if (File.separatorChar == '/') {//whether Unix or Windows system
            // On Unix, to get actual path, we remove "file:" from the Path
            jarPath = jarPath.substring("file:".length());
          } else {
            // On Windows jarPaths are like:
            // Local Path:   file:/G:/where/java/spring/spring-shell/1.0.0/spring-shell-1.0.0.RELEASE.jar
            // Network Path: file://stinger.pune.gemstone.com/shared/where/java/spring/spring-shell/1.0.0/spring-shell-1.0.0.RELEASE.jar
            // To get actual path, we remove "file:/" from the Path
            jarPath = jarPath.substring("file:/".length());
            // If the path still starts with a "/", then it's a network path. 
            // Hence, add one "/". 
            if (jarPath.startsWith("/") && !jarPath.startsWith("//")) {
              jarPath = "/" + jarPath;
            }
          }
        }
        // decode the jarPath as it's derived from an URL
        Class<?>[] classes = getClasses(CliUtil.decodeWithDefaultCharSet(jarPath), packageName);
        classesList.addAll(Arrays.asList(classes));
      } else {
        dirs.add(new File(packageUrl.getFile()));
      }
    }

    for (File directory : dirs) {
      classesList.addAll(findClasses(directory, packageName));
    }

    return (Class[]) classesList.toArray(new Class[classesList.size()]);
  }

  public static List<Class<?>> findClasses(File directory, String packageName)
      throws ClassNotFoundException {
    List<Class<?>> classes = new ArrayList<Class<?>>();
    if (!directory.exists()) {
      return classes;
    }

    ClassPathLoader cpLoader = ClassPathLoader.getLatest();
    // Load only .class files that are not from test code
    TestClassFilter tcf = new TestClassFilter();
    
    File[] files = directory.listFiles(tcf);
    File   file  = null;
    for (int i = 0; i < files.length; i++) {
      file = files[i];
      if (file.isDirectory()) {//sub-package
        // assert !file.getName().contains(".");
        classes.addAll(findClasses(file, packageName + "." + file.getName()));
      } else {
        //remove .class from the file name
        String classSimpleName = file.getName().substring(0, file.getName().length() - CLASSFILE_EXTENSION.length());
        classes.add(cpLoader.forName(packageName + '.' + classSimpleName));
      }
    }
    return classes;
  }
  

  /**
   * Returns all classes that are in the specified jar and package name.
   * 
   * @param jarPath
   *          The absolute or relative jar path.
   * @param packageName
   *          The package name.
   * @return Returns all classes that are in the specified jar and package name.
   * @throws ClassNotFoundException
   *           Thrown if unable to load a class
   * @throws IOException
   *           Thrown if error occurs while reading the jar file
   */
  public static Class<?>[] getClasses(String jarPath, String packageName) 
      throws ClassNotFoundException, IOException {
    ClassPathLoader cpLoader = ClassPathLoader.getLatest();
    
    String[] classNames = getClassNames(jarPath, packageName);
    Class<?> classes[]  = new Class[classNames.length];
    for (int i = 0; i < classNames.length; i++) {
      String className = (String)classNames[i];
      classes[i] = cpLoader.forName(className);
    }
    return classes;
  }

  /**
   * Returns all names of classes that are defined in the specified jar and
   * package name.
   * 
   * @param jarPath
   *          The absolute or relative jar path.
   * @param packageName
   *          The package name.
   * @return Returns all names of classes that are defined in the specified jar
   *         and package name.
   * @throws IOException
   *           Thrown if error occurs while reading the jar file
   */
  public static String[] getClassNames(String jarPath, String packageName) 
      throws IOException {
    if (jarPath == null) {
      return new String[0];
    }
    
    File file;
    //Path is absolute on Unix if it starts with '/' 
    //or path contains colon on Windows 
    if (jarPath.startsWith("/") || (jarPath.indexOf(':') >= 0 && File.separatorChar == '\\' )) {
      // absolute path
      file = new File(jarPath);
    } else {
      // relative path
      String workingDir = System.getProperty("user.dir");
      file = new File(workingDir + File.separator + jarPath);
    }
    
    List<String> classNames = new ArrayList<String>();
    String packagePath = packageName.replaceAll("\\.", "/");
    JarInputStream jarFile = new JarInputStream(new FileInputStream(file));
    JarEntry jarEntry;
    while (true) {
      jarEntry = jarFile.getNextJarEntry();
      if (jarEntry == null) {
        break;
      }
      String name = jarEntry.getName();
      if (name.startsWith(packagePath) && (name.endsWith(CLASSFILE_EXTENSION))) {
        int endIndex = name.length() - 6;
        name = name.replaceAll("/", "\\.");
        name = name.substring(0, endIndex);
        classNames.add(name);
      }
    }
    jarFile.close();

    return (String[])classNames.toArray(new String[0]);
  }

  /**
   * FileFilter to filter out GemFire Test Code.
   * 
   * @since GemFire 7.0
   */
  static class TestClassFilter implements FileFilter {
    private static final String TESTS_CODE_INDICATOR = "Test";

    @Override
    public boolean accept(File pathname) {
      String pathToCheck = pathname.getName();
      return !pathToCheck.contains(TESTS_CODE_INDICATOR) && pathToCheck.endsWith(CLASSFILE_EXTENSION);
    }
  }
}
