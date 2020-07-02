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
package org.apache.geode.management.internal.cli.shell;

import static java.lang.System.lineSeparator;

import java.io.File;

class DirectoryTree {

  /**
   * Pretty print the directory tree and its file names.
   */
  static String printDirectoryTree(File folder) {
    if (!folder.isDirectory()) {
      throw new IllegalArgumentException("folder is not a Directory");
    }
    int indent = 0;
    StringBuilder sb = new StringBuilder();
    printDirectoryTree(folder, indent, sb);
    return sb.toString();
  }

  private static void printDirectoryTree(File folder, int indent, StringBuilder sb) {
    if (!folder.isDirectory()) {
      throw new IllegalArgumentException("folder is not a Directory");
    }
    sb.append(getIndentString(indent));
    sb.append("+--");
    sb.append(folder.getName());
    sb.append("/");
    sb.append(lineSeparator());
    for (File file : folder.listFiles()) {
      if (file.isDirectory()) {
        printDirectoryTree(file, indent + 1, sb);
      } else {
        printFile(file, indent + 1, sb);
      }
    }
  }

  private static void printFile(File file, int indent, StringBuilder sb) {
    sb.append(getIndentString(indent));
    sb.append("+--");
    sb.append(file.getName());
    sb.append(lineSeparator());
  }

  private static String getIndentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      sb.append("|  ");
    }
    return sb.toString();
  }
}
