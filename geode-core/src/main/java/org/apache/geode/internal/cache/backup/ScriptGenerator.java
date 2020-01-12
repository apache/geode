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
package org.apache.geode.internal.cache.backup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

interface ScriptGenerator {

  void writePreamble(BufferedWriter writer) throws IOException;

  void writeExit(BufferedWriter writer) throws IOException;

  void writeCopyFile(BufferedWriter writer, File backup, File original) throws IOException;

  void writeCopyDirectoryContents(BufferedWriter writer, File backup, File original,
      boolean backupHasFiles) throws IOException;

  void writeExistenceTest(BufferedWriter writer, File file) throws IOException;

  void writeComment(BufferedWriter writer, String string) throws IOException;

  String getScriptName();
}
