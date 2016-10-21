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
package org.apache.geode.admin;

import org.apache.geode.internal.i18n.LocalizedStrings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;


/**
 * AdminConfig loads/stores the member information list. The list contains all of the members being
 * monitored.
 *
 * Config must be of the format:
 * <p>
 * <li>Name=What you want displayed as a name for the instance
 * <li>Type=SERVER|CLIENT
 * <li>Host=A valid hostname or IP Address where the instance is running
 * <li>Port=The port you are using to open the monitor port for the instance
 * 
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public class AdminConfig {
  // Name, Type, Host, Port
  public static Entry[] loadConfig(File file) throws IOException {

    // Place all lines into stack
    ArrayList entryList = new ArrayList();
    FileReader reader = null;
    BufferedReader fileReader = null;
    try {
      reader = new FileReader(file);
      fileReader = new BufferedReader(reader);
      // Read the first line.
      String line = fileReader.readLine();

      while (line != null) {
        line = line.trim();

        // Replace tabs with spaces
        line = line.replace('\t', ' ');

        // Skip all empty and comment lines
        if (line.length() != 0 && line.startsWith("#") == false) {
          try {
            entryList.add(new Entry(line));
          } catch (Exception ex) {
            // ignore - drop any lines that are not valid
          }
        }
        line = fileReader.readLine();
      }
    } finally {
      if (fileReader != null) {
        fileReader.close();
      }
      if (reader != null) {
        reader.close();
      }
    }

    return (Entry[]) entryList.toArray(new Entry[0]);
  }

  public static void storeConfig(File file, AdminConfig.Entry entries[]) throws IOException {
    FileOutputStream fos = null;
    PrintStream ps = null;
    try {
      fos = new FileOutputStream(file);
      ps = new PrintStream(fos);

      // Header
      ps.print("#");
      ps.println(
          LocalizedStrings.AdminConfig_THIS_FILE_IS_GENERATED_BY_ADMINCONSOLE_EDIT_AS_YOU_WISH_BUT_IT_WILL_BE_OVERWRITTEN_IF_IT_IS_MODIFIED_IN_ADMINCONSOLE
              .toLocalizedString());
      ps.println("#");
      ps.println(LocalizedStrings.AdminConfig_MODIFIED_0.toLocalizedString(new Date()));
      ps.println("#");
      ps.println("# Name, Type, Host, Port");
      ps.println("#");
      int len = entries.length;
      for (int i = 0; i < len; i++) {
        ps.println(entries[i].toString());
      }
      ps.flush();
    } finally {
      if (ps != null) {
        ps.close();
      }
      if (fos != null) {
        fos.close();
      }
    }
  }


  public static class Entry {
    public String name;
    public String type;
    public String host;
    public int port;

    public Entry(String line) {
      // Split
      String split[] = line.split(",");

      // Convert line to parameters
      name = split[0].trim();
      type = split[1].trim();
      host = split[2].trim();
      port = Integer.parseInt(split[3]);
    }

    public Entry(String name, String type, String host, int port) {
      this.name = name;
      this.type = type;
      this.host = host;
      this.port = port;
    }

    @Override // GemStoneAddition
    public String toString() {
      return name + "," + type + "," + host + "," + port;
    }
  }
}
