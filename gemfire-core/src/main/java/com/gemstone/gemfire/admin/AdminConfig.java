/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;


/**
 * AdminConfig loads/stores the member information list. The list contains
 * all of the members being monitored.
 *
 * Config must be of the format:
 * <p>
 * <li> Name=What you want displayed as a name for the instance
 * <li> Type=SERVER|CLIENT
 * <li> Host=A valid hostname or IP Address where the instance is
 * running
 * <li> Port=The port you are using to open the monitor port for
 * the instance
 * @author dpark
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class AdminConfig
{
  // Name, Type, Host, Port
  public static Entry[] loadConfig(File file) throws IOException
  {

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
    }
    finally {
      if (fileReader != null) {
        fileReader.close();
      }
      if (reader != null) {
        reader.close();
      }
    }

    return (Entry[])entryList.toArray(new Entry[0]);
  }

  public static void storeConfig(File file, AdminConfig.Entry entries[]) throws IOException
  {
    FileOutputStream fos = null;
    PrintStream ps = null;
    try {
      fos = new FileOutputStream(file);
      ps = new PrintStream(fos);
  
      // Header
      ps.print("#");
      ps.println(LocalizedStrings.AdminConfig_THIS_FILE_IS_GENERATED_BY_ADMINCONSOLE_EDIT_AS_YOU_WISH_BUT_IT_WILL_BE_OVERWRITTEN_IF_IT_IS_MODIFIED_IN_ADMINCONSOLE.toLocalizedString());
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
    }
    finally {
      if (ps != null) {
        ps.close();
      }
      if (fos != null) {
        fos.close();
      }
    }
  }


  public static class Entry
  {
    public String name;
    public String type;
    public String host;
    public int port;

    public Entry(String line)
    {
            // Split
            String split[] = line.split(",");

            // Convert line to parameters
            name = split[0].trim();
            type = split[1].trim();
            host = split[2].trim();
            port = Integer.parseInt(split[3]);
    }

    public Entry(String name, String type, String host, int port)
    {
      this.name = name;
      this.type = type;
      this.host = host;
      this.port = port;
    }

    @Override // GemStoneAddition
    public String toString()
    {
      return name + "," + type + "," + host + "," + port;
    }
  }
}
