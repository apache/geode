/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import com.gemstone.gemfire.internal.GemFireVersion;

public class Banner {

  private static String VERSION = "unknown";

  private static Properties props = new Properties();

  static {
    InputStream is = Banner.class.getResourceAsStream("/modules-version.properties");
    try {
      props.load(is);
      VERSION = props.getProperty("version");
    } catch (IOException e) {
    }
  }
	
  public static String getString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    print(pw);
    pw.close();
    return sw.toString();
  }
  
  private static void print(PrintWriter pw) {
    pw.println("GemFire Modules");
    pw.print("Modules version: ");
    pw.println(VERSION);
    GemFireVersion.print(pw);
  }
  
  private Banner() {}
}
