/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.*;
import java.io.*;
import java.text.SimpleDateFormat;
import org.apache.geode.*;
import org.apache.geode.internal.*;

public class LatestProp {
  public static void main(String[] args){
    System.out.println("build.date=" + GemFireVersion.getBuildDate());
    System.out.println("build.jdk=" + GemFireVersion.getBuildJavaVersion());
    System.out.println("build.version=" + GemFireVersion.getBuildJavaVersion());
    try{
      String date= GemFireVersion.getSourceDate();
      SimpleDateFormat sdf = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
      Date parsedDate = sdf.parse(date);
      sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.SSS");
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      System.out.println("source.date=" + dateFormat.format(parsedDate));
    }
    catch(Exception e){
      System.out.println("Exception is ::" + e);
    }
    System.out.println("source.repository=" + GemFireVersion.getSourceRepository());
    System.out.println("source.revision=" + GemFireVersion.getSourceRevision());
    System.out.println("runtime.jdk=" + System.getProperty("java.version"));
  }
}
