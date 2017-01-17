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
