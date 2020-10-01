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
package org.apache.geode.internal.sequencelog.io;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.internal.sequencelog.GraphType;
import org.apache.geode.internal.sequencelog.Transition;

/**
 * A utility to convert existing gemfire logs into a graph logger file. This will help me debug
 * hydra runs more quickly.
 *
 */
public class GemfireLogConverter {

  private static final Pattern DATE_PATTERN =
      Pattern.compile("(\\d\\d\\d\\d)/(\\d\\d)/(\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d).(\\d\\d\\d)");
  private static final Pattern ALL = Pattern.compile(".*");
  @Immutable
  private static final ArrayList<Test> tests = buildTests();


  public static void convertFiles(OutputStream output, File[] files) throws IOException {
    Context context = new Context();
    context.appender = new OutputStreamAppender(output);
    for (File file : files) {
      context.currentMember = null;
      BufferedReader reader = new BufferedReader(new FileReader(file));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          for (Test test : tests) {
            if (test.apply(context, line)) {
              // go to the next line
              break;
            }
          }
        }
      } finally {
        reader.close();
      }
    }
  }

  public static void main(String[] args) throws IOException {

    if (args.length == 0) {
      usage();
      ExitCode.FATAL.doSystemExit();
    }

    File outputFile = new File(args[0]);

    File[] files = new File[args.length - 1];
    for (int i = 1; i < args.length; i++) {
      String fileName = args[i];
      File file = new File(fileName);
      files[i - 1] = file;
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
        BufferedOutputStream output = new BufferedOutputStream(fileOutputStream)) {
      convertFiles(output, files);
    }
  }

  private static ArrayList<Test> buildTests() {
    ArrayList<Test> tests = new ArrayList<Test>();

    // Membership level events
    // [info 2011/04/27 00:43:31.020 PDT dataStoregemfire5_hs20e_16643
    // <vm_5_thr_11_dataStore5_hs20e_16643> tid=0x17] Starting DistributionManager
    // hs20e(16643)<v1>:6872/49575.
    tests.add(new Test("Starting DistributionManager (.*)\\.") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String member = matcher.group(1);
        context.currentMember = member;
        context.appender.write(new Transition(timestamp, GraphType.MEMBER, "member", "start",
            "running", member, member));
      }
    });

    // [info 2011/04/27 00:49:39.254 PDT dataStoregemfire5_hs20e_16643 <View Message Processor>
    // tid=0x71] Member at hs20e(16640)<v1>:50554/49570 unexpectedly left the distributed cache:
    // departed JGroups view
    tests.add(new Test("Member at (.*) unexpectedly left the distributed cache") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String member = matcher.group(1);
        context.appender.write(new Transition(timestamp, GraphType.MEMBER, "member", "crashed",
            "destroyed", member, member));
        context.appender.write(
            new Transition(timestamp, GraphType.REGION, ALL, "crash", "destroyed", member, member));
        context.appender.write(
            new Transition(timestamp, GraphType.KEY, ALL, "crash", "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.MESSAGE, ALL, "crash",
            "destroyed", member, member));
      }
    });

    // [info 2011/04/27 00:49:33.154 PDT dataStoregemfire5_hs20e_16643 <Distributed system shutdown
    // hook> tid=0x18] VM is exiting - shutting down distributed system
    tests.add(new Test("VM is exiting - shutting down distributed system") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String member = context.currentMember;
        context.appender.write(new Transition(timestamp, GraphType.MEMBER, "member", "stop",
            "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.REGION, ALL, "destroy",
            "destroyed", member, member));
        context.appender.write(
            new Transition(timestamp, GraphType.KEY, ALL, "destroy", "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.MESSAGE, ALL, "destroy",
            "destroyed", member, member));
      }
    });
    // [info 2011/04/27 00:49:33.104 PDT dataStoregemfire3_hs20e_16635
    // <vm_3_thr_6_dataStore3_hs20e_16635> tid=0xaf] GemFireCache[id = 746885; isClosing = true;
    // created = Wed Apr 27 00:43:41 PDT 2011; server = false; copyOnRead = false; lockLease = 120;
    // lockTimeout = 60]: Now closing.
    tests.add(new Test("GemFireCache .*Now closing") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String member = context.currentMember;
        context.appender.write(new Transition(timestamp, GraphType.MEMBER, "member", "stop",
            "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.REGION, ALL, "destroy",
            "destroyed", member, member));
        context.appender.write(
            new Transition(timestamp, GraphType.KEY, ALL, "destroy", "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.MESSAGE, ALL, "destroy",
            "destroyed", member, member));
      }
    });
    // [info 2011/04/27 00:49:45.152 PDT dataStoregemfire3_hs20e_16635
    // <vm_3_thr_6_dataStore3_hs20e_16635> tid=0xaf] Shutting down DistributionManager
    // hs20e(16635)<v1>:54340/49574.
    tests.add(new Test("Shutting down DistributionManager") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String member = context.currentMember;
        context.appender.write(new Transition(timestamp, GraphType.MEMBER, "member", "stop",
            "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.REGION, ALL, "destroy",
            "destroyed", member, member));
        context.appender.write(
            new Transition(timestamp, GraphType.KEY, ALL, "destroy", "destroyed", member, member));
        context.appender.write(new Transition(timestamp, GraphType.MESSAGE, ALL, "destroy",
            "destroyed", member, member));
      }
    });



    // Region level events
    // [info 2011/04/27 00:44:39.811 PDT dataStoregemfire5_hs20e_16643
    // <vm_5_thr_11_dataStore5_hs20e_16643> tid=0x17] initializing region __PR
    tests.add(new Test("initializing region (.*)") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String region = matcher.group(1);
        context.appender.write(new Transition(timestamp, GraphType.REGION, region, "create",
            "created", context.currentMember, context.currentMember));
      }
    });
    // [info 2011/05/09 00:12:19.789 GMT+05:30 peergemfire_2_1_pcc40_11040
    // <vm_6_thr_10_peer_2_1_pcc40_11040> tid=0x18] Partitioned Region /Region_GlobalVillage is
    // created with prId=1
    tests.add(new Test("Partitioned Region " + SEPARATOR + "(.*) is created with") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String region = matcher.group(1);
        context.appender.write(new Transition(timestamp, GraphType.REGION, region, "create",
            "created", context.currentMember, context.currentMember));
      }
    });
    // [info 2011/04/27 00:48:31.077 PDT dataStoregemfire5_hs20e_16643 <Pooled Waiting Message
    // Processor 1> tid=0x8b] Region _B__partitionedRegion_1 requesting initial image from
    // hs20e(16650)<v1>:13093/49571
    tests.add(new Test(" Region (.*) requesting initial image from (.*)") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String region = matcher.group(1);
        String source = matcher.group(2);
        context.appender.write(new Transition(timestamp, GraphType.REGION, region, "GII", "created",
            source, context.currentMember));
      }
    });
    // [info 2011/04/27 00:48:31.088 PDT dataStoregemfire5_hs20e_16643 <Pooled Waiting Message
    // Processor 1> tid=0x8b] Region _B__partitionedRegion_1 initialized persistent id:
    // hs20e.gemstone.com/10.138.45.5:/export/hs20e2/users/lynn/class_versioning_dev_Mar11_5/parRegPdxInstance/2011-04-26-14-46-03/serialParRegHAPersistPdx-0427-003656/vm_5_dataStore5_disk_1
    // created at timestamp 1303890276172 version 0 diskStoreId af159596-d932-4f69-8f78-b8914f88fab1
    // with data from hs20e(16650)<v1>:13093/49571.
    tests.add(
        new Test(" Region (.*) initialized persistent id: .* diskStoreId (.*) with data from ") {
          @Override
          public void process(Context context, long timestamp, Matcher matcher) throws IOException {
            String region = matcher.group(1);
            String store = matcher.group(2);
            context.appender.write(new Transition(timestamp, GraphType.REGION, region, "persist",
                "persisted", context.currentMember, store));
          }
        });

    // [info 2011/04/27 00:48:31.243 PDT dataStoregemfire5_hs20e_16643 <Pooled Waiting Message
    // Processor 1> tid=0x8b] Region /__PR/_B__partitionedRegion_2 was created on this member with
    // the persistent id
    // hs20e.gemstone.com/10.138.45.5:/export/hs20e2/users/lynn/class_versioning_dev_Mar11_5/parRegPdxInstance/2011-04-26-14-46-03/serialParRegHAPersistPdx-0427-003656/vm_5_dataStore5_disk_1
    // created at timestamp 1303890276172 version 0 diskStoreId
    // af159596-d932-4f69-8f78-b8914f88fab1.
    // TODO, need to clean this up. We're logging full paths with this message, but only region
    // name with other messages.
    tests.add(new Test(
        " Region " + SEPARATOR + "__PR" + SEPARATOR
            + "(.*) was created on this member with the persistent id .* diskStoreId (.*)\\.") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String region = matcher.group(1);
        String store = matcher.group(2);
        context.appender.write(new Transition(timestamp, GraphType.REGION, region, "persist",
            "persisted", context.currentMember, store));
      }
    });

    // [info 2011/04/27 00:54:04.413 PDT dataStoregemfire3_hs20e_19190 <Recovery thread for bucket
    // _B__partitionedRegion_25> tid=0x7b] Region /__PR/_B__partitionedRegion_25 recovered from the
    // local disk. Old persistent ID:
    // /10.138.45.5:/export/hs20e2/users/lynn/class_versioning_dev_Mar11_5/parRegPdxInstance/2011-04-26-14-46-03/serialParRegHAPersistPdx-0427-003656/vm_3_dataStore3_disk_1
    // created at timestamp 1303890276172 version 0 diskStoreId
    // 0b21e233-e1a9-4b2e-9190-13a191219760, new persistent ID
    // hs20e.gemstone.com/10.138.45.5:/export/hs20e2/users/lynn/class_versioning_dev_Mar11_5/parRegPdxInstance/2011-04-26-14-46-03/serialParRegHAPersistPdx-0427-003656/vm_3_dataStore3_disk_1
    // created at timestamp 1303890815750 version 0 diskStoreId 0b21e233-e1a9-4b2e-9190-13a191219760
    tests.add(new Test(
        " Region " + SEPARATOR + "__PR" + SEPARATOR
            + "(.*) recovered from the local disk. .* new persistent ID.*diskStoreId (.*)") {
      @Override
      public void process(Context context, long timestamp, Matcher matcher) throws IOException {
        String region = matcher.group(1);
        String store = matcher.group(2);
        context.appender.write(new Transition(timestamp, GraphType.REGION, region, "recover",
            "created", store, context.currentMember));
      }
    });

    return tests;
  }

  private static long extractDate(String line) {
    Matcher matcher = DATE_PATTERN.matcher(line);
    if (!matcher.find()) {
      throw new IllegalStateException("Could not parse timestamp from line " + line);
    }
    matcher.start();
    final DateFormat formatter = DateFormatter.createDateFormat();
    Date date = formatter.parse(line, new ParsePosition(matcher.start()));
    return date.getTime();
  }

  private static void usage() {
    System.err.println(
        "Usage java org.apache.sequence.logconversion.ConvertGemfireLogs outputfile [log_file]+");

  }

  private abstract static class Test {
    private Pattern pattern;

    public Test(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }

    public boolean apply(Context context, String line) throws IOException {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        long timestamp = extractDate(line);
        process(context, timestamp, matcher);
        return true;
      }
      return false;
    }

    public abstract void process(Context context, long timestamp, Matcher matcher)
        throws IOException;
  }

  private static class Context {
    private OutputStreamAppender appender;
    private String currentMember;
  }
}
