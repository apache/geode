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
package com.gemstone.gemfire.cache30;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * Test to make sure cache close is working.
 *
 * @author gregp
 * @since 6.5
 */
public class CacheLogRollDUnitTest extends CacheTestCase {

  public CacheLogRollDUnitTest(String name) {
    super(name);
  }

  //////////////////////  Test Methods  //////////////////////

  private void logAndRollAndVerify(String baseLogName,
      DistributedSystem ds,String mainId) throws FileNotFoundException, IOException {
    String logfile = baseLogName+".log";
    String metaLogFile = "meta-"+baseLogName+"-"+mainId+".log";
    String rolledLogFile1 = baseLogName+"-"+mainId+"-01.log";
    String rolledLogFile2 = baseLogName+"-"+mainId+"-02.log";
    String META_MARKER_1 = "Switching to log "+baseLogName+".log";
    String META_MARKER_2 = "Rolling current log to "+baseLogName+"-"+mainId+"-01.log";
    String META_MARKER_3 = "Rolling current log to "+baseLogName+"-"+mainId+"-02.log";

    String FIRST_CHILD_MARKER = "hey guys im the first child whatsup";
    String LOG_NONSENSE = "what is your story what are you doing hey whatsup i can't believe it wow";
    System.out.println("LOGNAME:"+logfile);
    /*
     * 1. Lets assert that the logfile exists and that it is a proper normal logfile
     * 2. Asser that the meta logfile exists and has good stuff in it
     * 3. Let's log a bunch and show that we rolled, 
     * 4. Show that old file has right old stuff in it
     * 5. Show that new file has right new stuff in it
     * 6. Show that meta has right stuff in it
     */
   
    ds.getLogWriter().info(FIRST_CHILD_MARKER);
    
    File f = new File(logfile);
    assertTrue("log-file :"+logfile+" should exist",f.exists());
    
    File fm = new File(metaLogFile);
    assertTrue("meta-log-file :"+metaLogFile+" should exist",fm.exists());
    
    File f1 = new File(rolledLogFile1);
    assertTrue("child-log-file :"+rolledLogFile1+" should'nt exist",!f1.exists());
    
    File f2 = new File(rolledLogFile2);
    assertTrue("child-log-file2 :"+rolledLogFile2+" should'nt exist yet",!f2.exists());
    
    
    String metalog = getLogContents(metaLogFile);
    assertTrue("metalog("+metaLogFile+") should have "+META_MARKER_1+" in it:\n"+metalog,metalog.indexOf(META_MARKER_1)!=-1);

    String mainlog = getLogContents(logfile);
    assertTrue("log("+logfile+") should have "+FIRST_CHILD_MARKER+" in it:\n"+mainlog,mainlog.indexOf(FIRST_CHILD_MARKER)!=-1);
    
    int i = 0;
    while(i<100000 && !f2.exists()) {
      i++;
      ds.getLogWriter().info(LOG_NONSENSE);
    }
    
    assertTrue("child-log-file1 :"+rolledLogFile1+" should exist now",f1.exists());
    assertTrue("child-log-file2 :"+rolledLogFile2+" should exist now",f2.exists());
    
    metalog = getLogContents(metaLogFile);

    assertTrue("log("+metaLogFile+") should have "+META_MARKER_2+" in it:\n"+metalog,metalog.indexOf(META_MARKER_2)!=-1);
    assertTrue("log("+metaLogFile+") should have "+META_MARKER_3+" in it:\n"+metalog,metalog.indexOf(META_MARKER_3)!=-1);
    assertTrue("log("+metaLogFile+") should'nt have "+LOG_NONSENSE+" in it:\n"+metalog,metalog.indexOf(LOG_NONSENSE)==-1);
    
    {
      String logChild2 = getLogContents(logfile);
      assertTrue("log("+logfile+") should have "+LOG_NONSENSE+" in it:\n"+logChild2,logChild2.indexOf(LOG_NONSENSE)!=-1);
    }
    
    {
      String logChild2 = getLogContents(rolledLogFile1);
      assertTrue("log("+rolledLogFile1+") should have "+LOG_NONSENSE+" in it:\n"+logChild2,logChild2.indexOf(LOG_NONSENSE)!=-1);
    }
    
    {
      String logChild2 = getLogContents(rolledLogFile2);
      assertTrue("log("+rolledLogFile2+") should have "+LOG_NONSENSE+" in it:\n"+logChild2,logChild2.indexOf(LOG_NONSENSE)!=-1);
    }
  }

  private void SecurityLogAndRollAndVerify(String baseLogName,
      DistributedSystem ds,String mainId) throws FileNotFoundException, IOException {
    String logfile = baseLogName+".log";
    String sec_logfile = "sec_"+logfile;
    String metaLogFile = "meta-"+baseLogName+"-"+mainId+".log";
    String rolledLogFile1 = baseLogName+"-"+mainId+"-01.log";
    String rolledLogFile2 = baseLogName+"-"+mainId+"-02.log";
    String rolledSecLogFile1 = "sec_"+baseLogName+"-"+mainId+"-01.log";
    String rolledSecLogFile2 = "sec_"+baseLogName+"-"+mainId+"-02.log";
    String META_MARKER_1 = "Switching to log "+baseLogName+".log";
    String META_MARKER_2 = "Rolling current log to "+baseLogName+"-"+mainId+"-01.log";
    String META_MARKER_3 = "Rolling current log to "+baseLogName+"-"+mainId+"-02.log";

    String FIRST_CHILD_MARKER = "hey guys im the first child whatsup";
    String LOG_NONSENSE = "what is your story what are you doing hey whatsup i can't believe it wow";
    System.out.println("LOGNAME:"+logfile+", SECLOGNAME:"+sec_logfile);
    /*
     * 1. Lets assert that the logfile exists and that it is a proper normal logfile
     * 2. Asser that the meta logfile exists and has good stuff in it
     * 3. Let's log a bunch and show that we rolled, 
     * 4. Show that old file has right old stuff in it
     * 5. Show that new file has right new stuff in it
     * 6. Show that meta has right stuff in it
     */
   
    ds.getLogWriter().info(FIRST_CHILD_MARKER);
    ds.getSecurityLogWriter().info(FIRST_CHILD_MARKER);
    
    File f = new File(logfile);
    File sec_f = new File(sec_logfile);
    assertTrue("log-file :"+logfile+" should exist",f.exists());
    assertTrue("security-log-file :"+sec_logfile+" should exist",sec_f.exists());
    
    File fm = new File(metaLogFile);
    assertTrue("meta-log-file :"+metaLogFile+" should exist",fm.exists());
    
    File f1 = new File(rolledLogFile1);
    File sec_f1 = new File(rolledSecLogFile1);
    assertTrue("child-log-file :"+rolledLogFile1+" should'nt exist",!f1.exists());
    assertTrue("security-child-log-file :"+rolledLogFile1+" should'nt exist",!sec_f1.exists());
    
    File f2 = new File(rolledLogFile2);
    File sec_f2 = new File(rolledSecLogFile2);
    assertTrue("child-log-file2 :"+rolledLogFile2+" should'nt exist yet",!f2.exists());
    assertTrue("security-child-log-file2 :"+rolledSecLogFile2+" should'nt exist yet",!sec_f2.exists());
    
    
    String metalog = getLogContents(metaLogFile);
    assertTrue("metalog("+metaLogFile+") should have "+META_MARKER_1+" in it:\n"+metalog,metalog.indexOf(META_MARKER_1)!=-1);

    String mainlog = getLogContents(logfile);
    assertTrue("log("+logfile+") should have "+FIRST_CHILD_MARKER+" in it:\n"+mainlog,mainlog.indexOf(FIRST_CHILD_MARKER)!=-1);
    String sec_mainlog = getLogContents(sec_logfile);
    assertTrue("log("+sec_logfile+") should have "+FIRST_CHILD_MARKER+" in it:\n"+sec_mainlog,sec_mainlog.indexOf(FIRST_CHILD_MARKER)!=-1);
    
    int i = 0;
    while(i<100000 && !f2.exists()) {
      i++;
      ds.getLogWriter().info(LOG_NONSENSE);
    }

    int j = 0;
    while(j<100000 && !sec_f2.exists()) {
      j++;
      ds.getSecurityLogWriter().info(LOG_NONSENSE);
    }

    assertTrue("child-log-file1 :"+rolledLogFile1+" should exist now",f1.exists());
    assertTrue("child-log-file2 :"+rolledLogFile2+" should exist now",f2.exists());
    assertTrue("security-child-log-file1 :"+rolledSecLogFile1+" should exist now",sec_f1.exists());
    assertTrue("security-child-log-file2 :"+rolledSecLogFile2+" should exist now",sec_f2.exists());
    
    metalog = getLogContents(metaLogFile);

    assertTrue("log("+metaLogFile+") should have "+META_MARKER_2+" in it:\n"+metalog,metalog.indexOf(META_MARKER_2)!=-1);
    assertTrue("log("+metaLogFile+") should have "+META_MARKER_3+" in it:\n"+metalog,metalog.indexOf(META_MARKER_3)!=-1);
    assertTrue("log("+metaLogFile+") should'nt have "+LOG_NONSENSE+" in it:\n"+metalog,metalog.indexOf(LOG_NONSENSE)==-1);
    
    {
      String logChild2 = getLogContents(logfile);
      assertTrue("log("+logfile+") should have "+LOG_NONSENSE+" in it:\n"+logChild2,logChild2.indexOf(LOG_NONSENSE)!=-1);
      String sec_logChild2 = getLogContents(sec_logfile);
      assertTrue("log("+sec_logfile+") should have "+LOG_NONSENSE+" in it:\n"+sec_logChild2,sec_logChild2.indexOf(LOG_NONSENSE)!=-1);
    }
    
    {
      String logChild2 = getLogContents(rolledLogFile1);
      assertTrue("log("+rolledLogFile1+") should have "+LOG_NONSENSE+" in it:\n"+logChild2,logChild2.indexOf(LOG_NONSENSE)!=-1);
      String sec_logChild2 = getLogContents(rolledSecLogFile1);
      assertTrue("log("+rolledSecLogFile1+") should have "+LOG_NONSENSE+" in it:\n"+sec_logChild2,sec_logChild2.indexOf(LOG_NONSENSE)!=-1);
    }
    
    {
      String logChild2 = getLogContents(rolledLogFile2);
      assertTrue("log("+rolledLogFile2+") should have "+LOG_NONSENSE+" in it:\n"+logChild2,logChild2.indexOf(LOG_NONSENSE)!=-1);
      String sec_logChild2 = getLogContents(rolledSecLogFile2);
      assertTrue("log("+rolledSecLogFile2+") should have "+LOG_NONSENSE+" in it:\n"+sec_logChild2,sec_logChild2.indexOf(LOG_NONSENSE)!=-1);
    }
  }

  public void testDiskSpace() throws Exception {
    Properties props = new Properties();
    String baseLogName = "diskarito";
    String logfile = baseLogName+".log";
    props.put("log-file", logfile);
    props.put("log-file-size-limit", "1");
    DistributedSystem ds = this.getSystem(props);
    props.put("log-disk-space-limit", "200");
    for(int i=0;i<10;i++) {
     ds = this.getSystem(props);
     ds.disconnect();
     }

     /*
      * This was throwing NPEs until my fix...
      */
  }
 
  public void testSimpleStartRestartWithRolling() throws Exception {
    Properties props = new Properties();
    String baseLogName = "restarto";
    String logfile = baseLogName+".log";
    props.put("log-file", logfile);
    props.put("log-file-size-limit", "1");
    props.put("log-disk-space-limit", "200");
    props.put("log-level", "config");
    InternalDistributedSystem ids = getSystem(props);
    assertEquals(InternalLogWriter.INFO_LEVEL, ((InternalLogWriter)ids.getLogWriter()).getLogWriterLevel());
    ids.disconnect();
    String mainId;
    {
      final Pattern mainIdPattern = Pattern.compile("meta-" + baseLogName + "-\\d\\d.log");
      File[] metaLogs = new File(".").listFiles(new FilenameFilter() {
          public boolean accept(File d, String name) {
            return mainIdPattern.matcher(name).matches();
          }
        });
      assertEquals(1, metaLogs.length);
      String f = metaLogs[0].getName();
      int idx = f.lastIndexOf("-");
      int idx2 = f.lastIndexOf(".");
      mainId = f.substring(idx+1, idx2);
    }
    String metaName = "meta-"+baseLogName+"-"+mainId+".log";
    File fm = new File(metaName);
     assertTrue("Ok so metalog:"+metaName+" better exist:",fm.exists());
    for(int i=1;i<10;i++) {
      int mainInt = Integer.parseInt(mainId)+(i);
      String myid;
      if(mainInt<10) {
         myid = "0"+mainInt;
      } else {
         myid = ""+mainInt;
      }
      String oldMain;
      if(mainInt<11) {
         oldMain = "0"+(mainInt-1);
      } else {
         oldMain = ""+(mainInt-1);
      }
      String lfold = "meta-"+baseLogName+"-"+(oldMain)+".log";
      File fold = new File(lfold);
      assertTrue("before we even get going here["+i+"] mainInt:"+mainInt+" myid:"+myid+" "+lfold+" should exist the metaname was :"+metaName +" and it should match that",fold.exists());
        String lf = "meta-"+baseLogName+"-"+myid+".log";
        String lfl = baseLogName+"-"+(oldMain)+"-01.log";
        File f1m = new File(lf);
        File f1l = new File(lfl);
        assertTrue(!f1m.exists());
        assertTrue(!f1l.exists());
        DistributedSystem ds = this.getSystem(props);
        assertTrue("We are hoping that:"+lf+" exists",f1m.exists());
        assertTrue("We are hoping that:"+lfl+" exists",f1l.exists());
        ds.disconnect();
    }

  }
  
  public void testStartWithRollingThenRestartWithRolling() throws Exception {
    Properties props = new Properties();
    String baseLogName = "biscuits";
    String logfile = baseLogName+".log";
    props.put("log-file", logfile);
    props.put("log-file-size-limit", "1");
    props.put("log-level", "config");
    DistributedSystem ds = getSystem(props);
    InternalDistributedSystem ids = (InternalDistributedSystem) ds;
    assertEquals(InternalLogWriter.INFO_LEVEL, ((InternalLogWriter)ids.getLogWriter()).getLogWriterLevel());

    // Lets figure out the mainId we start with
    String mainId;
    {
      final Pattern mainIdPattern = Pattern.compile("meta-" + baseLogName + "-\\d\\d\\d*.log");
      File[] metaLogs = new File(".").listFiles(new FilenameFilter() {
          public boolean accept(File d, String name) {
            return mainIdPattern.matcher(name).matches();
          }
        });
      assertEquals(1, metaLogs.length);
      String f = metaLogs[0].getName();
      int idx = f.lastIndexOf("-");
      int idx2 = f.lastIndexOf(".");
      mainId = f.substring(idx+1, idx2);
    }
    logAndRollAndVerify(baseLogName, ds, mainId);
    /*
     * Ok now we have rolled and yada yada. Let's disconnect and reconnect with same name!
     */
    int dsId = System.identityHashCode(ds);
    props.put("log-disk-space-limit", "200");
    
    File f1m = new File(logfile);
    assertTrue(f1m.exists());
    File f1c1 = new File(baseLogName+"-"+mainId+"-01.log");
    assertTrue(f1c1.exists());
    File f1c2 = new File(baseLogName+"-"+mainId+"-02.log");
    assertTrue(f1c2.exists());
    
    File f1c3 = new File(baseLogName+"-"+mainId+"-03.log");
    assertTrue(!f1c3.exists());

    String nextMainId;
    {
      int mId = Integer.parseInt(mainId);
      mId++;
      StringBuffer sb = new StringBuffer();
      if (mId < 10) {
        sb.append('0');
      }
      sb.append(mId);
      nextMainId = sb.toString();
    }
    File f2c1 = new File(baseLogName+"-"+nextMainId+"-01.log");
    assertTrue(!f2c1.exists());
    
    
    /*
     * Lets just make sure all the proper files exist
     */
    ds  = this.getSystem(props);
    int dsId2 = System.identityHashCode(ds);
    assertTrue("This should be a new ds!",dsId!=dsId2);
    /* creating the new system should have rolled the old rolling log (biscuits.log->biscuits-02-01.log)
     * 
     */
    // The following assert does not work on Windows because
    // we can't rename the last biscuits.log because it is still open
    // The DistributedSystem disconnect is not closing the logger enough
    // so that it can be renamed.
    // Reenable this assertion once this issue (bug 42176) is fixed.
    assertTrue(f1c3.exists());
  }
  
  public void testLogFileLayoutAndRolling() throws Exception {
    String baseLogName = "tacos";
      Properties props = new Properties();
      
      String logfile = baseLogName+".log";
      props.put("log-file", logfile);
      props.put("log-file-size-limit", "1");
      props.put("log-level", "config");
      
      DistributedSystem ds = getSystem(props);
      InternalDistributedSystem ids = (InternalDistributedSystem) ds;
      assertEquals(InternalLogWriter.INFO_LEVEL, ((InternalLogWriter)ids.getLogWriter()).getLogWriterLevel());

      // Lets figure out the mainId we start with
      String mainId;
      {
        final Pattern mainIdPattern = Pattern.compile("meta-" + baseLogName + "-\\d+.log");
        File[] metaLogs = new File(".").listFiles(new FilenameFilter() {
            public boolean accept(File d, String name) {
              return mainIdPattern.matcher(name).matches();
            }
          });
        assertEquals(1, metaLogs.length);
        String f = metaLogs[0].getName();
        int idx = f.lastIndexOf("-");
        int idx2 = f.lastIndexOf(".");
        mainId = f.substring(idx+1, idx2);
      }
      ds.getProperties();
      logAndRollAndVerify(baseLogName, ds, mainId);
      
  }

  public void testSecurityLogFileLayoutAndRolling() throws Exception {
    String baseLogName = "securitytacos";
      Properties props = new Properties();
      
      String logfile = baseLogName+".log";
      String sec_logfile = "sec_"+baseLogName+".log";
      props.put("log-file", logfile);
      props.put("log-file-size-limit", "1");
      props.put("log-level", "config");
      props.put("security-log-file", sec_logfile);
      props.put("security-log-level", "config");
      
      DistributedSystem ds = getSystem(props);
      InternalDistributedSystem ids = (InternalDistributedSystem) ds;
      assertEquals(InternalLogWriter.INFO_LEVEL, ((InternalLogWriter)ids.getLogWriter()).getLogWriterLevel());
      assertEquals(InternalLogWriter.INFO_LEVEL, ((InternalLogWriter)ids.getSecurityLogWriter()).getLogWriterLevel());

      // Lets figure out the mainId we start with
      String mainId;
      {
        final Pattern mainIdPattern = Pattern.compile("meta-" + baseLogName + "-\\d+.log");
        File[] metaLogs = new File(".").listFiles(new FilenameFilter() {
            public boolean accept(File d, String name) {
              return mainIdPattern.matcher(name).matches();
            }
          });
        assertEquals(1, metaLogs.length);
        String f = metaLogs[0].getName();
        int idx = f.lastIndexOf("-");
        int idx2 = f.lastIndexOf(".");
        mainId = f.substring(idx+1, idx2);
      }
      ds.getProperties();
      SecurityLogAndRollAndVerify(baseLogName, ds, mainId);
  }

  String getLogContents(String logfile) throws FileNotFoundException,IOException {
    File f = new File(logfile);
    BufferedReader reader = new BufferedReader(new FileReader(f));
    StringBuffer fileData = new StringBuffer();
    int numRead = 0;
    char[] buf = new char[1024];
    while((numRead = reader.read(buf)) !=-1) {
      String readData = String.valueOf(buf, 0,numRead);
      fileData.append(readData);
      buf = new char[1024];
    }
    return fileData.toString();
  }

}
