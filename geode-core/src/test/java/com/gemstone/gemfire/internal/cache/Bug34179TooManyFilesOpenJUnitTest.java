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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Disk region perf test for Persist only with Async writes and  Buffer.
 * Set Rolling oplog to true and setMaxOplogSize to 10240
 * 
 * If more than some number of files are open, an Exception is thrown. This ia JDK 1.4 bug. This
 * test should be run after transition to JDK 1.5 to verify that the bug does not exceed.
 * 
 * The disk properties will ensure that very many oplog files are created.
 * 
 * This test is currently not being executed and is marked with an underscore
 */
@Category(IntegrationTest.class)
public class Bug34179TooManyFilesOpenJUnitTest extends DiskRegionTestingBase {

  private static int ENTRY_SIZE = 1024;

  private static int OP_COUNT = 100000;

  private LogWriter log = null;

  private DiskRegionProperties diskProps = new DiskRegionProperties();
 
  @Override
  protected final void postSetUp() throws Exception {
    File file1 = new File("testingDirectory/" + getName()+ "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs = new File[1];
    dirs[0] = file1;
    diskProps.setDiskDirs(dirs);
     
    diskProps.setPersistBackup(true);
    diskProps.setTimeInterval(15000l);
    diskProps.setBytesThreshold(10000l);
    diskProps.setRolling(true);
    //set max oplog size as 10 kb
    diskProps.setMaxOplogSize(10240);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,diskProps);
    
    log = ds.getLogWriter();
  }

  /**
   * currently not being executed for congo but after transition to JDK 1.5, this test should be executed.
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testPopulate1kbwrites() {
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);
    for (int i = 0; i < OP_COUNT; i++) {
      region.put(new Integer(i), value);
    }
    closeDown(); // closes disk file which will flush all buffers
  }

  /**
   * cleans all the directory of all the files present in them
   */
  protected static void deleteFiles() {
    for (int i = 0; i < dirs.length; i++) {
      File[] files = dirs[i].listFiles();
      for (int j = 0; j < files.length; j++) {
        files[j].delete();
      }
    }
  }
  
}// end of Bug34179TooManyFilesOpenJUnitTest
