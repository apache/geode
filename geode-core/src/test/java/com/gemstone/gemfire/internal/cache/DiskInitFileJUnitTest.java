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

import junit.framework.TestCase;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.experimental.categories.Category;

import java.util.Collections;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DiskInitFileJUnitTest extends TestCase {
  
  private File testDirectory;
  private Mockery context = new Mockery() {{
    setImposteriser(ClassImposteriser.INSTANCE);
  }};
  
  public void setUp() throws Exception {
    testDirectory = new File("_DiskInitFileJUnitTest");
    FileUtil.delete(testDirectory);
    FileUtil.mkdirs(testDirectory);
  }
  
  public void tearDown() throws Exception {
    FileUtil.delete(testDirectory);
  }
  
  /**
   * Test the behavior of canonical ids in the init file.
   */
  public void testCanonicalIds() {
    //create a mock statistics factory for creating directory holders
    final StatisticsFactory sf = context.mock(StatisticsFactory.class);
    context.checking(new Expectations() {{
      ignoring(sf);
    }});

    //Create a mock disk store impl. All we need to do is return
    //this init file directory.
    final DiskStoreImpl parent = context.mock(DiskStoreImpl.class);
    context.checking(new Expectations() {{
      allowing(parent).getInfoFileDir();
      will(returnValue(new DirectoryHolder(sf, testDirectory, 0, 0)));
      ignoring(parent);
    }});
    
    //Create an init file and add some canonical ids
    DiskInitFile dif = new DiskInitFile("testFile", parent, false, Collections.<File>emptySet());
    assertEquals(null, dif.getCanonicalObject(5));
    assertNull(dif.getCanonicalObject(0));
    int id1 = dif.getOrCreateCanonicalId("object1");
    int id2 = dif.getOrCreateCanonicalId("object2");
    
    assertEquals("object1", dif.getCanonicalObject(id1));
    assertEquals("object2", dif.getCanonicalObject(id2));
    assertEquals(id2, dif.getOrCreateCanonicalId("object2"));
    
    
    //Add a mock region to the init file so it doesn't
    //delete the file when the init file is closed
    final DiskRegionView drv = context.mock(DiskRegionView.class);
    context.checking(new Expectations() {{
      ignoring(drv);
    }});
    dif.createRegion(drv);
    
    //close the init file
    dif.close();
    
    //recover the init file from disk
    dif = new DiskInitFile("testFile", parent, true, Collections.<File>emptySet());
    
    //make sure we can recover the ids from disk
    assertEquals("object1", dif.getCanonicalObject(id1));
    assertEquals("object2", dif.getCanonicalObject(id2));
    assertEquals(id2, dif.getOrCreateCanonicalId("object2"));
    
    //Make sure we can add new ids
    int id3 = dif.getOrCreateCanonicalId("object3");
    assertTrue(id3 > id2);
    assertEquals("object1", dif.getCanonicalObject(id1));
    assertEquals("object2", dif.getCanonicalObject(id2));
    assertEquals("object3", dif.getCanonicalObject(id3));
    
    dif.close();
  }

}
