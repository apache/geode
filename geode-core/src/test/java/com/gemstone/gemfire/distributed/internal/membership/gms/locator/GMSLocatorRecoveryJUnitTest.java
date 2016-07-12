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
package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.MemberFactory;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@Category(IntegrationTest.class)
public class GMSLocatorRecoveryJUnitTest {

  File tempStateFile = null;
  GMSLocator locator = null;

  @Before
  public void setUp() throws Exception {
    tempStateFile = new File("GMSLocatorJUnitTest_locator.dat");
    if (tempStateFile.exists()) {
      tempStateFile.delete();
    }
    locator = new GMSLocator(null, tempStateFile, null, false, false, new LocatorStats());
    // System.out.println("temp state file: " + tempStateFile);
  }

  @After
  public void tearDown() throws Exception {
    if (tempStateFile.exists()) {
      tempStateFile.delete();
    }
  }

  private void populateStateFile(File file, int fileStamp, int ordinal, Object object) throws Exception {
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
      oos.writeInt(fileStamp);
      oos.writeInt(ordinal);
      DataSerializer.writeObject(object, oos);
      oos.flush();
    }
  }

  @Test
  public void testRecoverFromFileWithNonExistFile() throws Exception {
    tempStateFile.delete();
    assertFalse(tempStateFile.exists());
    assertFalse(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithNormalFile() throws Exception {
    NetView view = new NetView();
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL, view);
    assertTrue(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithWrongFileStamp() throws Exception {
    // add 1 to file stamp to make it invalid
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP + 1, Version.CURRENT_ORDINAL, 1);
    assertFalse(locator.recoverFromFile(tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithWrongOrdinal() throws Exception {
    // add 1 to ordinal to make it wrong
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL + 1, 1);
    try {
      locator.recoverFromFile(tempStateFile);
      fail("expected an InternalGemFireException to be thrown");
    } catch (InternalGemFireException e) {
      // success
    }
  }

  @Test
  public void testRecoverFromFileWithInvalidViewObject() throws Exception {
    populateStateFile(tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL, 1);
    try {
      locator.recoverFromFile(tempStateFile);
      fail("should catch InternalGemFileException");
    } catch (InternalGemFireException e) {
      assertTrue(e.getMessage().startsWith("Unable to recover previous membership view from"));
    }
  }

  @Test
  public void testRecoverFromOther() throws Exception {
    
    MembershipManager m1=null, m2=null;
    Locator l = null;
    
    try {
      
      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();
      
      // this locator will hook itself up with the first MembershipManager
      // to be created
//      l = Locator.startLocator(port, new File(""), localHost);
      l = InternalLocator.startLocator(port, new File(""), null,
          null, null, localHost, false, new Properties(), true, false, null,
          false);
      
      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DISABLE_TCP, "true");
      nonDefault.put(MCAST_PORT, "0");
      nonDefault.put(LOG_FILE, "");
      nonDefault.put(LOG_LEVEL, "fine");
      nonDefault.put(LOCATORS, localHost.getHostAddress() + '[' + port + ']');
      nonDefault.put(BIND_ADDRESS, localHost.getHostAddress());
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport = new RemoteTransportConfig(config,
          DistributionManager.NORMAL_DM_TYPE);

      // start the first membership manager
      DistributedMembershipListener listener1 = mock(DistributedMembershipListener.class);
      DMStats stats1 = mock(DMStats.class);
      m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1);
      
      // hook up the locator to the membership manager
      ((InternalLocator)l).getLocatorHandler().setMembershipManager(m1);
      
      GMSLocator l2 = new GMSLocator(SocketCreator.getLocalHost(), new File("l2.dat"),
          m1.getLocalMember().getHost()+"["+port+"]", true, true, new LocatorStats());
      l2.init(null);
      
      assertTrue("expected view to contain "
          + m1.getLocalMember() + ": " + l2.getMembers(),
          l2.getMembers().contains(m1.getLocalMember()));
    } finally {
      if (m1 != null) {
        m1.shutdown();
      }
      if (l != null) {
        l.stop();
      }
    }
  }
}

