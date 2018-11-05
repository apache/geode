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
package org.apache.geode.distributed.internal.membership.gms.locator;

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.distributed.internal.membership.DistributedMembershipListener;
import org.apache.geode.distributed.internal.membership.MemberFactory;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class GMSLocatorRecoveryJUnitTest {

  private File tempStateFile;
  private GMSLocator locator;
  private File dir;

  @Before
  public void setUp() throws Exception {
    this.tempStateFile = new File("GMSLocatorJUnitTest_locator.dat");
    if (this.tempStateFile.exists()) {
      this.tempStateFile.delete();
    }
    this.locator = new GMSLocator(null, null, false, false, new LocatorStats(), "");
    locator.setViewFile(tempStateFile);
  }

  @After
  public void tearDown() throws Exception {
    if (this.tempStateFile.exists()) {
      this.tempStateFile.delete();
    }

    if (dir != null) {
      FileUtils.deleteQuietly(dir);
    }
  }

  private void populateStateFile(File file, int fileStamp, int ordinal, Object object)
      throws Exception {
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
      oos.writeInt(fileStamp);
      oos.writeInt(ordinal);
      DataSerializer.writeObject(object, oos);
      oos.flush();
    }
  }

  @Test
  public void testRecoverFromFileWithNonExistFile() throws Exception {
    this.tempStateFile.delete();
    assertFalse(this.tempStateFile.exists());
    assertFalse(this.locator.recoverFromFile(this.tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithNormalFile() throws Exception {
    NetView view = new NetView();
    populateStateFile(this.tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL,
        view);
    assertTrue(this.locator.recoverFromFile(this.tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithWrongFileStamp() throws Exception {
    // add 1 to file stamp to make it invalid
    populateStateFile(this.tempStateFile, GMSLocator.LOCATOR_FILE_STAMP + 1,
        Version.CURRENT_ORDINAL, 1);
    assertFalse(this.locator.recoverFromFile(this.tempStateFile));
  }

  @Test
  public void testRecoverFromFileWithWrongOrdinal() throws Exception {
    // add 1 to ordinal to make it wrong
    populateStateFile(this.tempStateFile, GMSLocator.LOCATOR_FILE_STAMP,
        Version.CURRENT_ORDINAL + 1, 1);
    try {
      this.locator.recoverFromFile(this.tempStateFile);
      fail("expected an InternalGemFireException to be thrown");
    } catch (InternalGemFireException e) {
      // success
    }
  }

  @Test
  public void testRecoverFromFileWithInvalidViewObject() throws Exception {
    populateStateFile(this.tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL,
        1);
    try {
      this.locator.recoverFromFile(this.tempStateFile);
      fail("should catch InternalGemFileException");
    } catch (InternalGemFireException e) {
      assertTrue(e.getMessage().startsWith("Unable to recover previous membership view from"));
    }
  }

  @Test
  public void testRecoverFromOther() throws Exception {

    MembershipManager m1 = null, m2 = null;
    Locator l = null;

    try {

      // boot up a locator
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      InetAddress localHost = SocketCreator.getLocalHost();

      // this locator will hook itself up with the first MembershipManager
      // to be created
      // l = Locator.startLocator(port, new File(""), localHost);
      l = InternalLocator.startLocator(port, new File(""), null, null, localHost, false,
          new Properties(), null);

      // create configuration objects
      Properties nonDefault = new Properties();
      nonDefault.put(DISABLE_TCP, "true");
      nonDefault.put(MCAST_PORT, "0");
      nonDefault.put(LOG_FILE, "");
      nonDefault.put(LOG_LEVEL, "fine");
      nonDefault.put(LOCATORS, localHost.getHostAddress() + '[' + port + ']');
      nonDefault.put(BIND_ADDRESS, localHost.getHostAddress());
      DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
      RemoteTransportConfig transport =
          new RemoteTransportConfig(config, ClusterDistributionManager.NORMAL_DM_TYPE);

      // start the first membership manager
      DistributedMembershipListener listener1 = mock(DistributedMembershipListener.class);
      DMStats stats1 = mock(DMStats.class);
      m1 = MemberFactory.newMembershipManager(listener1, config, transport, stats1,
          SecurityServiceFactory.create());

      // hook up the locator to the membership manager
      ((InternalLocator) l).getLocatorHandler().setMembershipManager(m1);

      GMSLocator l2 = new GMSLocator(SocketCreator.getLocalHost(),
          m1.getLocalMember().getHost() + "[" + port + "]", true, true, new LocatorStats(), "");
      l2.setViewFile(new File("l2.dat"));
      l2.init(null);

      assertTrue("expected view to contain " + m1.getLocalMember() + ": " + l2.getMembers(),
          l2.getMembers().contains(m1.getLocalMember()));
    } finally {
      if (m1 != null) {
        m1.disconnect(false);
      }
      if (l != null) {
        l.stop();
      }
    }
  }

  @Test
  public void testViewFileNotFound() throws Exception {
    NetView view = new NetView();
    populateStateFile(this.tempStateFile, GMSLocator.LOCATOR_FILE_STAMP, Version.CURRENT_ORDINAL,
        view);
    assertTrue(this.tempStateFile.exists());

    dir = new File("testViewFileFoundWhenUserDirModified");
    dir.mkdir();
    File viewFileInNewDirectory = new File(dir, tempStateFile.getName());

    assertFalse(viewFileInNewDirectory.exists());
    File locatorViewFile = locator.setViewFile(viewFileInNewDirectory);
    assertFalse(locator.recoverFromFile(locatorViewFile));
  }


}
