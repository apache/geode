package com.gemstone.gemfire.distributed.internal.membership.gms.membership;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Authenticator;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Manager;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Messenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.InstallViewMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinResponseMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.ViewAckMessage;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GMSJoinLeaveJUnitTest {
  private Services services;
  private ServiceConfig mockConfig;
  private DistributionConfig mockDistConfig;
  private Authenticator authenticator;
  private InternalDistributedMember gmsJoinLeaveMemberId;
  private InternalDistributedMember[] mockMembers;
  private InternalDistributedMember mockOldMember;
  private Object credentials = new Object();
  private Messenger messenger;
  private GMSJoinLeave gmsJoinLeave;
  
  public void initMocks() throws IOException {
    initMocks(false);
  }
  
  public void initMocks(boolean enableNetworkPartition) throws UnknownHostException {
    mockDistConfig = mock(DistributionConfig.class);
    when(mockDistConfig.getEnableNetworkPartitionDetection()).thenReturn(enableNetworkPartition);
    when(mockDistConfig.getLocators()).thenReturn("localhost[8888]");
    mockConfig = mock(ServiceConfig.class);
    when(mockConfig.getDistributionConfig()).thenReturn(mockDistConfig);
    when(mockDistConfig.getLocators()).thenReturn("localhost[12345]");
    when(mockDistConfig.getMcastPort()).thenReturn(0);
    
    authenticator = mock(Authenticator.class);
    gmsJoinLeaveMemberId = new InternalDistributedMember("localhost", 8887);
    
    messenger = mock(Messenger.class);
    when(messenger.getMemberID()).thenReturn(gmsJoinLeaveMemberId);
    
    services = mock(Services.class);
    when(services.getConfig()).thenReturn(mockConfig);
    when(services.getMessenger()).thenReturn(messenger);

    mockMembers = new InternalDistributedMember[4];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
    }
    mockOldMember = new InternalDistributedMember("localhost", 8700, Version.GFE_56);

    gmsJoinLeave = new GMSJoinLeave();
    gmsJoinLeave.init(services);
    gmsJoinLeave.start();
    gmsJoinLeave.started();
  }
  
  @Test
  public void testProcessJoinMessageRejectOldMemberVersion() throws IOException {
    initMocks();
 
    gmsJoinLeave.processMessage(new JoinRequestMessage(mockOldMember, mockOldMember, null));
    Assert.assertTrue("JoinRequest should not have been added to view request", gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(any(JoinResponseMessage.class));
  }
  
  @Test
  public void testProcessJoinMessageWithBadAuthentication() throws IOException {
    initMocks();
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(mockMembers[0], credentials)).thenThrow(new AuthenticationFailedException("we want to fail auth here"));
    when(services.getMessenger()).thenReturn(messenger);
         
    gmsJoinLeave.processMessage(new JoinRequestMessage(mockMembers[0], mockMembers[0], credentials));
    Assert.assertTrue("JoinRequest should not have been added to view request", gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(any(JoinResponseMessage.class));
  }
  
  @Test
  public void testProcessJoinMessageWithAuthenticationButNullCredentials() throws IOException {
    initMocks();
    when(services.getAuthenticator()).thenReturn(authenticator);
    when(authenticator.authenticate(mockMembers[0], null)).thenThrow(new AuthenticationFailedException("we want to fail auth here"));
    when(services.getMessenger()).thenReturn(messenger);
      
    gmsJoinLeave.processMessage(new JoinRequestMessage(mockMembers[0], mockMembers[0], null));
    Assert.assertTrue("JoinRequest should not have been added to view request", gmsJoinLeave.getViewRequests().size() == 0);
    verify(messenger).send(any(JoinResponseMessage.class));
  }
  
  
  private void prepareView() throws IOException {
    int viewId = 1;
    List<InternalDistributedMember> mbrs = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> shutdowns = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> crashes = new LinkedList<InternalDistributedMember>();
    mbrs.add(mockMembers[0]);
    
    when(services.getMessenger()).thenReturn(messenger);
    
    //prepare the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs, shutdowns, crashes);
    InstallViewMessage installViewMessage = new InstallViewMessage(netView, credentials, true);
    gmsJoinLeave.processMessage(installViewMessage);
    verify(messenger).send(any(ViewAckMessage.class));
  }
  
  private void prepareAndInstallView() throws IOException {
    int viewId = 1;
    List<InternalDistributedMember> mbrs = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> shutdowns = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> crashes = new LinkedList<InternalDistributedMember>();
    mbrs.add(mockMembers[0]);
    
    when(services.getMessenger()).thenReturn(messenger);
    
    //prepare the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs, shutdowns, crashes);
    InstallViewMessage installViewMessage = new InstallViewMessage(netView, credentials, true);
    gmsJoinLeave.processMessage(installViewMessage);
    verify(messenger).send(any(ViewAckMessage.class));
    
    //install the view
    installViewMessage = new InstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    Assert.assertEquals(netView, gmsJoinLeave.getView());
  }
  
  @Test
  public void testRemoveMember() throws Exception {
    initMocks();
    prepareAndInstallView();
    MethodExecuted removeMessageSent = new MethodExecuted();
    when(messenger.send(any(RemoveMemberMessage.class))).thenAnswer(removeMessageSent);
    gmsJoinLeave.remove(mockMembers[1], "removing for test");
    assert removeMessageSent.methodExecuted;
  }
  
  
  @Test 
  public void testRejectOlderView() throws IOException {
    initMocks();
    prepareAndInstallView();
    
    List<InternalDistributedMember> mbrs = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> shutdowns = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> crashes = new LinkedList<InternalDistributedMember>();
    mbrs.add(mockMembers[0]);
    mbrs.add(mockMembers[1]);
  
    //try to install an older view where viewId < currentView.viewId
    NetView olderNetView = new NetView(mockMembers[0], 0, mbrs, shutdowns, crashes);
    InstallViewMessage installViewMessage = new InstallViewMessage(olderNetView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    Assert.assertNotEquals(gmsJoinLeave.getView(), olderNetView);
  }
  
  @Test 
  public void testForceDisconnectedFromNewView() throws IOException {
    initMocks(true);//enabledNetworkPartition;
    Manager mockManager = mock(Manager.class);
    when(services.getManager()).thenReturn(mockManager);
    prepareAndInstallView();

    int viewId = 2;
    List<InternalDistributedMember> mbrs = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> shutdowns = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> crashes = new LinkedList<InternalDistributedMember>();
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(mockMembers[3]);
   
    //install the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs, shutdowns, crashes);
    InstallViewMessage installViewMessage = new InstallViewMessage(netView, credentials, false);
    gmsJoinLeave.processMessage(installViewMessage);
    
    Assert.assertNotEquals(netView, gmsJoinLeave.getView());
    verify(mockManager).forceDisconnect(any(String.class));
  }
  
  public void testOlderPreparedViewBeforeFirstViewInstalled() throws IOException {
    initMocks();
    prepareView();
    
    int viewId = 0;
    List<InternalDistributedMember> mbrs = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> shutdowns = new LinkedList<InternalDistributedMember>();
    List<InternalDistributedMember> crashes = new LinkedList<InternalDistributedMember>();
    mbrs.add(mockMembers[1]);
    mbrs.add(mockMembers[2]);
    mbrs.add(mockMembers[3]);
   
    //install the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs, shutdowns, crashes);
    InstallViewMessage installViewMessage = new InstallViewMessage(netView, credentials, true);
    gmsJoinLeave.processMessage(installViewMessage);
    
    Assert.assertNotEquals(netView, gmsJoinLeave.getView());
  }
}

