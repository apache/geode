package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services.Stopper;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Manager;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.MessageHandler;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.JoinRequestMessage;
import com.gemstone.gemfire.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JGroupsMessengerJUnitTest {
  private Services services;
  private JGroupsMessenger messenger;
  private JoinLeave joinLeave;
  private Manager manager;
  private Stopper stopper;
  private InterceptUDP interceptor;


  /**
   * Create stub and mock objects
   */
  @Before
  public void initMocks() throws Exception {
    Properties nonDefault = new Properties();
    nonDefault.put(DistributionConfig.DISABLE_TCP_NAME, "true");
    nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "0");
    nonDefault.put(DistributionConfig.LOG_FILE_NAME, "");
    nonDefault.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
    nonDefault.put(DistributionConfig.LOCATORS_NAME, "localhost[10344]");
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    RemoteTransportConfig tconfig = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);
    
    stopper = mock(Stopper.class);
    when(stopper.isCancelInProgress()).thenReturn(false);
    
    manager = mock(Manager.class);
    
    joinLeave = mock(JoinLeave.class);
    
    ServiceConfig serviceConfig = new ServiceConfig(tconfig, config);
    
    services = mock(Services.class);
    when(services.getConfig()).thenReturn(serviceConfig);
    when(services.getCancelCriterion()).thenReturn(stopper);
    when(services.getManager()).thenReturn(manager);
    when(services.getJoinLeave()).thenReturn(joinLeave);
    when(services.getStatistics()).thenReturn(mock(DMStats.class));
    
    messenger = new JGroupsMessenger();
    messenger.init(services);
    
    String jgroupsConfig = messenger.getJGroupsStackConfig();
    int startIdx = jgroupsConfig.indexOf("<UDP");
    int insertIdx = jgroupsConfig.indexOf('>', startIdx+4) + 1;
    jgroupsConfig = jgroupsConfig.substring(0, insertIdx) +
        "<"+InterceptUDP.class.getName()+"/>" +
        jgroupsConfig.substring(insertIdx);
    messenger.setJGroupsStackConfigForTesting(jgroupsConfig);
    System.out.println("jgroups config: " + jgroupsConfig);
    
    messenger.start();
    messenger.started();
    
    interceptor = (InterceptUDP)messenger.myChannel
        .getProtocolStack().getTransport().getUpProtocol();
    
  }
  
  @After
  public void stopMessenger() {
    if (messenger != null && messenger.myChannel != null) {
      messenger.stop();
    }
  }
  
  
  @Test
  public void testMessageDeliveredToHandler() throws Exception {
    MessageHandler mh = mock(MessageHandler.class);
    messenger.addHandler(JoinRequestMessage.class, mh);
    
    InternalDistributedMember sender = createAddress(8888);
    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null);
    
    Message jmsg = messenger.createJGMessage(msg, messenger.jgAddress, Version.CURRENT_ORDINAL);
    interceptor.up(new Event(Event.MSG, jmsg));
    
    verify(mh, times(1)).processMessage(any(JoinRequestMessage.class));
    
    LeaveRequestMessage lmsg = new LeaveRequestMessage(messenger.localAddress, sender, "testing");
    jmsg = messenger.createJGMessage(lmsg, messenger.jgAddress, Version.CURRENT_ORDINAL);
    interceptor.up(new Event(Event.MSG, jmsg));
    
    verify(manager).processMessage(any(LeaveRequestMessage.class));
    
  }
  
  
  @Test
  public void testBigMessageIsFragmented() throws Exception {
    MessageHandler mh = mock(MessageHandler.class);
    messenger.addHandler(JoinRequestMessage.class, mh);
    
    InternalDistributedMember sender = createAddress(8888);
    JoinRequestMessage msg = new JoinRequestMessage(messenger.localAddress, sender, null);

    messenger.send(msg);
    assertTrue("expected 1 message to be sent but found "+ interceptor.unicastSentDataMessages,
        interceptor.unicastSentDataMessages == 1);

    // send a big message and expect fragmentation
    msg = new JoinRequestMessage(messenger.localAddress, sender, new byte[(int)(services.getConfig().getDistributionConfig().getUdpFragmentSize()*(1.5))]);
    
    interceptor.unicastSentDataMessages = 0;
    messenger.send(msg);
    assertTrue("expected 2 messages to be sent but found "+ interceptor.unicastSentDataMessages,
        interceptor.unicastSentDataMessages == 2);
    
  }
  
  
  /**
   * creates an InternalDistributedMember address that can be used
   * with the doctored JGroups channel.  This includes a logical
   * (UUID) address and a physical (IpAddress) address.
   * 
   * @param port the UDP port to use for the new address
   */
  private InternalDistributedMember createAddress(int port) {
    GMSMember gms = new GMSMember("localhost", 8888);
    gms.setUUID(UUID.randomUUID());
    gms.setVmKind(DistributionManager.NORMAL_DM_TYPE);
    gms.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return new InternalDistributedMember(gms);
  }
  
}
