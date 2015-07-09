package com.gemstone.org.jgroups.protocols;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.distributed.internal.membership.jgroup.GFJGBasicAdapter;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.ChannelClosedException;
import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.ChannelNotConnectedException;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.TimeoutException;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.stack.GFBasicAdapterImpl;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.ProtocolStack;
import com.gemstone.org.jgroups.util.GemFireTracer;

@Category(IntegrationTest.class)
public class JGroupsFailureDetectionJUnitTest {

  static final int numMembers = 40;
  static final int memberTimeout = 300;

  // JGroups protocol stacks for each member
  List<ProtocolStack> stacks = new ArrayList<ProtocolStack>(numMembers);

  // each member's address
  Vector<IpAddress> members = new Vector<IpAddress>(numMembers);
  
  // mapping from address to member's protocol stack
  Map<IpAddress, ProtocolStack> addressToStack = new HashMap<IpAddress, ProtocolStack>();
  

  
  @Before
  public void setUp() throws Exception {
//    GemFireTracer.DEBUG = true;
    JChannel.setDefaultGFFunctions(new GFJGBasicAdapter());
    InetAddress addr = InetAddress.getLocalHost();

    // create the protocol stacks and addresses
    for (int i=1; i<=numMembers; i++) {
      IpAddress mbr = new IpAddress(addr, i+10000);
      members.add(mbr);
      ProtocolStack stack = createStack(mbr);
      stacks.add(stack);
      addressToStack.put(mbr, stack);
      getMessagingProtocol(stack).setRecipients(addressToStack);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    GemFireTracer.DEBUG = false;
    JChannel.setDefaultGFFunctions(new GFBasicAdapterImpl());
    for (ProtocolStack stack: stacks) {
      stack.stop();
    }
  }

  ProtocolStack createStack(IpAddress addr) throws Exception {
    CollectingProtocol top = new CollectingProtocol(true, false);
    TestFD_SOCK tfds = new TestFD_SOCK();
    TestFD tfd = new TestFD();
    TestVERIFY_SUSPECT tvs = new TestVERIFY_SUSPECT();
    MessagingProtocol bottom = new MessagingProtocol();
    
    // connect the protocols into a stack and start them
    top.setDownProtocol(tvs);
    tvs.setUpProtocol(top);  tvs.setDownProtocol(tfds);
    tfds.setUpProtocol(tvs); tfds.setDownProtocol(tfd);
    tfd.setUpProtocol(tfds); tfd.setDownProtocol(bottom);
//    tfdr.setUpProtocol(tfd); tfdr.setDownProtocol(bottom);
    bottom.setUpProtocol(tfd);
    
    MyChannel channel = MyChannel.create(null, addr);
    ProtocolStack stack = ProtocolStack.createTestStack(channel, top);
    Vector protocols = stack.getProtocols();
    for (int i=0; i<protocols.size(); i++) {
      Protocol p = (Protocol)protocols.get(i);
      p.init();
    }
    channel.setProtocolStack(stack);
    return stack;
  }
  
  IpAddress getFDSockAddress(ProtocolStack stack) {
    TestFD_SOCK fdsock = (TestFD_SOCK)stack.findProtocol("FD_SOCK");
    return fdsock.getFdSockAddress();
  }
  
  MessagingProtocol getMessagingProtocol(ProtocolStack stack) {
    return (MessagingProtocol)stack.findProtocol("MessagingProtocol");
  }
  
  
  void killStack(ProtocolStack stack) {
    TestFD_SOCK fdsock = (TestFD_SOCK)stack.findProtocol("FD_SOCK");
    TestVERIFY_SUSPECT vs = (TestVERIFY_SUSPECT)stack.findProtocol("VERIFY_SUSPECT");
    vs.playDead(true);
    fdsock.beSick();
    stack.getChannel().close();
  }
  
  
  /**
   * This test creates a jgroups protocol stack holding our failure-detection
   * protocols.  At the top of the stack is a protocol defined by this test
   * that collects non-message events and at the bottom is a messaging simulation
   * protocol that routes messages to the correct stack.<p>
   * 
   * Forty stacks are created, simulating 40 members in a distributed system.
   * The first twenty are then killed and we observe how long it takes for the
   * remaining members to figure it out.  If they don't figure it out fast enough
   * the test fails.
   *  
   * @throws Exception
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testFailureDetectionManyMembers() throws Exception {
    
    
    // create the first view
    IpAddress coord = members.get(0);
    ViewId vid = new ViewId(coord, 1);
    View view = new View(vid, members);
    for (int i=0; i<stacks.size(); i++) {
      Protocol bot = stacks.get(i).getBottomProtocol();
      bot.up(new Event(Event.SET_LOCAL_ADDRESS, members.get(i)));
      Protocol top = stacks.get(i).getTopProtocol();
      top.down(new Event(Event.CONNECT));
      bot.up(new Event(Event.CONNECT_OK));
      top.down(new Event(Event.VIEW_CHANGE, view));
      bot.up(new Event(Event.VIEW_CHANGE, view));
    }
    
    // collect the FD_SOCK server socket addresses and create
    // a cache, then pass it to all of the stacks
    Hashtable cache = new Hashtable();
    for (int i=0; i<numMembers; i++) {
      IpAddress fdSockAddr = getFDSockAddress(stacks.get(i));
      System.out.println("member " + members.get(i) + " has FD_SOCK address " + fdSockAddr);
      cache.put(members.get(i), fdSockAddr);
    }
    
    for (int i=0; i<stacks.size(); i++) {
      Event ev = new Event(Event.MSG);
      Message m = new Message();
      m.setSrc(coord);
      Hashtable c = new Hashtable(cache);
      m.putHeader("FD_SOCK", new FD_SOCK.FdHeader(FD_SOCK.FdHeader.GET_CACHE_RSP, c));
      ev.setArg(m);
      Protocol bot = stacks.get(i).getBottomProtocol();
      m.setDest(members.get(i));
      bot.up(ev);
    }
    
//    if (true) {
//      return;
//    }
  
    // give members time to exchange fd_sock information
    Thread.sleep(3000);
    
    // kill half of the members, including the coordinator
    for (int i=0; i<(numMembers/2); i++) {
      killStack(stacks.get(i));
    }
    
//    Thread.sleep(2 * memberTimeout);

    // now wait for members to figure out that half of them
    // are gone.  On an old unloaded linux machine this usually
    // takes < 1.5 seconds but we allow more since this test
    // will be run in parallel with other junit tests
    long maxWaitTime = (3 * memberTimeout) * 10;
    int expectedCount = numMembers/2;
    StringBuilder failures;
    long startTime = System.currentTimeMillis();
    long giveUpTime = startTime + maxWaitTime;
    do {
      // verify that all remaining members have kicked out the bad guys
      failures = new StringBuilder();
      for (int i=(numMembers/2); i<numMembers; i++) {
        CollectingProtocol top = (CollectingProtocol)stacks.get(i).getTopProtocol();
        boolean[] suspects = new boolean[numMembers/2];
        int numSuspects = 0;
        synchronized(top.collectedUpEvents) { 
          for (Event event: top.collectedUpEvents) {
            if (event != null  &&  event.getType() == Event.SUSPECT) { 
              SuspectMember suspect = (SuspectMember)event.getArg();
              int index = ((IpAddress)suspect.suspectedMember).getPort() - 10000 - 1;
              if (!suspects[index]) {
                suspects[index] = true;
                numSuspects++;
              }
            }
          }
        }
        if (numSuspects < expectedCount) {
          if (failures.length() > 0) {
            failures.append("\n");
          }
          failures.append("member ").append(i+1).append(" only saw ").append(numSuspects)
           .append(" failures but should have seen ").append(expectedCount).append(".  missing=");
          for (int si=0; si<suspects.length; si++) {
            if (!suspects[si]) {
              failures.append(" ").append(si+1);
            }
          }
        }
      }
    } while (failures.length() > 0 && System.currentTimeMillis() < giveUpTime);
    if (failures.length() > 0) {
      Assert.fail(failures);
    } else {
      System.out.println("completed in "
          + (System.currentTimeMillis() - startTime) + "ms");
    }
  }
  
  
  public static class TestFD_SOCK extends FD_SOCK {
    public TestFD_SOCK() {
      num_tries=1;
      connectTimeout=memberTimeout;
      start_port=1024;
      end_port=65535;
      up_thread=false;
      down_thread=false;
    }
    public IpAddress getFdSockAddress() {
      return this.srv_sock_addr;
    }
    @Override
    public boolean isDisconnecting() {
      return !this.stack.getChannel().isConnected();
    }
  }
  
  public static class TestFD extends FD {
    public TestFD() {
      timeout=memberTimeout;
      max_tries=1;
      up_thread=false;
      down_thread=false;
      shun=false;
    }
  }
  
  public static class TestVERIFY_SUSPECT extends VERIFY_SUSPECT {
    public TestVERIFY_SUSPECT() {
      timeout=memberTimeout;
      up_thread=false;
      down_thread=false;
    }
  }
  
  public static class MessagingProtocol extends Protocol {
    Map<IpAddress,ProtocolStack> recipients;
    IpAddress myAddress;
    
    public void setRecipients(Map<IpAddress,ProtocolStack> recipients) {
      this.recipients = recipients;
    }
    public String getName() {
      return "MessagingProtocol";
    }
    public void up(Event ev) {
      switch (ev.getType()) {
      case Event.SET_LOCAL_ADDRESS:
        myAddress = (IpAddress)ev.getArg();
        break;
      }
      passUp(ev);
    }
    public void down(Event ev) {
      switch (ev.getType()) {
      case Event.MSG:
        Message m = (Message)ev.getArg();
        m.setSrc(myAddress);
        log.debug(""+myAddress+" is sending " + m);
        if (m.getDest() == null) {
          for (ProtocolStack stack: recipients.values()) {
            stack.getBottomProtocol().up(ev);
          }
        } else {
          ProtocolStack stack = recipients.get(m.getDest());
          if (stack.getChannel().isOpen()) {
            stack.getBottomProtocol().up(ev);
          }
        }
        break;
      default:
        passDown(ev);
      }
    }
  }

  /**
   * CollectingProtocol gathers the messages it receives in up() and down()
   * for later inspection.
   */
  public static class CollectingProtocol extends Protocol {
    List<Event> collectedUpEvents = null;
    List<Event> collectedDownEvents;
    Address myAddress;
    
    public CollectingProtocol(boolean collectUp, boolean collectDown) {
      if (collectUp) {
        collectedUpEvents = new LinkedList<Event>();
      }
      if (collectDown) {
        collectedDownEvents = new LinkedList<Event>();;
      }
    }
    
    @Override
    public String getName() {
      return "CollectingProtocol";
    }
    
    public void clear() {
      if (collectedDownEvents != null) {
        synchronized(collectedDownEvents) {
          collectedDownEvents.clear();
        }
      }
      if (collectedUpEvents != null) {
        synchronized(collectedUpEvents) {
          collectedUpEvents.clear();
        }
      }
    }
    
    @Override
    public void down(Event ev) {
      if (collectedDownEvents != null) {
        log.debug(""+myAddress+" collecting event " + ev);
        synchronized(collectedDownEvents) {
          collectedDownEvents.add(ev);
        }
      }
      passDown(ev);
    }
    
    @Override
    public void up(Event ev) {
      boolean skip = false;
      switch (ev.getType()) {
      case Event.SET_LOCAL_ADDRESS:
        myAddress = (IpAddress)ev.getArg();
        skip = true;
        break;
      case Event.MSG:
        skip = true;
        break;
      }
      if (!skip  &&  collectedUpEvents != null) {
        log.debug(""+myAddress+" collecting event " + ev);
        synchronized(collectedUpEvents) {
          collectedUpEvents.add(ev);
        }
      }
      passUp(ev);
    }
  }
  
  public static class MyChannel extends JChannel {

    boolean isOpen = true;
    View myView;
    Address myAddress;
    
    public static MyChannel create(View v, Address addr) throws ChannelException {
      MyChannel instance = new MyChannel();
      instance.myView = v;
      instance.myAddress = addr;
      return instance;
    }

    public void setProtocolStack(ProtocolStack stack) {
      this.prot_stack = stack;
    }

    public MyChannel() throws ChannelException {
    }
    
    public MyChannel(Properties p) throws ChannelException {
    }

    protected GemFireTracer getLog() {
      return null;
    }
    public void connect(String channel_name) throws ChannelException,
        ChannelClosedException {
    }
    public void disconnect() {
      isOpen = false;
    }
    public void close() {
      isOpen = false;
      getProtocolStack().down(new Event(Event.DISCONNECT));
      getProtocolStack().down(new Event(Event.STOP));
    }
    public void shutdown() {
      isOpen = false;
    }
    public boolean isOpen() {
      return isOpen;
    }
    public boolean isConnected() {
      return isOpen;
    }
    public Map dumpStats() {
      return null;
    }
    public void send(Message msg) throws ChannelNotConnectedException,
        ChannelClosedException {
    }
    public void send(Address dst, Address src, Serializable obj)
        throws ChannelNotConnectedException, ChannelClosedException {
    }
    public Object receive(long timeout) throws ChannelNotConnectedException,
        ChannelClosedException, TimeoutException {
      return null;
    }
    public Object peek(long timeout) throws ChannelNotConnectedException,
        ChannelClosedException, TimeoutException {
      return null;
    }
    public View getView() {
      return myView;
    }
    public Address getLocalAddress() {
      return myAddress;
    }
    public String getChannelName() {
      return "GF7";
    }
    public void setOpt(int option, Object value) {
    }
    public Object getOpt(int option) {
      return null;
    }
    public void blockOk() {
    }
    public boolean getState(Address target, long timeout)
        throws ChannelNotConnectedException, ChannelClosedException {
      return false;
    }
    public boolean closing() {
      return !isOpen;
    }
    public boolean getAllStates(Vector targets, long timeout)
        throws ChannelNotConnectedException, ChannelClosedException {
      return false;
    }
    public void returnState(byte[] state) {
    }
    
  }
  
}
