package com.gemstone.org.jgroups;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;
import com.gemstone.org.jgroups.stack.IpAddress;

@Ignore
@Category(UnitTest.class)
public class JChannelJUnitTest extends TestCase {

  static String mcastAddress = "239.192.81.1";
  static int mcastPort = getRandomAvailableMCastPort(mcastAddress);
  
  static String jchannelConfig = "com.gemstone.org.jgroups.protocols.UDP("
      + " discard_incompatible_packets=true;"
      + "  enable_diagnostics=false;"
      + "  tos=16;"
      + "  mcast_port=" + mcastPort + ";  mcast_addr=" + mcastAddress +";"
      + "  loopback=false;"
      + "  use_incoming_packet_handler=false;"
      + "  use_outgoing_packet_handler=false;"
      + "  ip_ttl=0; down_thread=false; up_thread=false;)?"
      + "com.gemstone.org.jgroups.protocols.PING("
      + "  timeout=5000;"
      + "  down_thread=false; up_thread=false;"
      + "  num_initial_members=2;  num_ping_requests=1)?"
      + "com.gemstone.org.jgroups.protocols.FD_SOCK("
      + "  num_tries=2; connect_timeout=5000;"
      + "  up_thread=false; down_thread=false)?"
      + "com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT("
      + "  timeout=5000;  up_thread=false; down_thread=false)?"
      + "com.gemstone.org.jgroups.protocols.pbcast.NAKACK("
      + "  use_mcast_xmit=false;  gc_lag=10;"
      + "  retransmit_timeout=400,800,1200,2400,4800;"
      + "  down_thread=false; up_thread=false;"
      + "  discard_delivered_msgs=true)?"
      + "com.gemstone.org.jgroups.protocols.UNICAST("
      + "  timeout=400,800,1200,2400,4800;"
      + "  down_thread=false; up_thread=false)?"
      + "com.gemstone.org.jgroups.protocols.pbcast.STABLE("
      + "  stability_delay=50;"
      + "  desired_avg_gossip=2000;"
      + "  down_thread=false; up_thread=false;"
      + "  max_bytes=400000)?"
      + "com.gemstone.org.jgroups.protocols.pbcast.GMS("
      + "  disable_initial_coord=false;"
      + "  print_local_addr=false;"
      + "  join_timeout=5000;"
      + "  join_retry_timeout=2000;"
      + "  up_thread=false; down_thread=false;"
      + "  shun=true)?"
      + "com.gemstone.org.jgroups.protocols.FRAG2("
      + "   frag_size=20000;"
      + "   down_thread=false;"
      + "   up_thread=false)";
  
  private static int getRandomAvailableMCastPort(String addr) {
    InetAddress iAddr = null;
    try {
      iAddr = InetAddress.getByName(addr);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    while (true) {
      int port = getRandomWildcardBindPortNumber();
      if (isPortAvailable(port, iAddr)) {
        return port;
      }
    }
  }

  private static int getRandomWildcardBindPortNumber() {
    int rangeBase;
    int rangeTop;
// wcb port range on Windows is 1024..5000 (and Linux?)
// wcb port range on Solaris is 32768..65535
//     if (System.getProperty("os.name").equals("SunOS")) {
//       rangeBase=32768;
//       rangeTop=65535;
//     } else {
//       rangeBase=1024;
//       rangeTop=5000;
//     }
    rangeBase = 20001; // 20000/udp is securid
    rangeTop =  29999; // 30000/tcp is spoolfax

    Random rand = new Random();
    return rand.nextInt(rangeTop-rangeBase) + rangeBase;
  }
  
  public JChannelJUnitTest(String name) {
    super(name);
  }
  
  private static boolean isPortAvailable(int port, InetAddress addr) {
    DatagramSocket socket = null;
    try {
      socket = new MulticastSocket();
      socket.setSoTimeout(Integer.getInteger("AvailablePort.timeout", 2000).intValue());
      byte[] buffer = new byte[4];
      buffer[0] = (byte)'p';
      buffer[1] = (byte)'i';
      buffer[2] = (byte)'n';
      buffer[3] = (byte)'g';
      SocketAddress mcaddr = new InetSocketAddress(addr, port);
      DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length, mcaddr);
      socket.send(packet);
      try {
        socket.receive(packet);
        packet.getData();  // make sure there's data, but no need to process it
        return false;
      }
      catch (SocketTimeoutException ste) {
        //System.out.println("socket read timed out");
        return true;
      }
      catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }
    catch (java.io.IOException ioe) {
      if (ioe.getMessage().equals("Network is unreachable")) {
        throw new RuntimeException(ioe.getMessage(), ioe);
      }
      ioe.printStackTrace();
      return false;
    }
    catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    finally {
      if (socket != null) {
        try {
          socket.close();
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }  
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
//    System.setProperty("DistributionManager.DEBUG_JAVAGROUPS", "true");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
//    System.getProperties().remove("DistributionManager.DEBUG_JAVAGROUPS");
  }
  
  
  public void testAddressEquality() throws Exception {
    IpAddress addr1 = new IpAddress(InetAddress.getLocalHost(), 1234);
    IpAddress addr2 = new IpAddress(addr1.getIpAddress(), addr1.getPort());
    if (!addr1.equals(addr2)) {
      fail("expected addresses to be equal");
    }
    addr2.setBirthViewId(4);
    if (!addr1.equals(addr2)) {
      fail("expected addresses to be equal");
    }
    addr1.setBirthViewId(0);
    int comparison = addr1.compareTo(addr2);
    if (comparison >= 0) {
      fail("expected addresses to be unequal but compareTo returned " + comparison);
    }
  }
  

  public void testConnectAndSendMessage() throws Exception {
    String properties = jchannelConfig;
    System.out.println("creating channel 1");
    JChannel channel1 = new JChannel(properties);
    System.out.println("creating channel 2");
    JChannel channel2 = new JChannel(properties);
    
    String channelName = "JChannelJUnitTest";
    
    System.out.println("connecting channel 1");
    channel1.connect(channelName);
    
    try {
      System.out.println("connecting channel 2");
      channel2.connect(channelName);

      try {
        long giveupTime = System.currentTimeMillis() + 20000;
        String failure;
        do {
          failure = null;
          // There should be two members in the views
          if (channel1.getView().size() < 2) {
            failure = "expected 2 members to be in the view: " + channel1.getView();
          }
          if (channel2.getView().size() < 2) {
            failure = "expected 2 members to be in the view: " + channel2.getView();
          }
        } while (failure != null && System.currentTimeMillis() < giveupTime);

        if (failure != null) {
          fail(failure);
        }
        
        // Send a unicast message using one channel and receive it on the other channel
        sendAndReceive("JGroups test message", channel1, channel2, false);

        // Send a multicast message using one channel and receive it on the other channel
        sendAndReceive("JGroups test message", channel1, channel2, true);

      } finally {
        channel2.disconnect();
      }
    } finally {
      channel1.disconnect();
    }
  }
  
  private void sendAndReceive(String str, JChannel channel1, JChannel channel2, 
      boolean multicast) throws Exception {
    Message msg = new Message(true);
    String sourceStr = "JGroups test message";
    msg.setObject(sourceStr);
    msg.setSrc(channel1.getLocalAddress());
    if (!multicast) {
      msg.setDest(channel2.getLocalAddress());
    }
    channel1.send(msg);
    Object obj = receive(channel2, 30000);
    String result = (String)((Message)obj).getObject();
    if ( !result.equals(sourceStr) ) {
      fail("expected to receive a message containing \""+sourceStr+"\" but received \""+result+"\"");
    }
  }
  
  private Object receive(JChannel channel, long timeoutMS) throws Exception {
    Long giveupTime = System.currentTimeMillis() + timeoutMS;
    Object obj;
    do {
      try {
        obj = channel.receive(1000);
      } catch (TimeoutException e) {
        obj = null;
      }
    }
    while ( !(obj instanceof Message) && System.currentTimeMillis() < giveupTime );
    if (obj == null || !(obj instanceof Message)) {
      fail("expected to receive a Message but received " + obj);
    }
    return obj;
  }
  
}
