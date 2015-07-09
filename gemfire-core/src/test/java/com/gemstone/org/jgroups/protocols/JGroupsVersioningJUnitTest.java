/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.org.jgroups.protocols;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.membership.jgroup.GFJGBasicAdapter;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.VersionedDataOutputStream;
import com.gemstone.gemfire.internal.VersionedObjectInput;
import com.gemstone.gemfire.internal.VersionedObjectOutput;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.GFBasicAdapterImpl;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
@Category(UnitTest.class)
public class JGroupsVersioningJUnitTest {

  private static final String HEADER_NAME = "vHeader";

  /* 
   * For the tests in this class we create a functioning InternalDistributedSystem
   * with tcp disabled and test with its jgroups channel.
   */
  @Before
  public void setUp() throws IOException {
    JChannel.setDefaultGFFunctions(new GFJGBasicAdapter());
  }

  @After
  public void tearDown() {
    JChannel.setDefaultGFFunctions(new GFBasicAdapterImpl());
  }
  
  /** basic marshaling test for a Message */
  @Test
  public void testMessageVersioning() throws Exception {
    Version v = Version.GFE_81;
    VersionedIpAddress src = new VersionedIpAddress("localhost", 12345, v);
    Message m = new Message(true);
    m.setSrc(src);
    m.setVersion(v.ordinal()); // set the serialization version
    VersionedHeader header = new VersionedHeader();
    m.putHeader(HEADER_NAME, header);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    m.writeTo(out);
    Assert.assertTrue(header.serializedVersion == v, "expected header to serialize with older version");
    Assert.assertTrue(src.serializedVersion == v, "expected src address to serialize with older version");
    out.close();
    byte[] result = bos.toByteArray();
    
    ByteArrayInputStream bis = new ByteArrayInputStream(result);
    DataInputStream in = new DataInputStream(bis);
    m = new Message(true);
    m.setVersion(v.ordinal());
    m.readFrom(in);
    header = (VersionedHeader)m.getHeader(HEADER_NAME);
    Assert.assertTrue(header != null, "expected to find a VersionedHeader");
    Assert.assertTrue(header.serializedVersion == v, "expected header to be deserialized with an older version");
    // Deserialization of Messages create IpAddress instances directly, so we can't expect the
    // message to contain a VersionedIpAddress.
  }
  
  /**
   * Test that the transport protocol handles versioning properly
   * @throws Exception
   */
  @Test
  public void testUDP() throws Exception {
    System.out.println("starting testUDP");
    TestUDP udp = new TestUDP();
    JChannel.setDefaultGFFunctions(new GFJGBasicAdapter());
    Version v = Version.GFE_81;
    VersionedIpAddress src = new VersionedIpAddress("localhost", 12345, v);
    VersionedIpAddress dest = new VersionedIpAddress("localhost", 12347, v);
    udp.down(new Event(Event.SET_LOCAL_ADDRESS, src));

    Vector mbrs = new Vector();
    mbrs.add(src); mbrs.add(dest);
    View view = new View(src, 1, mbrs);
    udp.down(new Event(Event.VIEW_CHANGE, view));

    // send a multicast message first
    Message m = new Message(true);
    m.setSrc(src);
    VersionedHeader header = new VersionedHeader();
    m.putHeader(HEADER_NAME, header);
    udp.down(new Event(Event.MSG, m));
    Assert.assertTrue(header.serializedVersion == v, "expected header to serialize with older version");
    VersionedIpAddress msgSrc = (VersionedIpAddress)m.getSrc();
    Assert.assertTrue(msgSrc.serializedVersion == v, "expected src address to serialize with older version: " + msgSrc);

    // simulate receiving the message
    List<Buffer> buffers = udp.collectedBuffers;
    Assert.assertTrue(buffers.size() == 1); // should have serialized one message and sent it out
    Buffer buffer = buffers.get(0);
    udp.receive(src, dest, buffer.data, buffer.offset, buffer.length);
    Message msg = udp.lastReceivedMessage;
    Assert.assertTrue(msg.getVersion() == v.ordinal());
    header = (VersionedHeader)msg.getHeader(HEADER_NAME);
    Assert.assertTrue(header != null); // header should have been included
    Assert.assertTrue(header.serializedVersion == v);

    // now do the same with a point-to-point message
    udp.collectedBuffers.clear();
    udp.lastReceivedMessage = null;

    m = new Message(true);
    m.setSrc(src);
    m.setDest(dest);
    header = new VersionedHeader();
    m.putHeader(HEADER_NAME, header);
    udp.down(new Event(Event.MSG, m));
    Assert.assertTrue(header.serializedVersion == v, "expected header to serialize with older version");
    msgSrc = (VersionedIpAddress)m.getSrc();
    Assert.assertTrue(msgSrc.serializedVersion == v, "expected src address to serialize with older version: " + msgSrc);

    // simulate receiving the message
    buffers = udp.collectedBuffers;
    Assert.assertTrue(buffers.size() == 1); // should have serialized one message and sent it out
    buffer = buffers.get(0);
    udp.receive(src, dest, buffer.data, buffer.offset, buffer.length);
    msg = udp.lastReceivedMessage;
    Assert.assertTrue(msg.getVersion() == v.ordinal());
    header = (VersionedHeader)msg.getHeader(HEADER_NAME);
    Assert.assertTrue(header != null); // header should have been included
    Assert.assertTrue(header.serializedVersion == v);
  }
  
  
  public static class Buffer {
    Address dest;
    byte[] data;
    int offset;
    int length;
    
    public Buffer(byte[] data, int offset, int length) {
      this.data = data;
      this.offset = offset;
      this.length = length;
    }
    public Buffer(Address dest, byte[] data, int offset, int length) {
      this.data = data;
      this.offset = offset;
      this.length = length;
      this.dest = dest;
    }
  }
  
  
  /**
   * TestUDP extends UDP and overrides some methods for unit testing
   */
  public static class TestUDP extends UDP {
    List<Buffer> collectedBuffers = new LinkedList<Buffer>();
    Message lastReceivedMessage;
    
    @Override
    public void sendToAllMembers(byte[] data, int offset, int length)
        throws Exception {
      collectedBuffers.add(new Buffer(data, offset, length));
    }

    @Override
    protected Message bufferToMessage(DataInputStream instream, Address dest,
        Address sender, boolean multicast) throws Exception {
      this.lastReceivedMessage = super.bufferToMessage(instream, dest, sender, multicast);
      return this.lastReceivedMessage;
    }

    @Override
    public void sendToSingleMember(Address dest, boolean isJoinResponse,
        byte[] data, int offset, int length) throws Exception {
      collectedBuffers.add(new Buffer(dest, data, offset, length));
    }
    
  }
  

  /**
   * CollectingProtocol gathers the messages it receives in up() and down()
   * for later inspection.
   */
  public static class CollectingProtocol extends Protocol {
    List<Message> collectedMessages = new LinkedList<Message>();
    
    @Override
    public String getName() {
      return "CollectingProtocol";
    }
    
    @Override
    public void down(Event ev) {
      if (ev.getType() == Event.MSG) {
        collectedMessages.add((Message)ev.getArg());
      }
    }
    
    @Override
    public void up(Event ev) {
      if (ev.getType() == Event.MSG) {
        collectedMessages.add((Message)ev.getArg());
      }
    }
  }
  
  /**
   * VersionedIpAddress is an IpAddress that tracks what version was
   * used for serialization/deserialization
   */
  public static class VersionedIpAddress extends IpAddress {
    Version serializedVersion;
    
    public VersionedIpAddress(String host, int port, Version v) {
      super(host, port);
      setVersionOrdinal(v.ordinal());
    }
    
    @Override
    public String getName() {
      return "";
    }
    
    @Override
    public String toString() {
      String sup = super.toString();
      if (this.serializedVersion != null) {
        sup += " serialized with " + serializedVersion;
      }
      return sup + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
        ClassNotFoundException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
//      System.out.println("serializing " + this + " with " + out);
//      Thread.dumpStack();
      if (out instanceof VersionedDataOutputStream) {
        this.serializedVersion = ((VersionedDataOutputStream)out).getVersion();
      } else {
        this.serializedVersion = Version.CURRENT;
      }
      super.toData(out);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      if (in instanceof VersionedDataInputStream) {
        this.serializedVersion = ((VersionedDataInputStream)in).getVersion();
      } else {
        this.serializedVersion = Version.CURRENT;
      }
      super.fromData(in);
    }

    
  }
  
  
  /**
   * VersionedHeader is a JGroups message header that keeps track of
   * what version was used to serialize/deserialize it. 
   */
  public static class VersionedHeader extends Header {
    Version serializedVersion;
    
    public VersionedHeader() {
      super();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      System.out.println("VersionedHeader.writeExternal invoked with " + out);
      if (out instanceof VersionedObjectOutput) {
        this.serializedVersion = ((VersionedObjectOutput)out).getVersion();
      } else {
        this.serializedVersion = Version.CURRENT;
      }
      out.writeBoolean(true);
      out.writeUTF("a header written with " + this.serializedVersion);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
        ClassNotFoundException {
      System.out.println("VersionedHeader.readExternal invoked with " + in);
      if (in instanceof VersionedObjectInput) {
        this.serializedVersion = ((VersionedObjectInput)in).getVersion();
      } else {
        this.serializedVersion = Version.CURRENT;
      }
      in.readBoolean();
      in.readUTF();
    }
    
    @Override
    public String toString() {
      return "VersionedHeader(v="+this.serializedVersion+")";
    }
  }
}
