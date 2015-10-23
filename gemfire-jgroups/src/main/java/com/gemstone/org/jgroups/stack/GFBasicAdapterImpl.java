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
package com.gemstone.org.jgroups.stack;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.spi.GFBasicAdapter;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.SockCreator;
import com.gemstone.org.jgroups.util.Util;
import com.gemstone.org.jgroups.util.VersionedStreamable;

public class GFBasicAdapterImpl implements GFBasicAdapter {

  private static boolean useIPv6Addresses = !Boolean.getBoolean("java.net.preferIPv4Stack") &&
      Boolean.getBoolean("java.net.preferIPv6Addresses");
  
  static InetAddress localHost;
  
  // This static block avoids problems with poorly configured
  // /etc/hosts files, such as
  //   127.0.0.1 localhost
  //   127.0.1.1 ubuntu
  static {
    InetAddress lh = null;
    try {
      lh = InetAddress.getLocalHost();
      if (lh.isLoopbackAddress()) {
        InetAddress ipv4Fallback = null;
        InetAddress ipv6Fallback = null;
        // try to find a non-loopback address
        Set myInterfaces = getMyAddresses();
        boolean preferIPv6 = useIPv6Addresses;
        String lhName = null;
        for (Iterator<InetAddress> it = myInterfaces.iterator(); lhName == null && it.hasNext(); ) {
          InetAddress addr = it.next();
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress()) {
            break;
          }
          boolean ipv6 = addr instanceof Inet6Address;
          boolean ipv4 = addr instanceof Inet4Address;
          if ( (preferIPv6 && ipv6)
              || (!preferIPv6 && ipv4) ) {
            String addrName = addr.getCanonicalHostName();
            if (lh.isLoopbackAddress()) {
              lh = addr;
              lhName = addrName;
            } else if (addrName != null) {
              lh = addr;
              lhName = addrName;
            }
          } else {
            if (preferIPv6 && ipv4 && ipv4Fallback == null) {
              ipv4Fallback = addr;
            } else if (!preferIPv6 && ipv6 && ipv6Fallback == null) {
              ipv6Fallback = addr;
            }
          }
        }
        // vanilla Ubuntu installations will have a usable IPv6 address when
        // running as a guest OS on an IPv6-enabled machine.  We also look for
        // the alternative IPv4 configuration.
        if (lh.isLoopbackAddress()) {
          if (ipv4Fallback != null) {
            lh = ipv4Fallback;
            useIPv6Addresses = false;
          } else if (ipv6Fallback != null) {
            lh = ipv6Fallback;
            useIPv6Addresses = true;
          }
        }
      }
    } catch (UnknownHostException e) {
    }
    localHost = lh;
  }
  
  private static Set<InetAddress> getMyAddresses() {
    Set<InetAddress> result = new HashSet<InetAddress>();
    Set<InetAddress> locals = new HashSet<InetAddress>();
    Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new IllegalArgumentException("Unable to examine network interfaces", e);
    }
    while (interfaces.hasMoreElements()) {
      NetworkInterface face = interfaces.nextElement();
      boolean faceIsUp = false;
      try {
        faceIsUp = face.isUp();
      } catch (SocketException e) {
        GemFireTracer.getLog(GFBasicAdapterImpl.class).info(ExternalStrings.ONE_ARG,
            new Object[]{"Failed to check if network interface is up. Skipping " + face}, e);
      }
      if (faceIsUp) {
        Enumeration<InetAddress> addrs = face.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress() 
              /*|| (!useLinkLocalAddresses && addr.isLinkLocalAddress())*/) {
            locals.add(addr);
          } else {
            result.add(addr);
          }
        } // while
      }
    } // while
    // fix for bug #42427 - allow product to run on a standalone box by using
    // local addresses if there are no non-local addresses available
    if (result.size() == 0) {
      return locals;
    } else {
      return result;
    }
  }


  @Override
  public short getMulticastVersionOrdinal() {
    return JGroupsVersion.GFE_71_ORDINAL;
  }

  @Override
  public short getSerializationVersionOrdinal(short version) {
    return version;
  }

  @Override
  public short getCurrentVersionOrdinal() {
    return JGroupsVersion.CURRENT_ORDINAL;
  }

  @Override
  public byte[] serializeWithVersion(Object obj, int destVersionOrdinal) {
    try {
      ByteArrayOutputStream out_stream=new ByteArrayOutputStream();
      ObjectOutputStream out=new ObjectOutputStream(out_stream);
      out.writeObject(obj);
      return out_stream.toByteArray();
    }
    catch(IOException ex) {
      // GemStoneAddition - we need the cause to figure out what went wrong
      IllegalArgumentException ia = new
          IllegalArgumentException("Error serializing message");
      ia.initCause(ex);
      throw ia;
    }
    
  }

  
  static final byte DEST_SET=1;
  static final byte SRC_SET=2;
  static final byte BUF_SET=4;
  // static final byte HDRS_SET=8; // bela July 15 2005: not needed, we always create headers
  static final byte IPADDR_DEST=16;
  static final byte IPADDR_SRC=32;
  static final byte SRC_HOST_NULL=64;
  
  static final byte CACHE_OP = 1; // GemStoneAddition
  static final byte HIGH_PRIORITY = 8; // GemStoneAddition



  @Override
  public void serializeJGMessage(Message msg, DataOutputStream out)
      throws IOException {
    //int begIdx = out.size(); // GemStoneAddition
    byte leading=0;

//    if(dest_addr != null) {
//        leading+=DEST_SET;
//        if(dest_addr instanceof IpAddress)
//            leading+=IPADDR_DEST;
//    }

    short serVersion = msg.getDestVersionOrdinal();
    JGroupsVersion.writeOrdinal(out, serVersion, true);

    if(msg.getSrc() != null) {
        leading+=SRC_SET;
        if(msg.getSrc() instanceof IpAddress) {
            leading+=IPADDR_SRC;
            if(((IpAddress)msg.getSrc()).getIpAddress() == null) {
                leading+=SRC_HOST_NULL;
            }
        }
    }
    if(msg.getRawBuffer() != null)
        leading+=BUF_SET;

    // 1. write the leading byte first
    out.write(leading);

    // 2. dest_addr
//    if(dest_addr != null) {
//        if(dest_addr instanceof IpAddress)
//            dest_addr.writeTo(out);
//        else
//            Util.writeAddress(dest_addr, out);
//    }

    // 3. src_addr
    if(msg.getSrc() != null) {
        if(msg.getSrc() instanceof IpAddress) {
           ((IpAddress)msg.getSrc()).toData(out);
        }
        else {
            Util.writeAddress(msg.getSrc(), out);
        }
        //eidx = out.size();
        //if ( (eidx - sidx) > src_addr.size() ) {
        //  log.error("address serialized to " + (eidx-sidx) + " bytes but reported size = " + src_addr.size(), new Error());
        //}
    }

    // GemStoneAddition - more flags
    byte gfFlags = 0;
    if (msg.isCacheOperation)
      gfFlags += CACHE_OP;
    if (msg.isHighPriority)
      gfFlags += HIGH_PRIORITY;
    out.write(gfFlags);
    
    // 4. buf
    if(msg.getRawBuffer() != null) {
        out.writeInt(msg.getLength()-msg.getOffset());
        out.write(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
    }

    // 5. headers
    short size=(short)msg.getHeaders().size();
    out.writeShort(size);
    Map.Entry        entry;
//    if (log.isTraceEnabled()) {
//      log.trace("writing " + size + " headers");
//    }
    //long estSize, startPos, endPos; // GemStoneAddition
    for(Iterator it=msg.getHeaders().entrySet().iterator(); it.hasNext();) {
        entry=(Map.Entry)it.next();
        //estSize = ((String)entry.getKey()).length() + 2;
        //estSize = estSize + 5 + ((Header)entry.getValue()).size();
        //startPos = out.size();
        out.writeUTF((String)entry.getKey());
        ByteArrayOutputStream baos = new ByteArrayOutputStream(50);
        DataOutputStream hdos;
        hdos = new DataOutputStream(baos);
        
        msg.writeHeader((Header)entry.getValue(), hdos);
        hdos.flush();
        byte[] headerBytes = baos.toByteArray();
//        if (log.isTraceEnabled()) {
//          log.trace("writing header " + entry.getKey() + " length=" + headerBytes.length);
//        }
        out.writeInt(headerBytes.length);
        out.write(headerBytes);
        //endPos = out.size();
        //if ( (endPos-startPos) > estSize ) {
        //  log.error("bad estimating job in message header: " + entry.getKey(), new Error());
        //}
    }
    //int endIdx = out.size();
    //if ( (endIdx - begIdx) > size() ) {
    //  log.error("bad job estimating message size: " + size() + " (est), " + (endIdx-begIdx) + " (actual)", new Error());
    //}
  }

  @Override
  public void deserializeJGMessage(Message msg, DataInputStream in)
      throws IOException, IllegalAccessException, InstantiationException {
    int len, leading;
    String hdr_name;
    Header hdr = null;

    // create a versioned stream if a different version
    // was used to multicast this message
    short sv = JGroupsVersion.readOrdinal(in);
    msg.setVersion(sv);
    
    // 1. read the leading byte first
    leading=in.readByte();

    // 2. dest_addr
//    if((leading & DEST_SET) == DEST_SET) {
//        if((leading & IPADDR_DEST) == IPADDR_DEST) {
//            dest_addr=new IpAddress();
//            dest_addr.readFrom(in);
//        }
//        else {
//            dest_addr=Util.readAddress(in);
//        }
//    }


    // 3. src_addr
    if((leading & SRC_SET) == SRC_SET) {
        if((leading & IPADDR_SRC) == IPADDR_SRC) {
            IpAddress src_addr=new IpAddress();
            src_addr.fromData(in);
            msg.setSrc(src_addr);
            //readFrom(in);
        }
        else {
            msg.setSrc(Util.readAddress(in));
        }
    }
    
    // GemStoneAddition
    byte gfFlags = in.readByte();
    if ( (gfFlags & CACHE_OP) != 0 )
      msg.isCacheOperation = true;
    if ( (gfFlags & HIGH_PRIORITY) != 0 )
      msg.isHighPriority = true;

    // 4. buf
    if((leading & BUF_SET) == BUF_SET) {
        len=in.readInt();
        try { // GemStoneAddition -- flag this as a problem
          msg.setBuffer(new byte[len]);
        }
        catch (OutOfMemoryError e) {
          System.err.println(
              "JGroups#Message: unable to allocate buffer of size " + len
              + "; gfFlags = " + gfFlags + "; src_addr = " + msg.getSrc());
          throw e;
        }
        in.readFully(msg.getRawBuffer(), 0, len); // GemStoneAddition - use readFully
        //in.read(buf, 0, len);
    }

    // 5. headers
    len=in.readShort();
    if (msg.getHeaders() == null) {
      msg.setHeaders(msg.createHeaders(len));
    }
    for(int i=0; i < len; i++) {
        hdr_name=in.readUTF();
        int hlen = in.readInt();
//        if (log.isTraceEnabled()) {
//          log.trace("reading header " + hdr_name + " of length " + hlen);
//        }
        byte[] headerBytes = new byte[hlen];
        in.readFully(headerBytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(headerBytes);
        DataInputStream dis;
        dis = new DataInputStream(bais);
        try {
          hdr=msg.readHeader(dis);
//          if (log.isTraceEnabled()) {
//            log.trace("read " + hdr.toString());
//          }
          msg.getHeaders().put(hdr_name, hdr);
        } catch (Exception e) {
          GemFireTracer log = GemFireTracer.getLog(getClass());
          if (log.isErrorEnabled()) {
            log.error("Failed to deserialize a header " + hdr_name + " of length " + hlen, e);
          }
        }
    }
  }

  @Override
  public ObjectOutput getObjectOutput(DataOutputStream out) throws IOException {
    return new ObjectOutputStream(out);
  }

  @Override
  public ObjectInput getObjectInput(DataInputStream in) throws IOException {
    return new ObjectInputStream(in);
  }

  @Override
  public RuntimeException getGemFireConfigException(String string) {
    return new RuntimeException(string);
  }

  @Override
  public InetAddress getLocalHost() throws UnknownHostException {
    return InetAddress.getLocalHost();
  }
  
  @Override
  public void setDefaultGemFireAttributes(IpAddress local_addr) {
    // no-op by default
  }

  @Override
  public void setGemFireAttributes(IpAddress addr, Object obj) {
    // no-op by default
  }
  
  @Override
  public RuntimeException getSystemConnectException(String localizedString) {
    return new RuntimeException(localizedString);
  }

  @Override
  public RuntimeException getDisconnectException(String localizedString) {
    return new RuntimeException(localizedString);
  }

  @Override
  public Object getForcedDisconnectException(String localizedString) {
    return new RuntimeException(localizedString);
  }

  @Override
  public void invokeToData(Object obj, DataOutput out) throws IOException {
    ((VersionedStreamable)obj).toData(out);
  }

  @Override
  public void writeObject(Object obj, DataOutput out) throws IOException {
    _writeObject(obj, out);
  }

  @Override
  public void invokeFromData(Object obj, DataInput in) throws IOException,
      ClassNotFoundException {
    ((VersionedStreamable)obj).fromData(in);
  }

  @Override
  public <T> T readObject(DataInput in) throws IOException,
      ClassNotFoundException {
    return (T)_readObject(in);
  }

  @Override
  public void checkDisableDNS() {
  }

  @Override
  public void writeString(String str, DataOutput out) throws IOException {
    out.writeUTF(str);
  }

  @Override
  public String readString(DataInput in) throws IOException {
    return in.readUTF();
  }

  @Override
  public void writeStringArray(String[] strings, DataOutput out)
      throws IOException {
    (new ObjectOutputStream((DataOutputStream)out)).writeObject(strings);
  }

  @Override
  public String[] readStringArray(DataInput in) throws IOException {
    return (String[])_readObject(in);
  }

  @Override
  public String getHostName(InetAddress ip_addr) {
    return ip_addr.getCanonicalHostName();
  }

  @Override
  public DataOutputStream getVersionedDataOutputStream(DataOutputStream dos,
      short version) throws IOException {
    return dos;
  }

  @Override
  public DataInputStream getVersionedDataInputStream(DataInputStream instream,
      short version) throws IOException {
    return instream;
  }

  @Override
  public byte[] readByteArray(DataInput in) throws IOException {
    return (byte[])_readObject(in);
  }

  @Override
  public void writeByteArray(byte[] array, DataOutput out) throws IOException {
    _writeObject(array, out);
  }

  @Override
  public void writeProperties(Properties props, DataOutput oos)
      throws IOException {
    _writeObject(props, oos);
  }

  @Override
  public Properties readProperties(DataInput in) throws IOException,
      ClassNotFoundException {
    return (Properties)_readObject(in);
  }

  @Override
  public RuntimeException getAuthenticationFailedException(String failReason) {
    return new RuntimeException(failReason);
  }

  @Override
  public SockCreator getSockCreator() {
    return new SockCreatorImpl();
  }

  @Override
  public int getGossipVersionForOrdinal(short serverOrdinal) {
    return JGroupsVersion.CURRENT_ORDINAL;
  }

  @Override
  public boolean isVersionForStreamAtLeast(DataOutput stream, short version) {
    return true;
  }

  @Override
  public boolean isVersionForStreamAtLeast(DataInput stream, short version) {
    return true;
  }


  /**
   * basic method for reading objects from a DataInput.
   */
  private Object _readObject(DataInput in) throws IOException {
    ObjectInputStream ois;
    if (in instanceof ObjectInputStream) {
      ois = (ObjectInputStream)in;
    } else {
      ois = new ObjectInputStream((DataInputStream)in);
    }
    try {
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * basic method for writing objects to a DataOuput.
   */
  private void _writeObject(Object obj, DataOutput out) throws IOException {
    ObjectOutputStream oos;
    if (out instanceof ObjectOutputStream) {
      oos = (ObjectOutputStream)out;
    } else {
      oos = new ObjectOutputStream((DataOutputStream)out);
    }
    oos.writeObject(obj);
  }

  @Override
  public String getVmKindString(int vmKind) {
    return Integer.toString(vmKind);
  }
  
  
}
