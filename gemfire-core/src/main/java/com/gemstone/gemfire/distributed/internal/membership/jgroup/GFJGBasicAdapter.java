package com.gemstone.gemfire.distributed.internal.membership.jgroup;

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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.cache.UnsupportedVersionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.VersionedDataOutputStream;
import com.gemstone.gemfire.internal.VersionedObjectInput;
import com.gemstone.gemfire.internal.VersionedObjectOutput;
import com.gemstone.gemfire.internal.cache.tier.sockets.HandShake;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.GFBasicAdapter;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.GFLogWriter;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.SockCreator;
import com.gemstone.org.jgroups.util.Util;

public class GFJGBasicAdapter implements GFBasicAdapter {

  public static void insertGemFireAttributes(IpAddress addr, Object obj) {
    MemberAttributes attr = (MemberAttributes)obj;
    addr.setProcessId(attr.getVmPid());
    addr.setVmKind(attr.getVmKind());
    addr.setDirectPort(attr.getPort());
    addr.setBirthViewId(attr.getVmViewId());
    addr.setName(attr.getName());
    addr.setRoles(attr.getGroups());
    addr.setDurableClientAttributes(attr.getDurableClientAttributes());
    addr.setSize(-1);
    addr.setSize(addr.size(Version.CURRENT_ORDINAL));
  }

  public static void insertDefaultGemFireAttributes(IpAddress addr) {
    MemberAttributes attr = com.gemstone.gemfire.distributed.internal.membership.MemberAttributes.DEFAULT;
    insertGemFireAttributes(addr, attr);
  }

  @Override
  public void invokeToData(Object obj, DataOutput out) throws IOException {
    InternalDataSerializer.invokeToData(obj, out);
  }

  @Override
  public void writeObject(Object obj, DataOutput out) throws IOException {
    DataSerializer.writeObject(obj, out);
  }

  @Override
  public void invokeFromData(Object obj, DataInput in) throws IOException,
      ClassNotFoundException {
    InternalDataSerializer.invokeFromData(obj, in);
  }

  @Override
  public <T> T readObject(DataInput in) throws IOException,
      ClassNotFoundException {
    return DataSerializer.readObject(in);
  }

  @Override
  public short getMulticastVersionOrdinal() {
    return Version.GFE_71.ordinal();
  }

  @Override
  public short getSerializationVersionOrdinal(short version) {
    return Version.fromOrdinalOrCurrent(version).ordinal();
  }

  @Override
  public short getCurrentVersionOrdinal() {
    return Version.CURRENT_ORDINAL;
  }

  @Override
  public byte[] serializeWithVersion(Object obj, int destVersionOrdinal) {
    try {
      ByteArrayOutputStream out_stream=new ByteArrayOutputStream();
      //ObjectOutputStream out=new ObjectOutputStream(out_stream);
      //out.writeObject(obj);
      DataOutputStream out = new DataOutputStream(out_stream);
      if (destVersionOrdinal > 0 && destVersionOrdinal != Version.CURRENT_ORDINAL) {
        out = new VersionedDataOutputStream(out, Version.fromOrdinalOrCurrent((short)destVersionOrdinal));
      }
      DataSerializer.writeObject(obj, out); // GemStoneAddition
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
  public void serializeJGMessage(Message msg, DataOutputStream out) throws IOException {
    //int begIdx = out.size(); // GemStoneAddition
    byte leading=0;

//    if(dest_addr != null) {
//        leading+=DEST_SET;
//        if(dest_addr instanceof IpAddress)
//            leading+=IPADDR_DEST;
//    }

    short serVersion = msg.getDestVersionOrdinal();
    if (0 < serVersion  &&  serVersion < Version.CURRENT_ORDINAL) {
      out = new VersionedDataOutputStream(out, Version.fromOrdinalNoThrow(serVersion, false));
    }
    Version.writeOrdinal(out, serVersion, true);

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
           InternalDataSerializer.invokeToData(((IpAddress)msg.getSrc()), out); // GemStoneAddition
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
        if (0 < serVersion  &&  serVersion < Version.CURRENT_ORDINAL) {
          hdos = new VersionedDataOutputStream(baos, Version.fromOrdinalNoThrow(serVersion, false));
        } else {
          hdos = new DataOutputStream(baos);
        }
        
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
    short sv = Version.readOrdinal(in);
    msg.setVersion(sv);
    
    if (0 < msg.getVersion()  &&  msg.getVersion() < Version.CURRENT_ORDINAL) {
      try {
        in = new VersionedDataInputStream(in, Version.fromOrdinal(msg.getVersion(), false));
      } catch (UnsupportedVersionException e) {
        throw new IOException("Unexpected exception during deserialization", e);
      }
    }

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
            try {
              InternalDataSerializer.invokeFromData(((IpAddress)src_addr), in);
            } catch (ClassNotFoundException e) {
              throw new IOException(e);
            } // GemStoneAddition
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
        if (0 < msg.getVersion()  &&  msg.getVersion() < Version.CURRENT_ORDINAL) {
          try {
            dis = new VersionedDataInputStream(bais, Version.fromOrdinal(msg.getVersion(), false));
          } catch (UnsupportedVersionException e) {
            throw new IOException("Unexpected exception during deserialization", e);
          }
        } else {
          dis = new DataInputStream(bais);
        }
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
    ObjectOutput oos=new ObjectOutputStream(out);
    if (out instanceof VersionedDataOutputStream) {
      Version v = ((VersionedDataOutputStream)out).getVersion();
      if (v != null && v != Version.CURRENT) { 
        oos = new VersionedObjectOutput(oos, v);
      }
    }
    return oos;
  }

  @Override
  public ObjectInput getObjectInput(DataInputStream in) throws IOException {
    ObjectInput ois=new ObjectInputStream(in);
    if (in instanceof VersionedDataInputStream) {
      ois = new VersionedObjectInput(ois, ((VersionedDataInputStream)in).getVersion());
    }
    return ois;
  }

  @Override
  public int getGossipVersionForOrdinal(short serverOrdinal) {
    return TcpServer.getGossipVersionForOrdinal(serverOrdinal);
  }

  @Override
  public boolean isVersionForStreamAtLeast(DataOutput stream, short version) {
    return InternalDataSerializer.getVersionForDataStream(stream).ordinal() >= version;
  }

  @Override
  public boolean isVersionForStreamAtLeast(DataInput stream, short version) {
    return InternalDataSerializer.getVersionForDataStream(stream).ordinal() >= version;
  }

  @Override
  public DataOutputStream getVersionedDataOutputStream(DataOutputStream dos, short version) throws IOException {
    try {
      return new VersionedDataOutputStream(dos, Version.fromOrdinal(version, false));
    } catch (UnsupportedVersionException e) {
      throw new IOException("Unexpected exception during serialization", e);
    }
  }
  
  @Override
  public DataInputStream getVersionedDataInputStream(DataInputStream instream,
      short version) throws IOException {
    try {
      Version mcastVersion = Version.fromOrdinal(version, false);
      return new VersionedDataInputStream(instream, mcastVersion);
    } catch (UnsupportedVersionException e) {
      throw new IOException("Unexpected exception during deserialization", e);
    }
  }
  
  @Override
  public byte[] readByteArray(DataInput in) throws IOException {
    return DataSerializer.readByteArray(in);
  }

  @Override
  public void writeByteArray(byte[] array, DataOutput out) throws IOException {
    DataSerializer.writeByteArray(array, out);
  }

  @Override
  public void writeProperties(Properties props, DataOutput oos)
      throws IOException {
    DataSerializer.writeProperties(props, oos);
  }

  @Override
  public Properties readProperties(DataInput in) throws IOException, ClassNotFoundException {
    return DataSerializer.readProperties(in);
  }

  @Override
  public void checkDisableDNS() {
    if (!SocketCreator.FORCE_DNS_USE) {
      IpAddress.resolve_dns = false; // do not resolve host names since DNS lookup can hang if the NIC fails
      SocketCreator.resolve_dns = false;
    }
  }

  @Override
  public void writeString(String str, DataOutput out) throws IOException {
    DataSerializer.writeString(str, out);
  }
  
  @Override
  public String readString(DataInput in) throws IOException {
    return DataSerializer.readString(in);
  }

  @Override
  public void writeStringArray(String[] roles, DataOutput dao) throws IOException {
    DataSerializer.writeStringArray(roles, dao);
  }
  
  @Override
  public String[] readStringArray(DataInput in) throws IOException {
    return DataSerializer.readStringArray(in);
  }
  
  @Override
  public String getHostName(InetAddress ip_addr) {
    return SocketCreator.getHostName(ip_addr);
  }
  @Override
  public RuntimeException getGemFireConfigException(String string) {
    return new GemFireConfigException(string);
  }
  
  @Override
  public RuntimeException getAuthenticationFailedException(String failReason) {
    return new AuthenticationFailedException(failReason);
  }

  @Override
  public SockCreator getSockCreator() {
    return SocketCreator.getDefaultInstance();
  }

  
  @Override
  public void setDefaultGemFireAttributes(IpAddress addr) {
    MemberAttributes attr = com.gemstone.gemfire.distributed.internal.membership.MemberAttributes.DEFAULT;
    insertGemFireAttributes(addr, attr);
  }

  @Override
  public RuntimeException getDisconnectException(String localizedString) {
    return new DistributedSystemDisconnectedException(localizedString);
  }

  @Override
  public Object getForcedDisconnectException(String localizedString) {
    return new ForcedDisconnectException(localizedString);
  }

  @Override
  public RuntimeException getSystemConnectException(String localizedString) {
    return new SystemConnectException(localizedString);
  }

  @Override
  public InetAddress getLocalHost() throws UnknownHostException {
    return SocketCreator.getLocalHost();
  }

  @Override
  public void setGemFireAttributes(IpAddress addr, Object obj) {
    insertGemFireAttributes(addr, obj);
  }

  @Override
  public String getVmKindString(int vmKind) {
    String vmStr = "";
    switch (vmKind) {
    case DistributionManager.NORMAL_DM_TYPE:
      //vmStr = ":local"; // let this be silent
      break;
    case DistributionManager.LOCATOR_DM_TYPE:
      vmStr = ":locator";
      break;
    case DistributionManager.ADMIN_ONLY_DM_TYPE:
      vmStr = ":admin";
      break;
    case DistributionManager.LONER_DM_TYPE:
      vmStr = ":loner";
      break;
    default:
      //vmStr = ":<unknown:" + vmKind + ">"; // let this be silent for JGroups addresses
      break;
    }
    return vmStr;
  }
  
}
