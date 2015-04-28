/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Message.java,v 1.43 2005/11/07 13:37:23 belaban Exp $

package com.gemstone.org.jgroups;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.org.jgroups.conf.ClassConfigurator;
import java.util.concurrent.ConcurrentHashMap;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;


/**
 * A Message encapsulates data sent to members of a group. It contains among other things the
 * address of the sender, the destination address, a payload (byte buffer) and a list of
 * headers. Headers are added by protocols on the sender side and removed by protocols
 * on the receiver's side.
 * <p>
 * The byte buffer can point to a reference, and we can subset it using index and length. However,
 * when the message is serialized, we only write the bytes between index and length.
 * @author Bela Ban
 */
public class Message implements Externalizable, Streamable {
    // the version to use for multicast messages.  This
    // is currently established by TP.determineMulticastVersion during
    // view processing and defaults to GFXD_10 for multicast discovery
    // during rolling upgrade
    public static volatile short multicastVersion = JGroupsVersion.GFE_71_ORDINAL;
    
    protected Address dest_addr=null;
    protected Address src_addr=null;

    /** The payload */
    private byte[]    buf=null;

    /** The index into the payload (usually 0) */
    protected transient int     offset=0;

    /** The number of bytes in the buffer (usually buf.length is buf not equal to null). */
    protected transient int     length=0;

    /** Map key=String value=Header */
    protected Map headers;
    
    /** GemStoneAddition - serialization version for this message */
    private short version;

    protected static final GemFireTracer log=GemFireTracer.getLog(Message.class);

    static final long serialVersionUID=-1137364035832847034L;

    static final HashSet nonStreamableHeaders=new HashSet(); // todo: remove when all headers are streamable

    /** Map key=Address, value=Address. Maintains mappings to canonical addresses */
//    private static final Map canonicalAddresses=new ConcurrentReaderHashMap();

    /** can this message be enqueued and bundled with other messages?
        some messages shouldn't be bundled - e.g., unordered messages and
        messages requiring a response */
    public transient boolean bundleable; // GemstoneAddition
    
    /** added to help analyse where the time goes in jgroups messaging */
    public transient long timeStamp; // GemStoneAddition
    
    public boolean isCacheOperation; // GemStoneAddition
    public boolean isHighPriority; // GemStoneAddition
    public transient boolean isJoinResponse;

    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     *              not allowed), since we don't copy the contents on clopy() or clone().
     */
    public Message(Address dest, Address src, byte[] buf) {
        dest_addr=dest;
        src_addr=src;
        setBuffer(buf);
        headers=createHeaders(7);
    }

    /**
     * Constructs a message. The index and length parameters allow to provide a <em>reference</em> to
     * a byte buffer, rather than a copy, and refer to a subset of the buffer. This is important when
     * we want to avoid copying. When the message is serialized, only the subset is serialized.
     * @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     * @param src    Address of sender
     * @param buf    A reference to a byte buffer
     * @param offset The index into the byte buffer
     * @param length The number of bytes to be used from <tt>buf</tt>. Both index and length are checked for
     *               array index violations and an ArrayIndexOutOfBoundsException will be thrown if invalid
     */
    public Message(Address dest, Address src, byte[] buf, int offset, int length) {
        dest_addr=dest;
        src_addr=src;
        setBuffer(buf, offset, length);
        headers=createHeaders(7);
    }

    /**
     * GemStoneAddition - set the flag determining whether this is a
     * distributed cache operation message that can be ignored in
     * admin-only virtual machines
     */
    public void setIsDistributedCacheOperation(boolean flag) {
      isCacheOperation = flag;
    }
    
    /**
     * GemStoneAddition - get the flag that states whether this is a
     * distributed cache operation message that can be ignored in
     * admin-only virtual machines
     */
    public boolean getIsDistributedCacheOperation() {
      return isCacheOperation;
    }
    
    /**
     * GemStoneAddition - get the flag that states whether this is a
     * high priority distribution message (unordered execution)
     */
    public boolean isHighPriority() {
      return isHighPriority;
    }
    
    /** Public constructor
     *  @param dest Address of receiver. If it is <em>null</em> or a <em>string</em>, then
     *              it is sent to the group (either to current group or to the group as given
     *              in the string). If it is a Vector, then it contains a number of addresses
     *              to which it must be sent. Otherwise, it contains a single destination.<p>
     *              Addresses are generally untyped (all are of type <em>Object</em>. A channel
     *              instance must know what types of addresses it expects and downcast
     *              accordingly.
     *  @param src  Address of sender
     *  @param obj  The object will be serialized into the byte buffer. <em>Object
     *              has to be serializable </em>! Note that the resulting buffer must not be modified
     *              (e.g. buf[0]=0 is not allowed), since we don't copy the contents on clopy() or clone().
     */
    public Message(Address dest, Address src, Serializable obj) {
        dest_addr=dest;
        src_addr=src;
        setObject(obj);
        headers=createHeaders(7);
    }


    public Message() {
        headers=createHeaders(7);
    }


    public Message(boolean create_headers) {
        if(create_headers)
            headers=createHeaders(7);
    }

    public Address getDest() {
        return dest_addr;
    }

    public void setDest(Address new_dest) {
       dest_addr=canonicalAddress(new_dest);
    }

    public Address getSrc() {
        return src_addr;
    }

    public void setSrc(Address new_src) {
       src_addr=canonicalAddress(new_src);
    }

    /**
     * Returns a <em>reference</em> to the payload (byte buffer). Note that this buffer should not be modified as
     * we do not copy the buffer on copy() or clone(): the buffer of the copied message is simply a reference to
     * the old buffer.<br/>
     * Even if offset and length are used: we return the <em>entire</em> buffer, not a subset.
     */
    public byte[] getRawBuffer() {
        return buf;
    }

    /**
     * Returns a copy of the buffer if offset and length are used, otherwise a reference.
     * @return byte array with a copy of the buffer.
     */
    public byte[] getBuffer() {
        if(buf == null)
            return null;
        if(offset == 0 && length == buf.length)
            return buf;
        else {
            byte[] retval=new byte[length];
            System.arraycopy(buf, offset, retval, 0, length);
            return retval;
        }
    }

    public void setBuffer(byte[] b) {
        buf=b;
        if(buf != null) {
            offset=0;
            length=buf.length;
        }
        else {
            offset=length=0;
        }
    }

    /**
     * Set the internal buffer to point to a subset of a given buffer
     * @param b The reference to a given buffer. If null, we'll reset the buffer to null
     * @param offset The initial position
     * @param length The number of bytes
     */
    public void setBuffer(byte[] b, int offset, int length) {
        buf=b;
        if(buf != null) {
            if(offset < 0 || offset > buf.length)
                throw new ArrayIndexOutOfBoundsException(offset);
            if((offset + length) > buf.length)
                throw new ArrayIndexOutOfBoundsException((offset+length));
            this.offset=offset;
            this.length=length;
        }
        else {
//            offset=length=0; GemStoneAddition (dead stores)
        }
    }

    /** Returns the offset into the buffer at which the data starts */
    public int getOffset() {
        return offset;
    }

    /** Returns the number of bytes in the buffer */
    public int getLength() {
        return length;
    }

    public Map getHeaders() {
        return headers;
    }
    
    public short getDestVersionOrdinal() {
      if (this.version > 0) {
        return JChannel.getGfFunctions().getSerializationVersionOrdinal(version);
      }
      short result = JChannel.getGfFunctions().getCurrentVersionOrdinal();
      if (dest_addr != null && !dest_addr.isMulticastAddress()) {
        if (((IpAddress)dest_addr).getVersionOrdinal() < result) {
          result = JChannel.getGfFunctions().getSerializationVersionOrdinal(((IpAddress)dest_addr).getVersionOrdinal());
        }
      } else {
        result = JChannel.getGfFunctions().getSerializationVersionOrdinal(multicastVersion);
      }
      return result;
    }
        

    public void setObject(Serializable obj) {
        if(obj == null) return;
        byte[] serialized = JChannel.getGfFunctions().serializeWithVersion(obj, getDestVersionOrdinal());
        setBuffer(serialized);
    }

    public <T> T getObject() {
        if(buf == null) return null;
        try {
            ByteArrayInputStream in_stream=new ByteArrayInputStream(buf, offset, length);
            // ObjectInputStream in=new ObjectInputStream(in_stream);
            //ObjectInputStream in=new ContextObjectInputStream(in_stream); // put it back on norbert's request
            return JChannel.getGfFunctions().readObject(new DataInputStream(in_stream)); //in.readObject(); // GemStoneAddition
        }
        catch(Exception ex) {
            // GemStoneAddition - show why we couldn't deserialize the object
            RuntimeException e = new IllegalArgumentException(ex.toString());
            e.initCause(ex);
            throw e;
        }
    }


    /**
     * Nulls all fields of this message so that the message can be reused. Removes all headers from the
     * hashmap, but keeps the hashmap
     */
    public void reset() {
        dest_addr=src_addr=null;
        setBuffer(null);
        headers.clear();
    }

    /*---------------------- Used by protocol layers ----------------------*/

    /** Puts a header given a key into the hashmap. Overwrites potential existing entry. */
    public void putHeader(String key, Header hdr) {
        headers.put(key, hdr);
    }

    public Header removeHeader(String key) {
        return (Header)headers.remove(key);
    }

    public void removeHeaders() {
        headers.clear();
    }

    public <T> T getHeader(String key) {
        return (T)headers.get(key);
    }
    /*---------------------------------------------------------------------*/


    public Message copy() {
        return copy(true);
    }

    /**
     * Create a copy of the message. If offset and length are used (to refer to another buffer), the copy will
     * contain only the subset offset and length point to, copying the subset into the new copy.
     * @param copy_buffer
     * @return Message with specified data
     */
    public Message copy(boolean copy_buffer) {
        Message retval=new Message(false);
        retval.dest_addr=dest_addr;
        retval.src_addr=src_addr;

        if(copy_buffer && buf != null) {

            // change bela Feb 26 2004: we don't resolve the reference
            retval.setBuffer(buf, offset, length);
        }

        retval.headers=createHeaders(headers);
        /** GemStone Addition - copy fields added for GemFire */
        retval.bundleable = bundleable;
        retval.timeStamp = timeStamp;
        retval.isCacheOperation = isCacheOperation;
        retval.isHighPriority = isHighPriority;
        retval.version = version;
        /** end GemStone addition - copy fields added for GemFire */
        return retval;
    }


    @Override // GemStoneAddition
    protected Object clone() throws CloneNotSupportedException {
        return copy();
    }

    public Message makeReply() {
        return new Message(src_addr, null, null);
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer(64);
        ret.append("[dst: ");
        if(dest_addr == null)
            ret.append("<null>");
        else
            ret.append(dest_addr);
        ret.append(", src: ");
        if(src_addr == null)
            ret.append("<null>");
        else
            ret.append(src_addr);

//        int size;
//        if(headers != null && (size=headers.size()) > 0)
//            ret.append(" (").append(size).append(" headers)");

        ret.append(", oob=" + this.isHighPriority);

        ret.append(", size=");
        ret.append(String.valueOf(size()));
//        ret.append(" bytes");
        ret.append(']');

        if(headers != null) {
          for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            ret.append(", ").append(entry.getKey()).append(": ").append(entry.getValue());
          }
        }
        
        //if(buf != null && length > 0)
        //    ret.append(length);
        //else
        //    ret.append('0');
        //GemStoneAddition - print actual message size, not buffer size
        return ret.toString();
    }
    
    public short getVersion() {
      return this.version;
    }
    
    public void setVersion(short v) {
      this.version = v;
    }


    /** Tries to read an object from the message's buffer and prints it */
    public String toStringAsObject() {
        Object obj;

        if(buf == null) return null;
        try {
            obj=getObject();
            return obj != null ? obj.toString() : "";
        }
        catch(Exception e) {  // it is not an object
            return "";
        }
    }


    /**
     * Returns size of buffer, plus some constant overhead for src and dest, plus number of headers time
     * some estimated size/header. The latter is needed because we don't want to marshal all headers just
     * to find out their size requirements. If a header implements Sizeable, the we can get the correct
     * size.<p> Size estimations don't have to be very accurate since this is mainly used by FRAG to
     * determine whether to fragment a message or not. Fragmentation will then serialize the message,
     * therefore getting the correct value.
     */
    public long size() {
      long retval=Global.BYTE_SIZE                  // leading byte
              + length                              // buffer
              + (buf != null? Global.INT_SIZE : 0); // if buf != null 4 bytes for length

      short destVersion = this.version;
      if (destVersion == 0) {
        destVersion = (short)JChannel.getGfFunctions().getCurrentVersionOrdinal(); //Version.CURRENT_ORDINAL;
      }
      if (dest_addr != null) {
        if (0 < ((IpAddress)dest_addr).getVersionOrdinal()  &&  ((IpAddress)dest_addr).getVersionOrdinal() < destVersion) {
          // size() must reflect the size when serialized for the version supported
          // by the destination address.
          destVersion = ((IpAddress)dest_addr).getVersionOrdinal();
        }
      }
      // if(dest_addr != null)
         // retval+=dest_addr.size();
      if(src_addr != null)
          retval+=(src_addr).size(destVersion);

          Map.Entry entry;
          String key;
          Header hdr;
          retval+=Global.SHORT_SIZE; // size (short)
          for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
              entry=(Map.Entry)it.next();
              key=(String)entry.getKey();
              retval+=key.length() +2; // not the same as writeUTF(), but almost
              hdr=(Header)entry.getValue();
              retval+=5; // 1 for presence of magic number, 4 for magic number
              retval+=hdr.size(destVersion);
          }
          if (dest_addr == null || dest_addr.isMulticastAddress()) {
            retval += 3; // Version.uncompressedSize();
          }
      retval += 1; // GemStoneAddition - for isCacheOperation and isDirectAck
      return retval;
  }


    public String printObjectHeaders() {
        StringBuffer sb=new StringBuffer();
        Map.Entry entry;

        if(headers != null) {
            for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
            }
        }
        return sb.toString();
    }



    /* ----------------------------------- Interface Externalizable ------------------------------- */

    public void writeExternal(ObjectOutput out) throws IOException {
        if (true) throw new UnsupportedOperationException("messages are not externalizable");

//        int             len;
//        Externalizable  hdr;
//        Map.Entry       entry;
//        
//        if(dest_addr != null) {
//            out.writeBoolean(true);
//            Marshaller.write(dest_addr, out);
//        }
//        else {
//            out.writeBoolean(false);
//        }
//
//        if(src_addr != null) {
//            out.writeBoolean(true);
//            Marshaller.write(src_addr, out);
//        }
//        else {
//            out.writeBoolean(false);
//        }
//
//        // GemStoneAddition - more flags
//        byte gfFlags = 0;
//        if (isCacheOperation)
//          gfFlags += CACHE_OP;
//        if (isHighPriority)
//          gfFlags += HIGH_PRIORITY;
//        out.write(gfFlags);
//        
//        
//        if(buf == null)
//            out.writeInt(0);
//        else {
//            out.writeInt(length);
//            out.write(buf, offset, length);
//        }
//
//        len=headers.size();
//        out.writeInt(len);
//        // GemStoneAddition - create a versioned stream if src has a different
//        // version than CURRENT
//        Version srcVersion;
//        if (dest_addr != null
//            && !Version.CURRENT.equals(srcVersion = dest_addr.getVersion())) {
//          out = new VersionedObjectOutput(out, srcVersion);
//        }
//        for(Iterator it=headers.entrySet().iterator(); it.hasNext();) {
//            entry=(Map.Entry)it.next();
//            out.writeUTF((String)entry.getKey());
//            hdr=(Externalizable)entry.getValue();
//            Marshaller.write(hdr, out);
//        }
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        throw new UnsupportedOperationException("messages are not externalizable");

//        int      len;
//        boolean  destAddressExist=in.readBoolean();
//        boolean  srcAddressExist;
//        Object   key, value;
//        if(destAddressExist) {
//            dest_addr=(Address)Marshaller.read(in);
//        }
//
//        srcAddressExist=in.readBoolean();
//        if(srcAddressExist) {
//            src_addr=(Address)Marshaller.read(in);
//        }
//        
//        // GemStoneAddition
//        byte gfFlags = in.readByte();
//        if ( (gfFlags & CACHE_OP) != 0 )
//          isCacheOperation = true;
//        if ( (gfFlags & HIGH_PRIORITY) != 0 )
//          isHighPriority = true;
//
//        int i=in.readInt();
//        if(i != 0) {
//            buf=new byte[i];
//            in.readFully(buf);
//            offset=0;
//            length=buf.length;
//        }
//
//        len=in.readInt();
//        // GemStoneAddition - create a versioned stream if src has a different
//        // version than CURRENT
//        Version srcVersion;
//        if (src_addr != null
//            && !Version.CURRENT.equals(srcVersion = src_addr.getVersion())) {
//          in = new VersionedObjectInput(in, srcVersion);
//        }
//        while(len-- > 0) {
//            key=in.readUTF();
//            value=Marshaller.read(in);
//            headers.put(key, value);
//        }
    }

    /* --------------------------------- End of Interface Externalizable ----------------------------- */


    /* ----------------------------------- Interface Streamable  ------------------------------- */

    /**
     * Streams all members (dest and src addresses, buffer and headers) to the output stream.
     * @param out
     * @throws IOException
     */
    public void writeTo(DataOutputStream out) throws IOException {
      JChannel.getGfFunctions().serializeJGMessage(this, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
      JChannel.getGfFunctions().deserializeJGMessage(this, in);
    }



    /* --------------------------------- End of Interface Streamable ----------------------------- */



    /* ----------------------------------- Private methods ------------------------------- */

    public void writeHeader(Header value, DataOutputStream out) throws IOException {
        int magic_number;
        String classname;
        ObjectOutput oos=null;
        try {
            magic_number=ClassConfigurator.getInstance(false).getMagicNumber(value.getClass());
            // write the magic number or the class name
            if(magic_number == -1) {
                out.writeBoolean(false);
                classname=value.getClass().getName();
                out.writeUTF(classname);
            }
            else {
                out.writeBoolean(true);
                out.writeInt(magic_number);
            }

            // write the contents
            if(value instanceof Streamable) {
                ((Streamable)value).writeTo(out);
            }
            else {
                oos = JChannel.getGfFunctions().getObjectOutput(out);
                value.writeExternal(oos);
                if(!nonStreamableHeaders.contains(value.getClass())) {
                    nonStreamableHeaders.add(value.getClass());
                    if(log.isTraceEnabled())
                        log.trace("encountered non-Streamable header: " + value.getClass());
                }
            }
        }
        catch(ChannelException e) {
            log.error(ExternalStrings.Message_FAILED_WRITING_THE_HEADER, e);
        }
        finally {
            if(oos != null)
                oos.close(); // this is a no-op on ByteArrayOutputStream
        }
    }


    public Header readHeader(DataInputStream in) throws IOException {
        Header            hdr;
        boolean           use_magic_number=in.readBoolean();
        int               magic_number;
        String            classname;
        Class             clazz;
        ObjectInput       ois=null;

        try {
            if(use_magic_number) {
                magic_number=in.readInt();
                clazz=ClassConfigurator.getInstance(false).get(magic_number);
                if(clazz == null)
                    log.error(ExternalStrings.Message_MAGIC_NUMBER__0__IS_NOT_AVAILABLE_IN_MAGIC_MAP, magic_number);
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.getInstance(false).get(classname);
            }
            hdr=(Header)clazz.newInstance();
            if(hdr instanceof Streamable) {
               ((Streamable)hdr).readFrom(in);
            }
            else {
                ois = JChannel.getGfFunctions().getObjectInput(in);
                hdr.readExternal(ois);
            }
        }
        catch(Exception ex) {
          IOException e = new IOException(
            ExternalStrings.Message_FAILED_TO_READ_HEADER.toLocalizedString());
          e.initCause(ex);
          throw e;
        }
        finally {
            // if(ois != null) // we cannot close this because other readers depend on it
               // ois.close();
        }
        return hdr;
    }
    
    public void setHeaders(Map hdrs) {
      this.headers = hdrs;
    }

    public Map createHeaders(int size) {
        return size > 0? new ConcurrentHashMap(size) : new ConcurrentHashMap();
    }


    private Map createHeaders(Map m) {
        return new ConcurrentHashMap(m);
    }

    /** canonicalize addresses to some extent.  There are race conditions
     * allowed in this method, so it may not fully canonicalize an address
     * @param nonCanonicalAddress
     * @return canonical representation of the address
     */
    private static Address canonicalAddress(Address nonCanonicalAddress) {
//        Address result=null;
//        if(nonCanonicalAddress == null) {
//            return null;
//        }
//        // do not synchronize between get/put on the canonical map to avoid cost of contention
//        // this can allow multiple equivalent addresses to leak out, but it's worth the cost savings
//        try {
//            result=(Address)canonicalAddresses.get(nonCanonicalAddress);
//        }
//        catch(NullPointerException npe) {
//            // no action needed
//        }
//        if(result == null) {
//            result=nonCanonicalAddress;
//            canonicalAddresses.put(nonCanonicalAddress, result);
//        }
//        return result;
      return nonCanonicalAddress;
    }


    /* ------------------------------- End of Private methods ---------------------------- */

    public void dumpPayload() { // GemStoneAddition - for debugging
      try {
        DataInputStream di = new DataInputStream(new ByteArrayInputStream(buf, offset, length));
        Object o = JChannel.getGfFunctions().readObject(di);
        log.getLogWriter().warning(ExternalStrings.Message_MESSAGEDUMPPAYLOAD__0, o);
      }
      catch (Exception e) {
        log.warn("message.dumpPayload error: " + e.getMessage());
      }
    }

}
