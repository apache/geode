/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Util.java,v 1.62 2005/11/07 08:05:54 belaban Exp $

package com.gemstone.org.jgroups.util;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.conf.ClassConfigurator;
import com.gemstone.org.jgroups.protocols.FD;
import com.gemstone.org.jgroups.protocols.PingHeader;
import com.gemstone.org.jgroups.protocols.UdpHeader;
import com.gemstone.org.jgroups.protocols.pbcast.NakAckHeader;
import com.gemstone.org.jgroups.stack.IpAddress;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * Collection of various utility routines that can not be assigned to other classes.
 */
public class Util  {
    private static final Object mutex=new Object();
    private static final ByteArrayOutputStream out_stream=new ByteArrayOutputStream(512);

    private static  NumberFormat f;

    // constants
    public static final int MAX_PORT=65535; // highest port allocatable
    public static final String DIAG_GROUP="DIAG_GROUP-BELA-322649"; // unique
//    static boolean resolve_dns=false;
    static final String IGNORE_BIND_ADDRESS_PROPERTY="ignore.bind.address";

    static {
        /* Trying to get value of resolve_dns. PropertyPermission not granted if
        * running in an untrusted environment  with JNLP */
// GemStoneAddition - use resolve_dns setting from IpAddress
      //        try {
//            resolve_dns=Boolean.valueOf(System.getProperty("resolve.dns", "false")).booleanValue();
//        }
//        catch (SecurityException ex){
//            resolve_dns=false;
//        }
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }


    public static void closeInputStream(InputStream inp) {
        if(inp != null)
            try {inp.close();} catch(IOException e) {}
    }

    public static void closeOutputStream(OutputStream out) {
        if(out != null) {
            try {out.close();} catch(IOException e) {}
        }
    }

    /**
     * Insert an element into an array.  The element is inserted at the
     * given position, all elements afterwards are moved to the right.
     *
     * @param originalArray array to insert into
     * @param pos position at which to insert the element
     * @param element element to add
     * @return the new array
     */
    public static Object[] insert(Object[] originalArray, int pos, Object element) {
      Object[] newArray = (Object[]) java.lang.reflect.Array.newInstance(
        originalArray.getClass().getComponentType(), originalArray.length + 1);

      // Test Cases (proof of correctness by examining corner cases)
      // 1) A B C D insert at 0: expect X A B C D
      // 2) A B C D insert at 2: expect A B X C D
      // 3) A B C D insert at 4: expect A B C D X

      // copy everything before the given position
      if (pos > 0) {
        System.arraycopy(originalArray, 0, newArray, 0, pos); // does not copy originalArray[pos], where we insert
      }

      // 1) A B C D insert at 0: no change, ". . . . ."
      // 2) A B C D insert at 2: copy "A B", "A B . . ."
      // 3) A B C D insert at 4: copy "A B C D", "A B C D ."

      // insert
      newArray[pos] = element;

      // 1) A B C D insert at 0: "X . . . ."
      // 2) A B C D insert at 2: "A B X . ."
      // 3) A B C D insert at 4: "A B C D X" (all done)

      // copy remaining elements
      if (pos < originalArray.length) {
        System.arraycopy(originalArray, pos, // originalArray[pos] first element copied
            newArray, pos + 1, // newArray[pos + 1] first destination
            originalArray.length - pos); // number of elements left
      }

      // 1) A B C D insert at 0: "A B C D" copied at 1: "X A B C D"
      // 2) A B C D insert at 2: "C D" copied at 3: "A B X C D"
      // 3) A B C D insert at 4: no change
      return newArray;
    }

    /**
     * Remove element from an array.  The element is removed at the
     * specified position, and all remaining elements are moved to the left.
     *
     * @param originalArray array to remove from
     * @param pos position to remove
     * @return the new array
     */
    public static Object[] remove(Object[] originalArray, int pos) {
      Object[] newArray = (Object[])java.lang.reflect.Array.newInstance(
        originalArray.getClass().getComponentType(), originalArray.length - 1);

      // Test cases: (proof of correctness)
      // 1) A B C D E remove 0: expect "B C D E"
      // 2) A B C D E remove 2: expect "A B D E"
      // 3) A B C D E remove 4: expect "A B C D"

      // Copy everything before
      if (pos > 0) {
        System.arraycopy(originalArray, 0, newArray, 0, pos); // originalArray[pos - 1] is last element copied
      }

      // 1) A B C D E remove 0: no change, ". . . ."
      // 2) A B C D E remove 2: "A B"  copied at beginning: "A B . ."
      // 3) A B C D E remove 4: "A B C D" copied (all done)

      // Copy everything after
      if (pos < originalArray.length - 1) {
        System.arraycopy(originalArray, pos + 1, // originalArray[pos + 1] is first element copied
            newArray, pos, // first position to copy into
            originalArray.length - 1 - pos);
      }

      // 1) A B C D E remove 0: "B C D E" copied into to position 0
      // 2) A B C D E remove 2: "D E" copied into position 2: "A B D E"
      // 3) A B C D E remove 4: no change
      return newArray;
    }


    /**
     * Creates an object from a byte buffer
     */
    public static Object objectFromByteBuffer(byte[] buffer) throws IOException, ClassNotFoundException {
        synchronized(mutex) {
            if(buffer == null) return null;
            Object retval=null;
            ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer);
            //ObjectInputStream in=new ContextObjectInputStream(in_stream); // changed Nov 29 2004 (bela)
            //retval=in.readObject();
            retval = JChannel.getGfFunctions().readObject(new DataInputStream(in_stream));
            //in.close();
            if(retval == null)
                return null;
            return retval;
        }
    }

    /**
     * Serializes an object into a byte buffer.
     * The object has to implement interface Serializable or Externalizable
     */
    public static byte[] objectToByteBuffer(Object obj) throws IOException {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            //ObjectOutputStream out=new ObjectOutputStream(out_stream);
            //out.writeObject(obj);
            JChannel.getGfFunctions().writeObject(obj, new DataOutputStream(out_stream));
            result=out_stream.toByteArray();
            //out.close();
        }
        return result;
    }


     public static Streamable streamableFromByteBuffer(Class cl, byte[] buffer) throws Exception {
        synchronized(mutex) {
            if(buffer == null) return null;
            Streamable retval=null;
            ByteArrayInputStream in_stream=new ByteArrayInputStream(buffer);
            DataInputStream in=new DataInputStream(in_stream); // changed Nov 29 2004 (bela)
            retval=(Streamable)cl.newInstance();
            retval.readFrom(in);
            in.close();
//            if(retval == null) GemStoneAddition not possible
//                return null;
            return retval;
        }
    }

    public static byte[] streamableToByteBuffer(Streamable obj) throws Exception {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            DataOutputStream out=new DataOutputStream(out_stream);
            obj.writeTo(out);
            result=out_stream.toByteArray();
            out.close();
        }
        return result;
    }


    public static byte[] collectionToByteBuffer(Collection c) throws Exception {
        byte[] result=null;
        synchronized(out_stream) {
            out_stream.reset();
            DataOutputStream out=new DataOutputStream(out_stream);
            Util.writeAddresses(c, out);
            result=out_stream.toByteArray();
            out.close();
        }
        return result;
    }

    public static int size(Address addr, short version) {
        int retval=Global.BYTE_SIZE; // presence byte
        if(addr != null)
            retval+=addr.size(version) + Global.BYTE_SIZE; // plus type of address
        return retval;
    }


    public static void writeAddress(Address addr, DataOutputStream out) throws IOException {
        if(addr == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        if(addr instanceof IpAddress) {
            // regular case, we don't need to include class information about the type of Address, e.g. JmsAddress
            out.writeBoolean(true);
            addr.writeTo(out);
        }
        else {
            out.writeBoolean(false);
            writeOtherAddress(addr, out);
        }
    }



    public static Address readAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        Address addr=null;
        if(in.readBoolean() == false)
            return null;
        if(in.readBoolean()) {
            addr=new IpAddress();
            addr.readFrom(in);
        }
        else {
            addr=readOtherAddress(in);
        }
        return addr;
    }

    @SuppressFBWarnings(value="DE_MIGHT_IGNORE", justification="the exception is logged")
    private static Address readOtherAddress(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        ClassConfigurator conf=null;
        try {conf=ClassConfigurator.getInstance(false);} catch(Exception e) {}
        int b=in.read();
        int magic_number;
        String classname;
        Class cl=null;
        Address addr;
        if(b == 1) {
            magic_number=in.readInt();
            cl=conf.get(magic_number);
        }
        else {
            classname=in.readUTF();
            // GemStoneAddition - need to know what classes are failing to load here
            try {
              cl=conf.get(classname);
            }
            catch (Exception ce) {
              GemFireTracer.getLog(Util.class).error(ExternalStrings.Util_UNABLE_TO_LOAD_CLASS_NAMED__0_, classname, ce);
            }
            if (cl == null) {
              GemFireTracer.getLog(Util.class).error(ExternalStrings.Util_UNABLE_TO_LOAD_CLASS_NAMED__0_, classname, new Exception());
            }
        }
        addr=(Address)cl.newInstance();
        addr.readFrom(in);
        return addr;
    }

    @SuppressFBWarnings(value="DE_MIGHT_IGNORE", justification="exception causes 'non-magic' serialization to be used")
    private static void writeOtherAddress(Address addr, DataOutputStream out) throws IOException {
        ClassConfigurator conf=null;
        try {conf=ClassConfigurator.getInstance(false);} catch(Exception e) {}
        int magic_number=conf != null? conf.getMagicNumber(addr.getClass()) : -1;

        // write the class info
        if(magic_number == -1) {
            out.write(0);
            out.writeUTF(addr.getClass().getName());
        }
        else {
            out.write(1);
            out.writeInt(magic_number);
        }

        // write the data itself
        addr.writeTo(out);
    }

    /**
     * Writes a Vector of Addresses. Can contain 65K addresses at most
     * @param v A Collection<Address>
     * @param out
     * @throws IOException
     */
    public static void writeAddresses(Collection v, DataOutputStream out) throws IOException {
        if(v == null) {
            out.writeShort(-1);
            return;
        }
        out.writeShort(v.size());
        Address addr;
        for(Iterator it=v.iterator(); it.hasNext();) {
            addr=(Address)it.next();
            Util.writeAddress(addr, out);
        }
    }

    /**
     *
     * @param in
     * @param cl The type of Collection, e.g. Vector.class
     * @return Collection of Address objects
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static Collection readAddresses(DataInputStream in, Class cl) throws IOException, IllegalAccessException, InstantiationException {
        short length=in.readShort();
        if(length < 0) return null;
        Collection retval=(Collection)cl.newInstance();
        Address addr;
        for(int i=0; i < length; i++) {
            addr=Util.readAddress(in);
            retval.add(addr);
        }
        return retval;
    }


    /**
     * Returns the marshalled size of a Collection of Addresses.
     * <em>Assumes elements are of the same type !</em>
     * @param addrs Collection<Address>
     * @return long size
     */
    public static long size(Collection addrs, short version) {
        int retval=Global.SHORT_SIZE; // number of elements
        if(addrs != null && addrs.size() > 0) {
            Address addr=(Address)addrs.iterator().next();
            retval+=addr.size(version) * addrs.size();
        }
        return retval;
    }




    public static void writeStreamable(Streamable obj, DataOutputStream out) throws IOException {
        if(obj == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        obj.writeTo(out);
    }


    public static Streamable readStreamable(Class clazz, DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        Streamable retval=null;
        if(in.readBoolean() == false)
            return null;
        retval=(Streamable)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }


    public static void writeGenericStreamable(Streamable obj, DataOutputStream out) throws IOException {
        int magic_number;
        String classname;

        if(obj == null) {
            out.write(0);
            return;
        }

        try {
            out.write(1);
            magic_number=ClassConfigurator.getInstance(false).getMagicNumber(obj.getClass());
            // write the magic number or the class name
            if(magic_number == -1) {
                out.write(0);
                classname=obj.getClass().getName();
                out.writeUTF(classname);
            }
            else {
                out.write(1);
                out.writeInt(magic_number);
            }

            // write the contents
            obj.writeTo(out);
        }
        catch(ChannelException e) {
            throw new IOException("failed writing object of type " + obj.getClass() + " to stream: " + e.toString());
        }
    }



    public static Streamable readGenericStreamable(DataInputStream in) throws IOException {
        Streamable retval=null;
        int b=in.read();
        if(b == 0)
            return null;

        int use_magic_number=in.read(), magic_number;
        String classname;
        Class clazz;

        try {
            if(use_magic_number == 1) {
                magic_number=in.readInt();
                clazz=ClassConfigurator.getInstance(false).get(magic_number);
            }
            else {
                classname=in.readUTF();
                clazz=ClassConfigurator.getInstance(false).get(classname);
            }

            retval=(Streamable)clazz.newInstance();
            retval.readFrom(in);
            return retval;
        }
        catch(Exception ex) {
            throw new IOException("failed reading object: " + ex.toString());
        }
    }

    public static void writeObject(Object obj, DataOutputStream out) throws IOException {
       if(obj == null || !(obj instanceof Streamable)) {
           byte[] buf=objectToByteBuffer(obj);
           out.writeShort(buf.length);
           out.write(buf, 0, buf.length);
       }
       else {
           out.writeShort(-1);
           writeGenericStreamable((Streamable)obj, out);
       }
    }

    public static Object readObject(DataInputStream in) throws IOException, ClassNotFoundException {
        short len=in.readShort();
        Object retval=null;
        if(len == -1) {
            retval=readGenericStreamable(in);
        }
        else {
            byte[] buf=new byte[len];
            in.readFully(buf, 0, len);
            retval=objectFromByteBuffer(buf);
        }
        return retval;
    }



    public static void writeString(String s, DataOutputStream out) throws IOException {
        if(s != null) {
            out.write(1);
            out.writeUTF(s);
        }
        else {
            out.write(0);
        }
    }

    public static String readString(DataInputStream in) throws IOException {
        int b=in.read();
        if(b == 1)
            return in.readUTF();
        return null;
    }

    public static void writeByteBuffer(byte[] buf, DataOutputStream out) throws IOException {
        if(buf != null) {
            out.write(1);
            out.writeInt(buf.length);
            out.write(buf, 0, buf.length);
        }
        else {
            out.write(0);
        }
    }

    public static byte[] readByteBuffer(DataInputStream in) throws IOException {
        int b=in.read();
        if(b == 1) {
            b=in.readInt();
            byte[] buf=new byte[b];
            if (buf.length != in.read(buf, 0, buf.length)) {
              throw new IOException("Failed to read " + buf.length + "bytes"); // GemStoneAddition
            }
            return buf;
        }
        return null;
    }


    /**
       * Marshalls a list of messages.
       * @param xmit_list LinkedList<Message>
     * @param dest TODO
       * @return Buffer
       * @throws IOException
       */
    public static Buffer msgListToByteBuffer(List xmit_list, Address dest) throws IOException {
        ExposedByteArrayOutputStream output=new ExposedByteArrayOutputStream(512);
        DataOutputStream out=new DataOutputStream(output);
        Message msg;
        Buffer retval=null;

        out.writeInt(xmit_list.size());
        for(Iterator it=xmit_list.iterator(); it.hasNext();) {
            msg=(Message)it.next();
            msg.setDest(null); // GemStoneAddition - never include dest addresses in bundled messages
            msg.setVersion(dest.getVersionOrdinal());
            msg.writeTo(out);
        }
        out.flush();
        retval=new Buffer(output.getRawBuffer(), 0, output.size());
        out.close();
        output.close();
        return retval;
    }

    // GemStoneAddition - dest parameter added
    public static LinkedList byteBufferToMessageList(byte[] buffer, int offset, int length) throws Exception { // GemStoneAddition - dest address
        LinkedList retval=null;
        ByteArrayInputStream input=new ByteArrayInputStream(buffer, offset, length);
        DataInputStream in = new DataInputStream(input);
        int size=in.readInt();

        if(size == 0)
            return null;

        Message msg;
        retval=new LinkedList();
        for(int i=0; i < size; i++) {
            msg=new Message(false); // don't create headers, readFrom() will do this
            msg.readFrom(in);
            retval.add(msg);
        }

        return retval;
    }





    public static boolean match(Object obj1, Object obj2) {
        if(obj1 == null && obj2 == null)
            return true;
        if(obj1 != null)
            return obj1.equals(obj2);
        else
            return obj2.equals(obj1);
    }


    public static boolean match(long[] a1, long[] a2) {
        if(a1 == null && a2 == null)
            return true;
        if(a1 == null || a2 == null)
            return false;

        if(a1 == a2) // identity
            return true;

        // at this point, a1 != null and a2 != null
        if(a1.length != a2.length)
            return false;

        for(int i=0; i < a1.length; i++) {
            if(a1[i] != a2[i])
                return false;
        }
        return true;
    }

    /** Sleep for timeout msecs. Returns when timeout has elapsed or thread was interrupted */
    public static void sleep(long timeout) throws InterruptedException {
//        try {
            Thread.sleep(timeout);
//        }
//        catch(Exception e) { // GemStoneAddition
//        }
    }


    /**
     * On most UNIX systems, the minimum sleep time is 10-20ms. Even if we specify sleep(1), the thread will
     * sleep for at least 10-20ms. On Windows, sleep() seems to be implemented as a busy sleep, that is the
     * thread never relinquishes control and therefore the sleep(x) is exactly x ms long.
     */
    public static void sleep(long msecs, boolean busy_sleep)
    throws InterruptedException // GemStoneAddition
    {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        if(!busy_sleep) {
            sleep(msecs);
            return;
        }

        long start=System.currentTimeMillis();
        long stop=start + msecs;

        while(stop > start) {
            start=System.currentTimeMillis();
        }
    }


    /** Returns a random value in the range [1 - range] */
    public static long random(long range) {
        return (long)((Math.random() * 100000) % range) + 1;
    }


    /** Sleeps between 1 and timeout milliseconds, chosen randomly. Timeout must be > 1 */
    public static void sleepRandom(long timeout)
    throws InterruptedException // GemStoneAddition
    {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        if(timeout <= 0) {
            return;
        }

        long r=(int)((Math.random() * 100000) % timeout) + 1;
        sleep(r);
    }


    /**
     Tosses a coin weighted with probability and returns true or false. Example: if probability=0.8,
     chances are that in 80% of all cases, true will be returned and false in 20%.
     */
    public static boolean tossWeightedCoin(double probability) {
        long r=random(100);
        long cutoff=(long)(probability * 100);
        if(r < cutoff)
            return true;
        else
            return false;
    }


    public static String getHostname() {
        try {
            return JChannel.getGfFunctions().getLocalHost().getHostName();
        }
        catch(Exception ex) {
        }
        return "localhost";
    }


    public static void dumpStack(boolean exit) {
        try {
            throw new Exception("Dumping stack:");
        }
        catch(Exception e) {
            e.printStackTrace();
            //if(exit)
            //    System.exit(0);
        }
    }


    /**
     * Debugging method used to dump the content of a protocol queue in a condensed form. Useful
     * to follow the evolution of the queue's content in time.
     */
    public static String dumpQueue(Queue q) {
        StringBuffer sb=new StringBuffer();
        LinkedList values=q.values();
        if(values.size() == 0) {
            sb.append("empty");
        }
        else {
            for(Iterator it=values.iterator(); it.hasNext();) {
                Object o=it.next();
                String s=null;
                if(o instanceof Event) {
                    Event event=(Event)o;
                    int type=event.getType();
                    s=Event.type2String(type);
                    if(type == Event.VIEW_CHANGE)
                        s+=" " + event.getArg();
                    if(type == Event.MSG)
                        s+=" " + event.getArg();

                    if(type == Event.MSG) {
                        s+="[";
                        Message m=(Message)event.getArg();
                        Map headers=m.getHeaders();
                        for(Iterator i=headers.entrySet()/*keySet() GemStoneAddition*/.iterator(); i.hasNext();) {
                          Map.Entry entry = (Map.Entry)i.next();
                            Object headerKey=entry.getKey(); // i.next();
                            Object value=entry.getValue(); // headers.get(headerKey);
                            String headerToString=null;
                            if(value instanceof FD.FdHeader) {
                                headerToString=value.toString();
                            }
                            else
                                if(value instanceof PingHeader) {
                                    headerToString=headerKey + "-";
                                    if(((PingHeader)value).type == PingHeader.GET_MBRS_REQ) {
                                        headerToString+="GMREQ";
                                    }
                                    else
                                        if(((PingHeader)value).type == PingHeader.GET_MBRS_RSP) {
                                            headerToString+="GMRSP";
                                        }
                                        else {
                                            headerToString+="UNKNOWN";
                                        }
                                }
                                else {
                                    headerToString=headerKey + "-" + (value == null ? "null" : value.toString());
                                }
                            s+=headerToString;

                            if(i.hasNext()) {
                                s+=",";
                            }
                        }
                        s+="]";
                    }
                }
                else {
                    s=o.toString();
                }
                sb.append(s).append("\n");
            }
        }
        return sb.toString();
    }


    /**
     * Use with caution: lots of overhead
     */
    public static String printStackTrace(Throwable t) {
        StringWriter s=new StringWriter();
        PrintWriter p=new PrintWriter(s);
        t.printStackTrace(p);
        return s.toString();
    }

    public static String getStackTrace(Throwable t) {
        return printStackTrace(t);
    }



    public static String print(Throwable t) {
        return printStackTrace(t);
    }


    public static void crash() {
        //System.exit(-1);
    }


    public static String printEvent(Event evt) {
        Message msg;

        if(evt.getType() == Event.MSG) {
            msg=(Message)evt.getArg();
            if(msg != null) {
                if(msg.getLength() > 0)
                    return printMessage(msg);
                else
                    return msg.printObjectHeaders();
            }
        }
        return evt.toString();
    }


    /** Tries to read an object from the message's buffer and prints it */
    public static String printMessage(Message msg) {
        if(msg == null)
            return "";
        if(msg.getLength() == 0)
            return null;

        try {
            return msg.getObject().toString();
        }
        catch(Exception e) {  // it is not an object
            return "";
        }

    }


    /** Tries to read a <code>MethodCall</code> object from the message's buffer and prints it.
     Returns empty string if object is not a method call */
    public static String printMethodCall(Message msg) {
        Object obj;
        if(msg == null)
            return "";
        if(msg.getLength() == 0)
            return "";

        try {
            obj=msg.getObject();
            return obj.toString();
        }
        catch(Exception e) {  // it is not an object
            return "";
        }

    }


    public static void printThreads() {
        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        System.out.println("------- Threads -------");
        for(int i=0; i < threads.length; i++) {
            System.out.println("#" + i + ": " + threads[i]);
        }
        System.out.println("------- Threads -------\n");
    }


    public static String activeThreads() {
        StringBuffer sb=new StringBuffer();
        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        sb.append("------- Threads -------\n");
        for(int i=0; i < threads.length; i++) {
            sb.append("#").append(i).append(": ").append(threads[i]).append('\n');
        }
        sb.append("------- Threads -------\n");
        return sb.toString();
    }

    public static String printBytes(long bytes) {
        double tmp;

        if(bytes < 1000)
            return bytes + "b";
        if(bytes < 1000000) {
            tmp=bytes / 1000.0;
            return f.format(tmp) + "KB";
        }
        if(bytes < 1000000000) {
            tmp=bytes / 1000000.0;
            return f.format(tmp) + "MB";
        }
        else {
            tmp=bytes / 1000000000.0;
            return f.format(tmp) + "GB";
        }
    }


    /**
     Fragments a byte buffer into smaller fragments of (max.) frag_size.
     Example: a byte buffer of 1024 bytes and a frag_size of 248 gives 4 fragments
     of 248 bytes each and 1 fragment of 32 bytes.
     @return An array of byte buffers (<code>byte[]</code>).
     */
    public static byte[][] fragmentBuffer(byte[] buf, int frag_size, final int length) {
        byte[] retval[];
        int accumulated_size=0;
        byte[] fragment;
        int tmp_size=0;
        int num_frags;
        int index=0;

        num_frags=length % frag_size == 0 ? length / frag_size : length / frag_size + 1;
        retval=new byte[num_frags][];

        while(accumulated_size < length) {
            if(accumulated_size + frag_size <= length)
                tmp_size=frag_size;
            else
                tmp_size=length - accumulated_size;
            fragment=new byte[tmp_size];
            System.arraycopy(buf, accumulated_size, fragment, 0, tmp_size);
            retval[index++]=fragment;
            accumulated_size+=tmp_size;
        }
        return retval;
    }

    public static byte[][] fragmentBuffer(byte[] buf, int frag_size) {
        return fragmentBuffer(buf, frag_size, buf.length);
    }



    /**
     * Given a buffer and a fragmentation size, compute a list of fragmentation offset/length pairs, and
     * return them in a list. Example:<br/>
     * Buffer is 10 bytes, frag_size is 4 bytes. Return value will be ({0,4}, {4,4}, {8,2}).
     * This is a total of 3 fragments: the first fragment starts at 0, and has a length of 4 bytes, the second fragment
     * starts at offset 4 and has a length of 4 bytes, and the last fragment starts at offset 8 and has a length
     * of 2 bytes.
     * @param frag_size
     * @return List. A List<Range> of offset/length pairs
     */
    public static java.util.List computeFragOffsets(int offset, int length, int frag_size) {
        java.util.List   retval=new ArrayList();
        long   total_size=length + offset;
        int    index=offset;
        int    tmp_size=0;
        Range  r;

        while(index < total_size) {
            if(index + frag_size <= total_size)
                tmp_size=frag_size;
            else
                tmp_size=(int)(total_size - index);
            r=new Range(index, tmp_size);
            retval.add(r);
            index+=tmp_size;
        }
        return retval;
    }

    public static java.util.List computeFragOffsets(byte[] buf, int frag_size) {
        return computeFragOffsets(0, buf.length, frag_size);
    }

    /**
     Concatenates smaller fragments into entire buffers.
     @param fragments An array of byte buffers (<code>byte[]</code>)
     @return A byte buffer
     */
    public static byte[] defragmentBuffer(byte[] fragments[]) {
        int total_length=0;
        byte[] ret;
        int index=0;

        if(fragments == null) return null;
        for(int i=0; i < fragments.length; i++) {
            if(fragments[i] == null) continue;
            total_length+=fragments[i].length;
        }
        ret=new byte[total_length];
        for(int i=0; i < fragments.length; i++) {
            if(fragments[i] == null) continue;
            System.arraycopy(fragments[i], 0, ret, index, fragments[i].length);
            index+=fragments[i].length;
        }
        return ret;
    }


    public static void printFragments(byte[] frags[]) {
        for(int i=0; i < frags.length; i++)
            System.out.println('\'' + new String(frags[i]) + '\'');
    }



//      /**
//         Peeks for view on the channel until n views have been received or timeout has elapsed.
//         Used to determine the view in which we want to start work. Usually, we start as only
//         member in our own view (1st view) and the next view (2nd view) will be the full view
//         of all members, or a timeout if we're the first member. If a non-view (a message or
//         block) is received, the method returns immediately.
//         @param channel The channel used to peek for views. Has to be operational.
//         @param number_of_views The number of views to wait for. 2 is a good number to ensure that,
//                if there are other members, we start working with them included in our view.
//         @param timeout Number of milliseconds to wait until view is forced to return. A value
//                of <= 0 means wait forever.
//       */
//      public static View peekViews(Channel channel, int number_of_views, long timeout) {
//  	View     retval=null;
//  	Object   obj=null;
//  	int      num=0;
//  	long     start_time=System.currentTimeMillis();

//  	if(timeout <= 0) {
//  	    while(true) {
//  		try {
//  		    obj=channel.peek(0);
//  		    if(obj == null || !(obj instanceof View))
//  			break;
//  		    else {
//  			retval=(View)channel.receive(0);
//  			num++;
//  			if(num >= number_of_views)
//  			    break;
//  		    }
//  		}
//  		catch(Exception ex) {
//  		    break;
//  		}
//  	    }
//  	}
//  	else {
//  	    while(timeout > 0) {
//  		try {
//  		    obj=channel.peek(timeout);
//  		    if(obj == null || !(obj instanceof View))
//  			break;
//  		    else {
//  			retval=(View)channel.receive(timeout);
//  			num++;
//  			if(num >= number_of_views)
//  			    break;
//  		    }
//  		}
//  		catch(Exception ex) {
//  		    break;
//  		}
//  		timeout=timeout - (System.currentTimeMillis() - start_time);
//  	    }
//  	}

//  	return retval;
//      }




    public static String array2String(long[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(int[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }

        ret.append(']');
        return ret.toString();
    }

    public static String array2String(boolean[] array) {
        StringBuffer ret=new StringBuffer("[");

        if(array != null) {
            for(int i=0; i < array.length; i++)
                ret.append(array[i]).append(" ");
        }
        ret.append(']');
        return ret.toString();
    }


    /**
     * Selects a random subset of members according to subset_percentage and returns them.
     * Picks no member twice from the same membership. If the percentage is smaller than 1 -> picks 1 member.
     */
    public static Vector pickSubset(Vector members, double subset_percentage) {
        Vector ret=new Vector(), tmp_mbrs;
        int num_mbrs=members.size(), subset_size, index;

        if(num_mbrs == 0) return ret;
        subset_size=(int)Math.ceil(num_mbrs * subset_percentage);

        tmp_mbrs=(Vector)members.clone();

        for(int i=subset_size; i > 0 && tmp_mbrs.size() > 0; i--) {
            index=(int)((Math.random() * num_mbrs) % tmp_mbrs.size());
            ret.addElement(tmp_mbrs.elementAt(index));
            tmp_mbrs.removeElementAt(index);
        }

        return ret;
    }


    public static Object pickRandomElement(Vector list) {
        if(list == null) return null;
        int size=list.size();
        int index=(int)((Math.random() * size * 10) % size);
        return list.get(index);
    }


    /**
     * Returns all members that left between 2 views. All members that are element of old_mbrs but not element of
     * new_mbrs are returned.
     */
    public static Vector determineLeftMembers(Vector old_mbrs, Vector new_mbrs) {
        Vector retval=new Vector();
        Object mbr;

        if(old_mbrs == null || new_mbrs == null)
            return retval;

        for(int i=0; i < old_mbrs.size(); i++) {
            mbr=old_mbrs.elementAt(i);
            if(!new_mbrs.contains(mbr))
                retval.addElement(mbr);
        }

        return retval;
    }


    public static String printMembers(Vector v) {
        StringBuffer sb=new StringBuffer("(");
        boolean first=true;
        Object el;

        if(v != null) {
            for(int i=0; i < v.size(); i++) {
                if(!first)
                    sb.append(", ");
                else
                    first=false;
                el=v.elementAt(i);
                if(el instanceof Address)
                    sb.append(el);
                else
                    sb.append(el);
            }
        }
        sb.append(')');
        return sb.toString();
    }


    /**
     Makes sure that we detect when a peer connection is in the closed state (not closed while we send data,
     but before we send data). Two writes ensure that, if the peer closed the connection, the first write
     will send the peer from FIN to RST state, and the second will cause a signal (IOException).
     */
    public static void doubleWrite(byte[] buf, OutputStream out) throws Exception {
        if(buf.length > 1) {
            out.write(buf, 0, 1);
            out.write(buf, 1, buf.length - 1);
        }
        else {
            out.write(buf, 0, 0);
            out.write(buf);
        }
    }


    /**
     Makes sure that we detect when a peer connection is in the closed state (not closed while we send data,
     but before we send data). Two writes ensure that, if the peer closed the connection, the first write
     will send the peer from FIN to RST state, and the second will cause a signal (IOException).
     */
    public static void doubleWrite(byte[] buf, int offset, int length, OutputStream out) throws Exception {
        if(length > 1) {
            out.write(buf, offset, 1);
            out.write(buf, offset+1, length - 1);
        }
        else {
            out.write(buf, offset, 0);
            out.write(buf, offset, length);
        }
    }

    /**
    * if we were to register for OP_WRITE and send the remaining data on
    * readyOps for this channel we have to either block the caller thread or
    * queue the message buffers that may arrive while waiting for OP_WRITE.
    * Instead of the above approach this method will continuously write to the
    * channel until the buffer sent fully.
    */
    public static void writeFully(ByteBuffer buf, WritableByteChannel out) throws IOException {
        int written = 0;
        int toWrite = buf.limit();
        while (written < toWrite) {
            written += out.write(buf);
        }
    }

//    /* double writes are not required.*/
//	public static void doubleWriteBuffer(
//		ByteBuffer buf,
//		WritableByteChannel out)
//		throws Exception
//	{
//		if (buf.limit() > 1)
//		{
//			int actualLimit = buf.limit();
//			buf.limit(1);
//			writeFully(buf,out);
//			buf.limit(actualLimit);
//			writeFully(buf,out);
//		}
//		else
//		{
//			buf.limit(0);
//			writeFully(buf,out);
//			buf.limit(1);
//			writeFully(buf,out);
//		}
//	}


    public static long sizeOf(String classname) {
        Object inst;
        byte[] data;

        try {
            inst=Util.loadClass(classname, null).newInstance();
            data=Util.objectToByteBuffer(inst);
            return data.length;
        }
        catch(Exception ex) {
            return -1;
        }
    }


    public static long sizeOf(Object inst) {
        byte[] data;

        try {
            data=Util.objectToByteBuffer(inst);
            return data.length;
        }
        catch(Exception ex) {
            return -1;
        }
    }

    public static long sizeOf(Streamable inst) {
        byte[] data;
        ByteArrayOutputStream output;
        DataOutputStream out;

        try {
            output=new ByteArrayOutputStream();
            out=new DataOutputStream(output);
            inst.writeTo(out);
            out.flush();
            data=output.toByteArray();
            return data.length;
        }
        catch(Exception ex) {
            return -1;
        }
    }



    /**
     * Tries to load the class from the current thread's context class loader. If
     * not successful, tries to load the class from the current instance.
     * @param classname Desired class.
     * @param clazz Class object used to obtain a class loader
     * 				if no context class loader is available.
     * @return Class, or null on failure.
     */
    public static Class loadClass(String classname, Class clazz) throws ClassNotFoundException {
        ClassLoader loader;
        
        // TODO - is this needed?  ClassPathLoader is a GemFire service and not available
        // in JGroups
//        try {
//          return ClassPathLoader.getLatest().forName(classname);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//        }

        if(clazz != null) {
            try {
                loader=clazz.getClassLoader();
                if(loader != null) {
                  // Using forName instead of loadClass to fix bug 36502
                  return Class.forName(classname, false, loader);
                }
            }
            catch (VirtualMachineError err) { // GemStoneAddition
              // If this ever returns, rethrow the error.  We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            catch(Throwable t) {
            }
        }

        try {
            loader=ClassLoader.getSystemClassLoader();
            if(loader != null) {
              // Using forName instead of loadClass to fix bug 36502
              return Class.forName(classname, false, loader);
            }
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable t) {
        }

        //GemFireTracer.getLog(Util.class).error("Unable to load class named '" + classname + "'");
        throw new ClassNotFoundException(classname);
    }


    public static InputStream getResourceAsStream(String name, Class clazz) {
        ClassLoader loader;

        // TODO - is this needed?  ClassPathLoader is a GemFire service and
        // is not available in JGroups
//        try {
//          return ClassPathLoader.getLatest().getResourceAsStream(name);
//        }
//        catch (VirtualMachineError err) { // GemStoneAddition
//          // If this ever returns, rethrow the error.  We're poisoned
//          // now, so don't let this thread continue.
//          throw err;
//        }
//        catch(Throwable t) {
//        }

        if(clazz != null) {
            try {
                loader=clazz.getClassLoader();
                if(loader != null) {
                    return loader.getResourceAsStream(name);
                }
            }
            catch (VirtualMachineError err) { // GemStoneAddition
              // If this ever returns, rethrow the error.  We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            catch(Throwable t) {
            }
        }
        try {
            loader=ClassLoader.getSystemClassLoader();
            if(loader != null) {
                return loader.getResourceAsStream(name);
            }
        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable t) {
        }

        return null;
    }


    /** Checks whether 2 Addresses are on the same host */
    public static boolean sameHost(Address one, Address two) {
        InetAddress a, b;
        String host_a, host_b;

        if(one == null || two == null) return false;
        if(!(one instanceof IpAddress) || !(two instanceof IpAddress)) {
            return false;
        }

        a=((IpAddress)one).getIpAddress();
        b=((IpAddress)two).getIpAddress();
        if(a == null || b == null) return false;
        host_a=a.getHostAddress();
        host_b=b.getHostAddress();

        // System.out.println("host_a=" + host_a + ", host_b=" + host_b);
        return host_a.equals(host_b);
    }



    public static boolean fileExists(String fname) {
        return (new File(fname)).exists();
    }


    /**
     * Parses comma-delimited longs; e.g., 2000,4000,8000.
     * Returns array of long, or null.
     */
    public static long[] parseCommaDelimitedLongs(String s) {
        StringTokenizer tok;
        Vector v=new Vector();
        Long l;
        long[] retval=null;

        if(s == null) return null;
        tok=new StringTokenizer(s, ",");
        while(tok.hasMoreTokens()) {
            l=Long.valueOf(tok.nextToken());
            v.addElement(l);
        }
        if(v.size() == 0) return null;
        retval=new long[v.size()];
        for(int i=0; i < v.size(); i++)
            retval[i]=((Long)v.elementAt(i)).longValue();
        return retval;
    }

    /** e.g. "bela,jeannette,michelle" --> List{"bela", "jeannette", "michelle"} */
    public static java.util.List parseCommaDelimitedStrings(String l) {
        java.util.List tmp=new ArrayList();
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;

        while(tok.hasMoreTokens()) {
            t=tok.nextToken();
            tmp.add(t);
        }

        return tmp;
    }



    public static String shortName(String hostname) {
        int index;
        StringBuffer sb=new StringBuffer();

        if(hostname == null) return null;

        index=hostname.indexOf('.');
        if(index > 0 && !Character.isDigit(hostname.charAt(0)))
            sb.append(hostname.substring(0, index));
        else
            sb.append(hostname);
        return sb.toString();
    }

    public static String shortName(InetAddress hostname) {
        if(hostname == null) return null;
        StringBuffer sb=new StringBuffer();
        if(IpAddress.resolve_dns)
            sb.append(hostname.getHostName());
        else
            sb.append(hostname.getHostAddress());
        return sb.toString();
    }


    /** Finds first available port starting at start_port and returns server socket */
    public static ServerSocket createServerSocket(int start_port) {
        ServerSocket ret=null;

        // GemStoneAddition - use the configured bind address, if there is one
        String bindAddress = System.getProperty("gemfire.jg-bind-address");

        while(true) {
            try {
              if (bindAddress != null && bindAddress.length() > 0) {
                InetAddress bind_addr = InetAddress.getByName(bindAddress); // GemStoneAddition - this whole if/else stmt
                ret=new ServerSocket(start_port, 50, bind_addr);
              } else {
                ret=new ServerSocket(start_port);
              }
            }
            catch(SocketException bind_ex) { // GemStoneAddition
              if(treatAsBindException(bind_ex)) {
                start_port++;
                continue;
              } //else ignore
            }
            catch(IOException io_ex) {
            }
            break;
        }
        return ret;
    }

    // GemStoneAddition - changed this method to use the same approache that
    // we added in UDP.createEphemeralDatagramSocket
    public static ServerSocket createServerSocket(InetAddress bind_addr, int start_port,
        int end_port) { // GemStoneAddition - end port
        ServerSocket ret=null;
        Random rand = new SecureRandom();
        // GemstoneAddition - a real membership port range
        int localPort =  (start_port == end_port)? start_port :
                (start_port + rand.nextInt(end_port - start_port + 1));

        int startingLocalPort = localPort;
        int lastPortInRange = end_port;
        
        while(true) {
            // wrap around
            if (localPort > lastPortInRange) {
              if (startingLocalPort != 0) {
                localPort = start_port;
                lastPortInRange = startingLocalPort-1;
                startingLocalPort = 0; // flag that wrap-around has occurred
              } else {
                throw JChannel.getGfFunctions().getSystemConnectException(ExternalStrings.UNABLE_TO_FIND_FREE_PORT.toLocalizedString());
              }
            }
            try {
                ret=new ServerSocket(localPort, 1, bind_addr); // GemStoneAddition - backlog of 1 to keep suspended processes from queuing FD_SOCK pings
            }
            catch(SocketException bind_ex) { // GemStoneAddition
              if(Util.treatAsBindException(bind_ex)) {
                localPort++; //GemStoneAddition
              } // else ignore
              continue;
            }
            catch(IOException io_ex) {
            }
            break;
        }
        return ret;
    }



    /**
     * Creates a DatagramSocket bound to addr. If addr is null, socket won't be bound. If address is already in use,
     * start_port will be incremented until a socket can be created.
     * @param addr The InetAddress to which the socket should be bound. If null, the socket will not be bound.
     * @param port The port which the socket should use. If 0, a random port will be used. If > 0, but port is already
     *             in use, it will be incremented until an unused port is found, or until MAX_PORT is reached.
     */
    public static DatagramSocket createDatagramSocket(InetAddress addr, int port) throws Exception {
        DatagramSocket sock=null;

        if(addr == null) {
            if(port == 0) {
                return new DatagramSocket();
            }
            else {
                while(port < MAX_PORT) {
                    try {
                        return new DatagramSocket(port);
                    }
                    catch(SocketException bind_ex) { // port already used
                      if(treatAsBindException(bind_ex)) { // GemStone Addition
                        port++;
                      } else {
                        throw bind_ex;
                      }
                    }
                    catch(Exception ex) {
                        throw ex;
                    }
                }
            }
        }
        else {
            if(port == 0) port=1024;
            while(port < MAX_PORT) {
                try {
                    return new DatagramSocket(port, addr);
                }
                catch(SocketException bind_ex) { // port already used
                  if(treatAsBindException(bind_ex)) { // GemStone Addition
                    port++;
                  } else {
                    throw bind_ex;
                  }
                }
                catch(Exception ex) {
                    throw ex;
                }
            }
        }
        return sock; // will never be reached, but the stupid compiler didn't figure it out...
    }


    public static boolean checkForAIX() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("aix") ? true : false;
    }

    public static boolean checkForLinux() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("linux") ? true : false;
    }

    public static boolean checkForSolaris() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("sun") ? true : false;
    }

    public static boolean checkForWindows() {
        String os=System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("win") ? true : false;
    }

    public static void prompt(String s) {
        System.out.println(s);
        System.out.flush();
        try {
            while(System.in.available() > 0)
                System.in.read();
            System.in.read();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }


    public static int getJavaVersion() {
        String version=System.getProperty("java.version");
        int retval=0;
        if(version != null) {
            if(version.startsWith("1.2"))
                return 12;
            if(version.startsWith("1.3"))
                return 13;
            if(version.startsWith("1.4"))
                return 14;
            if(version.startsWith("1.5"))
                return 15;
            if(version.startsWith("5"))
                return 15;
            if(version.startsWith("1.6"))
                return 16;
            if(version.startsWith("6"))
                return 16;
        }
        return retval;
    }

//    public static String memStats(boolean gc) {
//        StringBuffer sb=new StringBuffer();
//        Runtime rt=Runtime.getRuntime();
//        if(gc)
//            rt.gc();
//        long free_mem, total_mem, used_mem;
//        free_mem=rt.freeMemory();
//        total_mem=rt.totalMemory();
//        used_mem=total_mem - free_mem;
//        sb.append("Free mem: ").append(free_mem).append("\nUsed mem: ").append(used_mem);
//        sb.append("\nTotal mem: ").append(total_mem);
//        return sb.toString();
//    }


//    public static InetAddress getFirstNonLoopbackAddress() throws SocketException {
//        Enumeration en=NetworkInterface.getNetworkInterfaces();
//        while(en.hasMoreElements()) {
//            NetworkInterface i=(NetworkInterface)en.nextElement();
//            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
//                InetAddress addr=(InetAddress)en2.nextElement();
//                if(!addr.isLoopbackAddress())
//                    return addr;
//            }
//        }
//        return null;
//    }


    public static InetAddress getFirstNonLoopbackAddress() throws SocketException {
        Enumeration en=NetworkInterface.getNetworkInterfaces();
        //boolean preferIpv4=Boolean.getBoolean("java.net.preferIPv4Stack");
        boolean preferIPv6=Boolean.getBoolean("java.net.preferIPv6Addresses");
        //if (preferIPv6) preferIpv4 = false; // GemStoneAddition
        while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                if(!addr.isLoopbackAddress()) {
                    if(addr instanceof Inet4Address) {
                        if(preferIPv6)
                            continue;
                        return addr;
                    }
                    /*if(addr instanceof Inet6Address) {
                        if(preferIpv4)
                            continue;
                        return addr;
                    }*/
                }
            }
        }
        return null;
    }

    public static List getAllAvailableInterfaces() throws SocketException {
        List retval=new ArrayList(10);
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            intf=(NetworkInterface)en.nextElement();
            retval.add(intf);
        }
        return retval;
    }


    public static boolean isBindAddressPropertyIgnored() {
        String tmp=System.getProperty(IGNORE_BIND_ADDRESS_PROPERTY);
        if(tmp == null)
            return false;

        tmp=tmp.trim().toLowerCase();
        if(tmp.equals("false") ||
                tmp.equals("no") ||
                tmp.equals("off"))
            return false;

        return true;
    }


    /*
    public static void main(String[] args) {
	DatagramSocket sock;
	InetAddress    addr=null;
	int            port=0;

	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-help")) {
		System.out.println("Util [-help] [-addr] [-port]");
		return;
	    }
	    if(args[i].equals("-addr")) {
		try {
		    addr=InetAddress.getByName(args[++i]);
		    continue;
		}
		catch(Exception ex) {
		    log.error(ex);
		    return;
		}
	    }
	    if(args[i].equals("-port")) {
		port=Integer.parseInt(args[++i]);
		continue;
	    }
	    System.out.println("Util [-help] [-addr] [-port]");
	    return;
	}

	try {
	    sock=createDatagramSocket(addr, port);
	    System.out.println("sock: local address is " + sock.getLocalAddress() + ":" + sock.getLocalPort() +
			       ", remote address is " + sock.getInetAddress() + ":" + sock.getPort());
	    System.in.read();
	}
	catch(Exception ex) {
	    log.error(ex);
	}
    }
    */

    public static void main(String args[]) throws Exception {
        ClassConfigurator.getInstance(true);

        Message msg=new Message(null, new IpAddress("127.0.0.1", 4444), "Bela");
        long    size=Util.sizeOf(msg);
        System.out.println("size=" + msg.size() + ", streamable size=" + size);

        msg.putHeader("belaban", new NakAckHeader((byte)1, 23, 34));
        size=Util.sizeOf(msg);
        System.out.println("size=" + msg.size() + ", streamable size=" + size);

        msg.putHeader("bla", new UdpHeader("groupname"));
        size=Util.sizeOf(msg);
        System.out.println("size=" + msg.size() + ", streamable size=" + size);


        IpAddress a1=new IpAddress(1234), a2=new IpAddress("127.0.0.1", 3333);
        a1.setAdditionalData("Bela".getBytes());
        size=Util.sizeOf(a1);
        System.out.println("size=" + a1.size(JGroupsVersion.CURRENT_ORDINAL) + ", streamable size of a1=" + size);
        size=Util.sizeOf(a2);
        System.out.println("size=" + a2.size(JGroupsVersion.CURRENT_ORDINAL) + ", streamable size of a2=" + size);


//        System.out.println("Check for Linux:   " + checkForLinux());
//        System.out.println("Check for Solaris: " + checkForSolaris());
//        System.out.println("Check for Windows: " + checkForWindows());
//        System.out.println("version: " + getJavaVersion());
    }

  // GemStoneAddition
  //IBM J9 sometimes reports "listen failed" instead of BindException
  //see bug #40589
  public static boolean treatAsBindException(SocketException se) {
    if(se instanceof BindException) {
      return true;
    }
    final String msg = se.getMessage();
    return (msg != null && msg.contains("Invalid argument: listen failed"));
  }
}
