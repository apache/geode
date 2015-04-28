/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PerfHeader.java,v 1.9 2005/08/08 12:45:43 belaban Exp $

package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.Util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;


/**
 * Inserted by PERF into each message. Records the time taken by each protocol to process the message to
 * which this header is attached. Travels down through the stack and up the other stack with the message.
 *
 * @author Bela Ban
 */
@SuppressFBWarnings(value="SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",
    justification="GemFire does not use the PERF protocol")
public class PerfHeader extends Header  {
    Object sender=null;
    Object receiver=null;
    long start_time=0;                       // time when header was created
    long end_time=0;                         // time when header was received
    long network_send=0;                     // time the packet was put on the network
    long network_recv=0;                     // time the packet was received from the network
    long network_time=0;                     // time spent on the network (between bottom layers)
    HashMap down=new HashMap();               // key=protocol name, val=PerfEntry
    HashMap up=new HashMap();                 // key=protocol name, val=PerfEntry
    final static int UP=1;
    final static int DOWN=2;
    final static String classname="org.jgroups.protocols.PerfHeader";
    static long size=0;
    private static Message msg2;
    static GemFireTracer            log=GemFireTracer.getLog(PerfHeader.class);


    static {
        size=Util.sizeOf(classname);
        if(size <= 0) size=400;
    }


    // Needed for externalization
    public PerfHeader() {
    }


    public PerfHeader(Object sender, Object receiver) {
        this.sender=sender;
        this.receiver=receiver;
        start_time=System.currentTimeMillis();
    }


    @Override // GemStoneAddition
    public String toString() {
        return "[PerfHeader]";
    }


    public String printContents(boolean detailed) {
        return printContents(detailed, null);
    }


    public String printContents(boolean detailed, Vector prots) {
        StringBuffer sb=new StringBuffer();
        String key;
        PerfEntry val;
        Protocol p;

        if(sender != null)
            sb.append("sender=").append(sender).append('\n');
        if(receiver != null)
            sb.append("receiver=").append(receiver).append('\n');

        if(detailed)
            sb.append("start_time=").append(start_time).append("\nend_time=").append(end_time).append('\n');

        if(end_time >= start_time)
            sb.append("total time=").append((end_time - start_time)).append('\n');
        else
            sb.append("total time=n/a\n");

        if(detailed) {
            if(network_send > 0) sb.append("network_send=").append(network_send).append('\n');
            if(network_recv > 0) sb.append("network_recv=").append(network_recv).append('\n');
        }

        if(network_time > 0)
            sb.append("network=").append(network_time).append('\n');

        sb.append("\nDOWN\n-----\n");
        if(prots != null) {
            for(int i=0; i < prots.size(); i++) {
                p=(Protocol)prots.elementAt(i);
                key=p.getName();
                val=(PerfEntry)down.get(key);
                sb.append(key).append(':').append('\t').append(val.printContents(detailed)).append('\n');
            }
        }
        else
            for(Iterator it=down.keySet().iterator(); it.hasNext();) {
                key=(String)it.next();
                val=(PerfEntry)down.get(key);
                sb.append(key).append(':').append('\t').append(val.printContents(detailed)).append('\n');
            }

        sb.append("\nUP\n-----\n");
        if(prots != null) {
            for(int i=prots.size() - 1; i >= 0; i--) {
                p=(Protocol)prots.elementAt(i);
                key=p.getName();
                val=(PerfEntry)up.get(key);
                sb.append(key).append(':').append('\t').append(val.printContents(detailed)).append('\n');
            }
        }
        else
            for(Iterator it=up.keySet().iterator(); it.hasNext();) {
                key=(String)it.next();
                val=(PerfEntry)up.get(key);
                sb.append(key).append(':').append('\t').append(val.printContents(detailed)).append('\n');
            }


        return sb.toString();
    }


    public void setEndTime() {
        end_time=System.currentTimeMillis();
    }


    public void setReceived(String prot_name, int type) {
        PerfEntry entry=getEntry(prot_name, type);
        long t=System.currentTimeMillis();
        if(entry != null)
            entry.setReceived(t);
    }

    public void setDone(String prot_name, int type) {
        PerfEntry entry=getEntry(prot_name, type);
        long t=System.currentTimeMillis();
        if(entry != null)
            entry.setDone(t);
    }

    public void setNetworkSent() {
        network_send=System.currentTimeMillis();
    }


    public void setNetworkReceived() {
        network_recv=System.currentTimeMillis();
        if(network_send > 0 && network_recv > network_send)
            network_time=network_recv - network_send;
    }


    /**
     * Adds a new entry to both hashtables
     */
    public void addEntry(String prot_name) {
        up.put(prot_name, new PerfEntry());
        down.put(prot_name, new PerfEntry());
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sender);
        out.writeObject(receiver);
        out.writeLong(start_time);
        out.writeLong(end_time);
        out.writeLong(network_send);
        out.writeLong(network_recv);
        out.writeLong(network_time);
        writeHashtable(down, out);
        writeHashtable(up, out);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sender=in.readObject();
        receiver=in.readObject();
        start_time=in.readLong();
        end_time=in.readLong();
        network_send=in.readLong();
        network_recv=in.readLong();
        network_time=in.readLong();
        down=readHashtable(in);
        up=readHashtable(in);
    }


    @Override // GemStoneAddition
    public long size(short version) {
        return size;
    }


    void writeHashtable(HashMap h, ObjectOutput out) {
        String key;
        PerfEntry val;

        try {
            if(h == null) {
                out.writeInt(0);
                return;
            }
            out.writeInt(h.size());
            for(Iterator it=h.entrySet()/*keySet() GemStoneAddition*/.iterator(); it.hasNext();) {
//                key=(String)it.next();
//                val=(PerfEntry)h.get(key);
                Map.Entry entry = (Map.Entry)it.next(); // GemStoneAddition
                key = (String)entry.getKey();
                val = (PerfEntry)entry.getValue();
                if(key == null || val == null) {
                    System.err.println("PerfHeader.writeHashtable(): key or val is null");
                    continue;
                }
                out.writeObject(key);
                out.writeObject(val);
            }
        }
        catch(Exception ex) {
            System.err.println("PerfHeader.writeHashtable(): " + ex);
        }
    }


    HashMap readHashtable(ObjectInput in) {
        HashMap h=new HashMap();
        int num=0;
        String key;
        PerfEntry val;

        try {
            num=in.readInt();
            if(num == 0)
                return h;
            for(int i=0; i < num; i++) {
                key=(String)in.readObject();
                val=(PerfEntry)in.readObject();
                h.put(key, val);
            }
        }
        catch(Exception ex) {
            System.err.println("PerfHeader.readHashtable(): " + ex);
        }

        return h;
    }


    PerfEntry getEntry(String prot_name, int type) {
        HashMap tmp=null;
        PerfEntry entry=null;

        if(prot_name == null) return null;
        if(type == UP)
            tmp=up;
        else
            if(type == DOWN) tmp=down;
        if(tmp == null) return null;
        entry=(PerfEntry)tmp.get(prot_name);
        if(entry == null)
            log.error(ExternalStrings.PerfHeader_PERFHEADERGETENTRY_PROTOCOL__0__NOT_FOUND, prot_name);
        return entry;
    }


    public static void main(String[] args) {
        PerfHeader hdr=new PerfHeader(), hdr2;
        Message msg;
        ByteArrayOutputStream out_stream;
        ByteArrayInputStream in_stream;
        ObjectOutputStream out;
        ObjectInputStream in;
        byte[] out_buf, in_buf;


        hdr.addEntry("GMS");
        hdr.addEntry("GMS");
        hdr.addEntry("FRAG");
        hdr.addEntry("FRAG");
        hdr.addEntry("UDP");
        hdr.addEntry("UDP");


        msg=new Message();
        msg.putHeader("PERF", hdr);

        try { // GemStoneAddition
        hdr.setReceived("GMS", PerfHeader.DOWN);
        Util.sleep(2);
        hdr.setDone("GMS", PerfHeader.DOWN);

        hdr.setReceived("FRAG", PerfHeader.DOWN);
        Util.sleep(20);
        hdr.setDone("FRAG", PerfHeader.DOWN);


        long len=msg.size();
        System.out.println("Size is " + len);


        hdr.setReceived("UDP", PerfHeader.DOWN);
        Util.sleep(12);
        hdr.setDone("UDP", PerfHeader.DOWN);


        Util.sleep(30);

        hdr.setReceived("UDP", PerfHeader.UP);
        hdr.setDone("UDP", PerfHeader.UP);

        hdr.setReceived("FRAG", PerfHeader.UP);
        Util.sleep(23);
        hdr.setDone("FRAG", PerfHeader.UP);

        hdr.setReceived("GMS", PerfHeader.UP);
        Util.sleep(3);
        hdr.setDone("GMS", PerfHeader.UP);


        hdr.setEndTime();

        System.out.println(hdr.printContents(true));

        try {
            System.out.println("Saving hdr to byte buffer");
            out_stream=new ByteArrayOutputStream(256);
            out=new ObjectOutputStream(out_stream);
            out.writeObject(msg);
            out_buf=out_stream.toByteArray();

            System.out.println("Constructing hdr2 from byte buffer");
            in_buf=out_buf; // ref

            in_stream=new ByteArrayInputStream(in_buf);
            in=new ObjectInputStream(in_stream);

            msg2=(Message)in.readObject();
            hdr2=(PerfHeader)msg2.removeHeader("PERF");
            System.out.println(hdr2.printContents(true));
        }
        catch(Exception ex) {
            log.error(ex);
        }


    }
    catch (InterruptedException e) {
      // We're in a main.  Nothing to do; just exit.
    }
    
    }
}


/**
 * Entry specific for 1 protocol layer. Records time message was received by that layer and when message was passed on
 */
class PerfEntry implements Externalizable {
    long received=0;
    long done=0;
    long total=-1;


    // Needed for externalization
    public PerfEntry() {

    }


    public long getReceived() {
        return received;
    }

    public long getDone() {
        return done;
    }

    public long getTotal() {
        return total;
    }

    public void setReceived(long r) {
        received=r;
    }

    public void setDone(long d) {
        done=d;
        if(received > 0 && done > 0 && done >= received)
            total=done - received;
    }

    @Override // GemStoneAddition
    public String toString() {
        if(total >= 0)
            return "time: " + total;
        else
            return "time: n/a";
    }


    public String printContents(boolean detailed) {
        StringBuffer sb=new StringBuffer();
        if(detailed) {
            if(received > 0) sb.append("received=").append(received);
            if(done > 0) {
                if(received > 0) sb.append(", ");
                sb.append("done=").append(done);
            }
        }
        if(detailed && (received > 0 || done > 0)) sb.append(", ");
        sb.append(toString());
        return sb.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(received);
        out.writeLong(done);
        out.writeLong(total);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        received=in.readLong();
        done=in.readLong();
        total=in.readLong();
    }


}
