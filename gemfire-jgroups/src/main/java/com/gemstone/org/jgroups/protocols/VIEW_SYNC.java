/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.Vector;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.TimeScheduler.Task;




/**
 * Periodically sends the view to the group. When a view is received which is greater than the current view, we
 * install it. Otherwise we simply discard it. This is used to solve the problem for unreliable view
 * dissemination outlined in JGroups/doc/ReliableViewInstallation.txt. This protocol is supposed to be just below GMS.
 * @author Bela Ban
 * @version $Id: VIEW_SYNC.java,v 1.5 2005/12/16 16:18:16 belaban Exp $
 */
public class VIEW_SYNC extends Protocol  {
    Address             local_addr=null;
    final Vector        mbrs=new Vector();
    View                my_view=null;
    ViewId              my_vid=null;

    /** Sends a VIEW_SYNC message to the group every 20 seconds on average. 0 disables sending of VIEW_SYNC messages */
    long                avg_send_interval=60000;

    private int         num_views_sent=0;
    private int         num_views_adjusted=0;

    volatile/*GemStoneAddition*/ ViewSendTask        view_send_task=null;             // bcasts periodic STABLE message (added to timer below)
    final Object        view_send_task_mutex=new Object(); // to sync on stable_task
    TimeScheduler       timer=null;                   // to send periodic STABLE msgs (and STABILITY messages)
    public static final String name="VIEW_SYNC";
    
    static boolean VERBOSE = Boolean.getBoolean("VS.VERBOSE"); // GemStoneAddition



    @Override // GemStoneAddition
    public String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumVIEWSYNC;
    }
    // end GemStone addition

    public long getAverageSendInterval() {
        return avg_send_interval;
    }

    public void setAverageSendInterval(long gossip_interval) {
        avg_send_interval=gossip_interval;
    }

    public int getNumViewsSent() {
        return num_views_sent;
    }

    public int getNumViewsAdjusted() {
        return num_views_adjusted;
    }

    @Override // GemStoneAddition
    public void resetStats() {
        super.resetStats();
        num_views_adjusted=num_views_sent=0;
    }



    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("avg_send_interval");
        if(str != null) {
            avg_send_interval=Long.parseLong(str);
            props.remove("avg_send_interval");
        }

        //GemStoneAddition - split-brain detection support
        str=props.getProperty("split-brain-detection");
        if (str != null) {
          splitBrainDetectionEnabled = Boolean.valueOf(str).booleanValue();
          props.remove("split-brain-detection");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.VIEW_SYNC_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }
        return true;
    }


    @Override // GemStoneAddition
    public void start() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("timer cannot be retrieved from protocol stack");
    }

    @Override // GemStoneAddition
    public void stop() {
        stopViewSender();
    }

    /** Sends a VIEW_SYNC_REQ to all members, every member replies with a VIEW multicast */
    public void sendViewRequest() {
        Message msg=new Message(null, null, null);
        ViewSyncHeader hdr=new ViewSyncHeader(ViewSyncHeader.VIEW_SYNC_REQ);
        msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, msg));
    }

//    public void sendFakeViewForTestingOnly() {
//        ViewId fake_vid=new ViewId(local_addr, my_vid.getId() +2);
//        View fake_view=new View(fake_vid, new Vector(my_view.getMembers()));
//        System.out.println("sending fake view " + fake_view);
//        my_view=fake_view;
//        my_vid=fake_vid;
//        sendView();
//    }


    @Override // GemStoneAddition
    public void up(Event evt) {
        Message msg;
        ViewSyncHeader hdr;
        int type=evt.getType();

        switch(type) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            hdr=(ViewSyncHeader)msg.removeHeader(name);
            if(hdr == null)
                break;
            Address sender=msg.getSrc();
            switch(hdr.type) {
            case ViewSyncHeader.VIEW_SYNC:
              View v = msg.getObject();
              if (VERBOSE) {
                log.getLogWriter().info(
                    ExternalStrings.DEBUG, 
                    "Received view sync from " + sender + ": " + v);
              }
                handleView(v, sender);
                break;
            case ViewSyncHeader.VIEW_SYNC_REQ:
                if(!sender.equals(local_addr))
                    sendView(sender); // GemStoneAddition - make sure it goes to the sender
                break;
            default:
                if(log.isErrorEnabled()) log.error(ExternalStrings.VIEW_SYNC_VIEWSYNCHEADER_TYPE__0__NOT_KNOWN, hdr.type);
            }
            return;


        case Event.VIEW_CHANGE:
            View view=(View)evt.getArg();
            handleViewChange(view);
            break;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
            
        case Event.SUSPECT_NOT_MEMBER:
          Address mbr = (Address)evt.getArg();
          sendView(mbr);
          break;
        }
        passUp(evt);
    }



    @Override // GemStoneAddition
    public void down(Event evt) {
        switch(evt.getType()) {
        case Event.VIEW_CHANGE:
            View v=(View)evt.getArg();
            handleViewChange(v);
            break;
        }
        passDown(evt);
    }



    /* --------------------------------------- Private Methods ---------------------------------------- */

    private volatile Task sendTask;
    private boolean splitBrainDetectionEnabled;
    
    private void handleView(View v, final Address sender) {
      // GemStoneAddition - avoid being noisy during shutdown
      if (stack.getChannel().closing()) {
        return;
      }
        Vector members=v.getMembers();
//        if(!members.contains(local_addr)) {
//            if(log.isWarnEnabled())
//            log.warn("VIEW_SYNC ignoring view as I (" + local_addr + ") am not a member of view (" + v + ")");
//            return;
//        }

        final ViewId vid=v.getVid();
        int rc=vid.compareTo(my_vid);
        if(rc > 0) { // foreign view is greater than my own view; update my own view !
            if(VERBOSE || log.isTraceEnabled()) {
                log.getLogWriter().info(
                  ExternalStrings.DEBUG,   
                  "view from " + sender + " (" + vid + ") is greater than my own view (" + my_vid + ");" +
                  " will update my own view");
            }
            Message view_change=new Message(local_addr, local_addr, null);
            com.gemstone.org.jgroups.protocols.pbcast.GMS.GmsHeader hdr;
            hdr=new com.gemstone.org.jgroups.protocols.pbcast.GMS.GmsHeader(com.gemstone.org.jgroups.protocols.pbcast.GMS.GmsHeader.VIEW);
            view_change.putHeader(GMS.name, hdr);
            view_change.setObject(v);
            passUp(new Event(Event.MSG, view_change));
            num_views_adjusted++;
        }
        else if (sender != null && rc < 0 && this.splitBrainDetectionEnabled) {
          // GemStoneAddition - foreign view is older than my view - send a new
          // view, but do it in another thread to avoid blocking the UDP
          // receiver thread
          Task oldTask = this.sendTask;
          if (oldTask == null || oldTask.cancelled()) {  // only permit one task at a time to prevent runaway behavior
            Task newTask = new Task() {
              volatile boolean hasRun;
              public boolean cancelled() {
                return hasRun;
              }

              public long nextInterval() {
                return 0;
              }

              public void run() {
                hasRun = true;
                if (VERBOSE || log.isTraceEnabled()) {
                  log.getLogWriter().info(
                      ExternalStrings.DEBUG,
                      "view from " + sender + " (" + vid +
                      ") is older than my own view (" + my_vid + ").  Sending my view to it");
                }
                sendView(sender);
              }
            };
            this.sendTask = newTask;
            stack.timer.add(newTask);
          }
        }
    }

    private void handleViewChange(View view) {
        Vector tmp=view.getMembers();
        if(tmp != null) {
            mbrs.clear();
            mbrs.addAll(tmp);
        }
        my_view=(View)view.clone();
        my_vid=my_view.getVid();
        if(my_view.size() > 1 && (view_send_task == null || !view_send_task.running()))
            startViewSender();
    }
    
    
    protected void sendView() {
      sendView(null);
    }

    protected/*GemStoneAddition*/ void sendView(Address oldViewSender) {
        View tmp=(View)(my_view != null? my_view.clone() : null);
        if(tmp == null) return;
        ViewSyncHeader hdr=new ViewSyncHeader(ViewSyncHeader.VIEW_SYNC);
        // GemStoneAddition - sending to a specific member
        if (oldViewSender == null) {
          Message msg=new Message(null, null, null); // send to the group
          msg.isHighPriority = true;
          msg.putHeader(name, hdr);
          msg.putHeader(UNICAST.BYPASS_UNICAST, new ViewSyncHeader(ViewSyncHeader.VIEW_SYNC));
          msg.setObject(tmp);
          passDown(new Event(Event.MSG, msg));
        }
        else {
          // GemStoneAddition - send current view to sender of old view, even if
          // it's not in the current membership view
          Message msg = new Message(oldViewSender, null, null);
          msg.putHeader(name, hdr);
          msg.putHeader(UNICAST.BYPASS_UNICAST, new ViewSyncHeader(ViewSyncHeader.VIEW_SYNC));
          msg.setObject(tmp);
          passDown(new Event(Event.MSG, msg));
        }
        num_views_sent++;
    }

    void startViewSender() {
        // Here, double-checked locking works: we don't want to synchronize if the task already runs (which is the case
        // 99% of the time). If stable_task gets nulled after the condition check, we return anyways, but just miss
        // 1 cycle: on the next message or view, we will start the task
        if(view_send_task != null)
            return;
        synchronized(view_send_task_mutex) {
            if(view_send_task != null && view_send_task.running()) {
                return;  // already running
            }
            view_send_task=new ViewSendTask();
            timer.add(view_send_task, true); // fixed-rate scheduling
        }
        if(trace)
            log.trace("view send task started");
    }


    void stopViewSender() {
        // contrary to startViewSender(), we don't need double-checked locking here because this method is not
        // called frequently
        synchronized(view_send_task_mutex) {
            if(view_send_task != null) {
                view_send_task.stop();
                if(trace)
                    log.trace("view send task stopped");
                view_send_task=null;
            }
        }
    }






    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class ViewSyncHeader extends Header implements Streamable {
        public static final int VIEW_SYNC     = 1; // contains a view
        public static final int VIEW_SYNC_REQ = 2; // request to all members to send their views

        public int   type=0;

        public ViewSyncHeader() {
        }


        public ViewSyncHeader(int type) {
            this.type=type;
        }


        static String type2String(int t) {
            switch(t) {
                case VIEW_SYNC:
                    return "VIEW_SYNC";
                case VIEW_SYNC_REQ:
                    return "VIEW_SYNC_REQ";
                default:
                    return "<unknown>";
            }
        }

        @Override // GemStoneAddition
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append('[');
            sb.append(type2String(type));
            sb.append("]");
//            if(view != null)
//                sb.append(", view= ").append(view);
            return sb.toString();
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
//            if(view == null) {
//                out.writeBoolean(false);
//                return;
//            }
//            out.writeBoolean(true);
//            view.writeExternal(out);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
//            boolean available=in.readBoolean();
//            if(available) {
//                view=new View();
//                view.readExternal(in);
//            }
        }

        @Override // GemStoneAddition
        public long size(short version) {
            long retval=Global.INT_SIZE + Global.BYTE_SIZE; // type + presence for digest
//            if(view != null)
//                retval+=view.serializedSize(version);
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(type);
//            Util.writeStreamable(view, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readInt();
//            view=(View)Util.readStreamable(View.class, in);
        }


    }




    /**
     Periodically multicasts a View_SYNC message
     */
    protected/*GemStoneAddition*/ class ViewSendTask implements TimeScheduler.Task {
        boolean stopped=false;

        public void stop() {
            stopped=true;
        }

        public boolean running() { // syntactic sugar
            return !stopped;
        }

        public boolean cancelled() {
            return stopped;
        }

        public long nextInterval() {
            long interval=computeSleepTime();
            if(interval <= 0)
                return 10000;
            else
                return interval;
        }


        public void run() {
            sendView();
        }

        long computeSleepTime() {
            int num_mbrs=Math.max(mbrs.size(), 1);
            return getRandom((num_mbrs * avg_send_interval * 2));
        }

        long getRandom(long range) {
            return (long)((Math.random() * range) % range);
        }
    }



}
