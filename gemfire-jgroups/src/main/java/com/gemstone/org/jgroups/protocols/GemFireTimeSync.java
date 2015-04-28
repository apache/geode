/**
 * An implementation of the Berkeley Algorithm for clock synchronization.
 * On view changes and otherwise at a configurable interval this protocol
 * sends a request for the current millisecond clock to all members.  It
 * computes the average round-trip time and the average clock value, throwing
 * out samples outside the standard deviation.  The result is then used to
 * compute and send clock offsets to each member.
 *  
 * @author Bruce Schuchardt
 */
package com.gemstone.org.jgroups.protocols;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;




import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.protocols.pbcast.GMS.GmsHeader;
import com.gemstone.org.jgroups.stack.GFPeerAdapter;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;

public class GemFireTimeSync extends Protocol {

  static private boolean DEBUG = Boolean.getBoolean("gemfire.time-service.debug");

  private int clockSyncInterval = 100;  // every 100 seconds (clock drift of 1 ms per second)
  private int replyWaitInterval = 15;  // fifteen second wait before timing out
  private long currCoordOffset = 0;
  
  private Address localAddress = null;
  private volatile View view;
  private final AtomicLong nextProcId = new AtomicLong(0);
  private final ConcurrentMap<Long, ReplyProcessor> processors = new ConcurrentHashMap<Long, ReplyProcessor>();
  private final ConcurrentMap<Address, GFTimeSyncHeader> joinTimeOffsets = new ConcurrentHashMap<Address, GFTimeSyncHeader>();

  private ServiceThread syncThread;
  // bug #50833 - use a different object to sync on creation of the syncThread
  private final Object syncThreadLock = new Object();

  // Test hook for unit testing.
  private TestHook testHook;

  private long joinReqTime = 0;
  public static final int TIME_RESPONSES = 0;
  public static final int OFFSET_RESPONSE = 1;

  @Override
  public String getName() {
    return "GemFireTimeSync";
  }
  
  /**
   * This method holds most of the logic for this protocol.  The rest of the
   * class is composed of utility calculation methods and messaging infrastructure.
   * Here we request the current clock from each member, wait for the replies,
   * calculate the distributed time using the Berkeley Algorithm and then send
   * individual time offsets to each member including this JVM.
   */
  synchronized void computeAndSendOffsets(View v) {

    if (v.getMembers().size() < 2) {
      return;
    }
    // send a message containing a reply processor ID and the current time.
    // Others will respond with their own time
    long currentTime = System.currentTimeMillis();
    long procID = nextProcId.incrementAndGet();
    GFTimeSyncHeader timeHeader = new GFTimeSyncHeader(procID, GFTimeSyncHeader.OP_TIME_REQUEST, currentTime);
    ReplyProcessor proc = new ReplyProcessor(view, procID);
    processors.put(procID, proc);
    
    // specify use of UNICAST by setting the message destination.  By doing
    // this we can take advantage of the isHighPriority flag for OOB messaging
    // and avoid cache traffic that might be clogging up the regular send/receive windows
    try {
      for (Iterator<?> it = v.getMembers().iterator(); it.hasNext();) {
        Address mbr = (Address)it.next();
        if (!mbr.equals(this.localAddress)) {
          // JGroups requires a different message for each destination but the
          // header can be reused
          Message timeMessage = new Message();
          timeMessage.setDest(mbr);
          timeMessage.isHighPriority = true;
          timeMessage.putHeader(getName(), timeHeader);
          passDown(new Event(Event.MSG, timeMessage));
        }
      }
      GFTimeSyncHeader myResponse = new GFTimeSyncHeader(0, (byte)0, currentTime);
      proc.replyReceived(this.localAddress, myResponse);
      proc.waitForReplies(replyWaitInterval * 1000);
    } catch (InterruptedException e) {
      return;
    } finally {
      if (testHook != null) {
        testHook.setResponses(proc.responses, currentTime);
        testHook.hook(TIME_RESPONSES);
      }
      processors.remove(procID);
    }
    
    Map<Address, GFTimeSyncHeader> responses = proc.responses;
    int numResponses = responses.size();
    if (DEBUG || log.getLogWriter().fineEnabled()) {
      log.getLogWriter().info(ExternalStrings.DEBUG, "Received " + numResponses + " responses");
    }

    if (numResponses > 1) {

      // now compute the average round-trip time and the average clock time,
      // throwing out values outside of the standard deviation for each
      
      long averageRTT = getMeanRTT(responses, 0, Long.MAX_VALUE);
      long rTTStddev = getRTTStdDev(responses, averageRTT);
      // now recompute the average throwing out ones that are way off
      long newAverageRTT = getMeanRTT(responses, averageRTT, rTTStddev);
      if (newAverageRTT > 0) {
        averageRTT = newAverageRTT;
      }
      
      long averageTime = getMeanClock(responses, 0, Long.MAX_VALUE);
      long stddev = getClockStdDev(responses, averageTime);
      long newAverageTime = getMeanClock(responses, averageTime, stddev);
      if (newAverageTime > 0) {
        averageTime = newAverageTime;
      }
      
      long averageTransmitTime = averageRTT / 2;
      long adjustedAverageTime = averageTime + averageTransmitTime;

      if (DEBUG || log.getLogWriter().fineEnabled()) {
        StringBuilder buffer = new StringBuilder(5000);
        for (Map.Entry<Address,GFTimeSyncHeader> entry: responses.entrySet()) {
          buffer.append("\n\t").append(entry.getKey()).append(": ").append(entry.getValue());
        }
        log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service computed " 
            + "round trip time of " + averageRTT + " with stddev of " + rTTStddev + " and " 
            + "clock time of " + averageTime + " with stddev of " + stddev
            + " for " + numResponses + " members.  Details: \n\tstart time=" 
            + currentTime + "  group time=" + adjustedAverageTime + " transmit time=" + averageTransmitTime
            + buffer.toString());
      }
      
      // TODO: should all members on the same machine get the same time offset?
      
      for (Iterator<Map.Entry<Address, GFTimeSyncHeader>> it = responses.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<Address, GFTimeSyncHeader> entry = it.next();
        IpAddress mbr = (IpAddress)entry.getKey();
        GFTimeSyncHeader response = entry.getValue();
        Message offsetMessage = new Message();
        offsetMessage.setDest(mbr);
        offsetMessage.isHighPriority = true;
        
        long responseTransmitTime = (response.timeReceived - currentTime) / 2;
        long offset = adjustedAverageTime - (response.time + responseTransmitTime);
        
        if (DEBUG || log.getLogWriter().fineEnabled()) {
          log.getLogWriter().info(ExternalStrings.DEBUG, "sending time offset of " + offset + " to " + entry.getKey()
              + " whose time was " + response.time + " and transmit time was " + responseTransmitTime);
        }
        
        offsetMessage.putHeader(getName(), new GFTimeSyncHeader(0, GFTimeSyncHeader.OP_TIME_OFFSET, offset));
        if (mbr == this.localAddress) {
          // We need to cache offset here too just for co-ordinator.
          currCoordOffset = offset;
          offsetMessage.setSrc(this.localAddress);
          up(new Event(Event.MSG, offsetMessage));
        } else {
          passDown(new Event(Event.MSG, offsetMessage));
        }
      }
    }
  }
  

  @Override
  public void up(Event event) {
    switch (event.getType()) {
    case Event.SET_LOCAL_ADDRESS:
      this.localAddress = (Address)event.getArg();
      if (DEBUG || log.getLogWriter().fineEnabled()) {
        log.getLogWriter().info(ExternalStrings.DEBUG, "GF time service setting local address to " + this.localAddress); 
      }
      break;
    case Event.MSG:
      Message msg = (Message)event.getArg();
      GFTimeSyncHeader header = (GFTimeSyncHeader)msg.removeHeader(getName());
      if (header != null) {
        switch (header.opType){
          case GFTimeSyncHeader.JOIN_TIME_REQUEST:
            long beforeJoinTime = System.currentTimeMillis() + currCoordOffset;
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service received join time offset request from " + msg.getSrc() + " join time=" + beforeJoinTime);
            }
            
            // Store in a map and wait for JOIN reply GMS message.
            GFTimeSyncHeader respHeader = new GFTimeSyncHeader(0, GFTimeSyncHeader.JOIN_RESPONSE_OFFSET, currCoordOffset, beforeJoinTime, 0);
            joinTimeOffsets.put(msg.getSrc(), respHeader);
            break;
          case GFTimeSyncHeader.JOIN_RESPONSE_OFFSET:
            if (header.coordTimeAfterJoin == 0) { // member sending offset is using old version - ignore it
              break;
            }
            long currentLocalTime = System.currentTimeMillis();
            
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(
                  ExternalStrings.DEBUG,
                  " currentLocalTime =" + currentLocalTime 
                  + " coordAfterJoinTime = " + header.coordTimeAfterJoin
                  + " coordBeforeJoinTime = " + header.coordTimeBeforeJoin 
                  + " joinReqTime = " + joinReqTime);
            }
            long transmissionTime = ((currentLocalTime - (header.coordTimeAfterJoin - header.coordTimeBeforeJoin)) - joinReqTime)/2;
            long timeOffs = header.coordTimeBeforeJoin - (joinReqTime + transmissionTime);
            
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service received join time offset from " + msg.getSrc() + " offset=" + timeOffs);
            }
            GFPeerAdapter mgr = stack.gfPeerFunctions;
            if (mgr != null) {
              // give the time to the manager who can install it in gemfire
              mgr.setCacheTimeOffset(msg.getSrc(), timeOffs, true);
            }
            break;
          case GFTimeSyncHeader.OP_TIME_REQUEST:
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service received time request from " + msg.getSrc());
            }
            GFTimeSyncHeader responseHeader = new GFTimeSyncHeader(header.procID, GFTimeSyncHeader.OP_TIME_RESPONSE, System.currentTimeMillis());
            Message response = new Message();
            response.setDest(msg.getSrc());
            response.putHeader(getName(), responseHeader);
            response.isHighPriority = true;
            passDown(new Event(Event.MSG, response));
            return;
          case GFTimeSyncHeader.OP_TIME_RESPONSE:
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service received time response from " + msg.getSrc());
            }
            ReplyProcessor p = processors.get(new Long(header.procID));
            if (p != null) {
              p.replyReceived(msg.getSrc(), header);
            }
            return;
          case GFTimeSyncHeader.OP_TIME_OFFSET:
            long timeOffset = header.time;
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service received time offset update from " + msg.getSrc() + " offset=" + timeOffset);
            }
            GFPeerAdapter jmm = stack.gfPeerFunctions;
            if (jmm != null) {
              // give the time offset to the Distribution manager who can set it in GemfireCacheImpl.
              jmm.setCacheTimeOffset(msg.getSrc(), timeOffset, false);
            }
            if (testHook != null) {
              testHook.hook(OFFSET_RESPONSE);
            }
            return;
        }
      }
    }
    passUp(event);
  }
  
  /*private long getCurrTimeOffset() {
    JGroupMembershipManager mgr = stack.jgmm;
    if (mgr != null) {
      // give the time offset to the Distribution manager who can set it in GemfireCacheImpl.
      DistributedMembershipListener listener = mgr.getListener();
      if (listener != null) {
        DistributionManager dm = listener.getDM();
        return dm .getCacheTimeOffset();
      }
    }
    return 0;
  }*/

  @Override
  public void down(Event event) {
    switch (event.getType()) {
    case Event.VIEW_CHANGE:
      View view = (View)event.getArg();
      if (DEBUG || log.getLogWriter().fineEnabled()) {
        log.getLogWriter().info(ExternalStrings.DEBUG, "GF time service is processing view " + view);
      }
      viewChanged(view);
      break;
    case Event.MSG:
      Message msg = (Message)event.getArg();
      GMS.GmsHeader header = (GmsHeader) msg.getHeader(GMS.name);

      if (header != null) {
        if (header.getType() == GmsHeader.JOIN_REQ) {
          // Send Time Sync OFFSET request header in JOIN_REQ message.
          joinReqTime  = System.currentTimeMillis();
          GFTimeSyncHeader timeHeader = new GFTimeSyncHeader(0, GFTimeSyncHeader.JOIN_TIME_REQUEST, joinReqTime);
          msg.putHeader(getName(), timeHeader);
        } else if (header.getType() == GmsHeader.JOIN_RSP) {
          // Send the time offset in JOIN_RSP response message.
          GFTimeSyncHeader joinTimeSyncHeader = joinTimeOffsets.remove(msg.getDest());
          if (joinTimeSyncHeader != null) {
            long afterJoinTime = System.currentTimeMillis() + currCoordOffset;
            joinTimeSyncHeader.coordTimeAfterJoin = afterJoinTime;
            msg.putHeader(getName(), joinTimeSyncHeader);
          
            if (DEBUG || log.getLogWriter().fineEnabled()) {
              log.getLogWriter().info(ExternalStrings.DEBUG, "GemFire time service is including after-join time in join-response to " + msg.getDest() + " after join time=" + afterJoinTime);
            }
          }
        }
      }
      break;
    }
    passDown(event);
  }
 
  private void viewChanged(View newView) {
    this.view = (View) newView.clone();
    if (this.localAddress.equals(newView.getCoordinator())) {
      boolean newThread = false;
      synchronized(this.syncThreadLock) {
        if (this.syncThread == null) {
          this.syncThread = new ServiceThread(GemFireTracer.GROUP, "GemFire Time Service");
          this.syncThread.setDaemon(true);
          this.syncThread.start();
          newThread = true;
        }
        if (!newThread) {
          this.syncThread.computeOffsetsForNewView();
        }
      }
    } else {
      synchronized (this.syncThreadLock) {
        if (this.syncThread != null) {
          this.syncThread.cancel();
        }
      }
    }
  }
  
  /**
   * retrieves the average of the samples.  This can be used with (samples, 0, Long.MAX_VALUE) to get
   * the initial mean and then (samples, lastResult, stddev) to get those within the standard deviation.
   * @param values
   * @param previousMean
   * @param stddev
   * @return the mean
   */
  private long getMeanRTT(Map<Address, GFTimeSyncHeader> values, long previousMean, long stddev) {
    long totalTime = 0;
    long numSamples = 0;
    long upperLimit = previousMean + stddev;
    for (GFTimeSyncHeader response: values.values()) {
      long rtt = response.timeReceived - response.time;
      if (rtt <= upperLimit) {
        numSamples++;
        totalTime += rtt;
      }
    }
    long averageTime = totalTime / numSamples;
    return averageTime;
  }
  
  private long getRTTStdDev(Map<Address, GFTimeSyncHeader> values, long average) {
    long sqDiffs = 0;
    for (GFTimeSyncHeader response: values.values()) {
      long diff = average - (response.timeReceived - response.time);
      sqDiffs += diff * diff;
    }
    return Math.round(Math.sqrt((double)sqDiffs));
  }

  /**
   * retrieves the average of the samples.  This can be used with (samples, 0, Long.MAX_VALUE) to get
   * the initial mean and then (samples, lastResult, stddev) to get those within the standard deviation.
   * @param values
   * @param previousMean
   * @param stddev
   * @return the mean
   */
  private long getMeanClock(Map<Address, GFTimeSyncHeader> values, long previousMean, long stddev) {
    long totalTime = 0;
    long numSamples = 0;
    long upperLimit = previousMean + stddev;
    long lowerLimit = previousMean - stddev;
    for (GFTimeSyncHeader response: values.values()) {
      if (lowerLimit <= response.time && response.time <= upperLimit) {
        numSamples++;
        totalTime += response.time;
      }
    }
    long averageTime = totalTime / numSamples;
    return averageTime;
  }
  
  private long getClockStdDev(Map<Address, GFTimeSyncHeader> values, long average) {
    long sqDiffs = 0;
    for (GFTimeSyncHeader response: values.values()) {
      long diff = average - response.time;
      sqDiffs += diff * diff;
    }
    return Math.round(Math.sqrt((double)sqDiffs));
  }
  
  @Override
  public boolean setProperties(Properties props) {
    String str;

    super.setProperties(props);

    str=props.getProperty("clock_sync_interval");
    if (str != null) {
      clockSyncInterval = Integer.parseInt(str);
      props.remove("clock_sync_interval");
    }

    str=props.getProperty("reply_wait_interval");
    if (str != null) {
      replyWaitInterval = Integer.parseInt(str);
      props.remove("reply_wait_interval");
    }

    if (props.size() > 0) {
      // this will not normally be seen by customers, even if there are unrecognized properties, because
      // jgroups error messages aren't displayed unless the debug flag is turned on
      log.error(ExternalStrings.DEBUG, "The following GemFireTimeSync properties were not recognized: " + props);
      return false;
    }
    return true;
  }

  @Override
  public void init() throws Exception {
    super.init();
  }

  @Override
  public void start() throws Exception {
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    if (this.syncThread != null) {
      this.syncThread.cancel();
    }
  }
  
  static class ReplyProcessor {
    int responderCount;
    long procID;
    Map<Address, GFTimeSyncHeader> responses = new HashMap<Address, GFTimeSyncHeader>();
    Object doneSync = new Object();
    
    ReplyProcessor(View view, long procID) {
      responderCount = view.getMembers().size();
      this.procID = procID;
    }
    
    void replyReceived(Address sender, GFTimeSyncHeader response) {
      response.timeReceived = System.currentTimeMillis();
      synchronized(responses) {
        responses.put(sender, response);
      }
      synchronized(doneSync) {
        if (responses.size() >= responderCount) {
          doneSync.notify();
        }
      }
    }
    
    boolean done() {
      synchronized(responses) {
        return responses.size() >= responderCount;
      }
    }
    
    void waitForReplies(long timeout) throws InterruptedException {
      synchronized(doneSync) {
        long endTime = System.currentTimeMillis() + timeout;
        while (!done()) {
          // compute remaining time in case of spurious wake-up
          long remainingTime = endTime - System.currentTimeMillis();
          if (remainingTime <= 0) {
            return;
          }
          doneSync.wait(remainingTime);
        }
      }
    }
  }
  
  public static class GFTimeSyncHeader extends Header implements Streamable {
    static final byte OP_TIME_REQUEST = 0;
    static final byte OP_TIME_RESPONSE = 1;
    static final byte OP_TIME_OFFSET = 2;
    static final byte JOIN_TIME_REQUEST = 3;
    static final byte JOIN_RESPONSE_OFFSET = 4;
    
    public long procID;
    public byte opType;
    public long time;
    public long coordTimeBeforeJoin;
    public long coordTimeAfterJoin;
    public transient long timeReceived; // set by ReplyProcessor when a response is received
    
    public GFTimeSyncHeader() {}
    
    public GFTimeSyncHeader(long procID, byte opType, long time) {
      super();
      this.procID = procID;
      this.opType = opType;
      this.time = time;
    }

    GFTimeSyncHeader(long procID, byte opType, long time, long beforeTime, long afterTime) {
      super();
      this.procID = procID;
      this.opType = opType;
      this.time = time;
      this.coordTimeBeforeJoin = beforeTime;
      this.coordTimeAfterJoin = afterTime;
    }

    @Override
    public long size(short version) {
      return super.size(version) + 17;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeLong(this.procID);
      out.writeLong(this.time);
      out.write(this.opType);
      if (JChannel.getGfFunctions().isVersionForStreamAtLeast(out, JGroupsVersion.GFE_80_ORDINAL)) {
        out.writeLong(this.coordTimeBeforeJoin);
        out.writeLong(this.coordTimeAfterJoin);
      }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
        ClassNotFoundException {
      this.procID = in.readLong();
      this.time = in.readLong();
      this.opType = in.readByte();
      if (JChannel.getGfFunctions().isVersionForStreamAtLeast(in, JGroupsVersion.GFE_80_ORDINAL)) {
        this.coordTimeBeforeJoin = in.readLong();
        this.coordTimeAfterJoin = in.readLong();
      }
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
      out.writeLong(this.procID);
      out.writeLong(this.time);
      out.write(this.opType);
      if (JChannel.getGfFunctions().isVersionForStreamAtLeast(out, JGroupsVersion.GFE_80_ORDINAL)) {
        out.writeLong(this.coordTimeBeforeJoin);
        out.writeLong(this.coordTimeAfterJoin);
      }
    }

    @Override
    public void readFrom(DataInputStream in) throws IOException,
        IllegalAccessException, InstantiationException {
      this.procID = in.readLong();
      this.time = in.readLong();
      this.opType = in.readByte();
      if (JChannel.getGfFunctions().isVersionForStreamAtLeast(in, JGroupsVersion.GFE_80_ORDINAL)) {
        this.coordTimeBeforeJoin = in.readLong();
        this.coordTimeAfterJoin = in.readLong();
      }
    }
    
    @Override
    public String toString() {
      return "SyncMessage(procID=" + this.procID + "; op="
          + op2String(this.opType) + "; time=" + this.time + "; timeRcvd="
          + this.timeReceived + "; coordTimeBeforeJoin="
          + this.coordTimeBeforeJoin + "; coordTimeAfterJoin="
          + this.coordTimeAfterJoin + ")";
    }
    
    private String op2String(byte op) {
      switch (op) {
      case OP_TIME_REQUEST: return "REQUEST";
      case OP_TIME_RESPONSE: return "RESPONSE";
      case OP_TIME_OFFSET: return "OFFSET";
      case JOIN_TIME_REQUEST: return "JOIN_TIME_REQUEST";
      case JOIN_RESPONSE_OFFSET: return "JOIN_OFFSET";
      }
      return "??";
    }
  }

  private class ServiceThread extends Thread {
    private boolean cancelled;
    private boolean waiting; // true if waiting for next scheduled time
    private boolean skipWait; // true if the thread should begin processing as soon as it finishes current view
    private Object lock = new Object();
    
    ServiceThread(ThreadGroup g, String name) {
      super(g,name);
    }

    @Override
    public void run() {
      synchronized(this.lock) {
        this.cancelled = false;
      }
      while (!cancelled()) {
        View v = view;
        if (v != null && v.getCreator().equals(localAddress)) {
          computeAndSendOffsets(v);
        }
        try {
          synchronized(this.lock) {
            if (this.skipWait) {
              this.skipWait = false;
            } else {
              this.waiting = true;
              try {
                this.lock.wait(GemFireTimeSync.this.clockSyncInterval*1000);
              } finally {
                this.waiting = false;
              }
            }
          }
        } catch (InterruptedException e) {
          // ignore unless cancelled
        }
      }
    }

    public boolean cancelled() {
      synchronized(this.lock) {
        return this.cancelled;
      }
    }

    public void cancel() {
      synchronized(this.lock) {
        this.cancelled = true;
        this.lock.notifyAll();
      }
    }
    
    public void computeOffsetsForNewView() {
      synchronized(this.lock) {
        if (this.waiting) {
          this.lock.notifyAll();
        } else {
          this.skipWait = true;
        }
      }
    }
  }


  /**
   * Use only for unit testing.
   */
  public void invokeServiceThreadForTest() {
    if (this.syncThread != null) {
      this.syncThread.computeOffsetsForNewView();
    }
  }

  /**
   * Use only for unit testing.
   */
  public boolean isServiceThreadCancelledForTest() {
    if (this.syncThread != null) {
      return this.syncThread.cancelled();
    } else {
      return true;
    }
  }
  
  public TestHook getTestHook() {
    return testHook;
  }

  public void setTestHook(TestHook testHook) {
    this.testHook = testHook;
  }

  public interface TestHook {
    
    public void hook(int barrier);

    public void setResponses(Map<Address, GFTimeSyncHeader> responses, long currentTime);
  }
}
