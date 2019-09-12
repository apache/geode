/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.snapshot;

import static org.apache.geode.distributed.internal.InternalDistributedSystem.getLogger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.util.RegionMembershipListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ProcessorKeeper21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Provides flow control using permits based on the sliding window algorithm. The sender should
 * invoke {@link #create(Region, DistributedMember, int)} while the recipient should respond with
 * {@link #sendAck(DistributionManager, DistributedMember, int, String)} or
 * {@link #sendAbort(DistributionManager, int, DistributedMember)}.
 *
 */
public class FlowController {
  // watch out for rollover problems with MAX_VALUE
  private static final int MAX_PERMITS = Integer.MAX_VALUE / 2;

  /**
   * Provides a callback interface for sliding window flow control.
   */
  public interface Window {
    /**
     * Returns the window id.
     *
     * @return the window id
     */
    int getWindowId();

    /**
     * Returns true if the operation has been aborted.
     *
     * @return true if aborted
     */
    boolean isAborted();

    /**
     * Returns true if the window is open and {{@link #waitForOpening()} will return immediately.
     *
     * @return true if open
     */
    boolean isOpen();

    /**
     * Blocks until the window is open.
     *
     * @throws InterruptedException Interrupted while waiting
     */
    void waitForOpening() throws InterruptedException;

    /**
     * Closes the window and releases resources.
     */
    void close();
  }

  /** the singleton */
  @MakeNotStatic
  private static final FlowController instance = new FlowController();

  public static FlowController getInstance() {
    return instance;
  }

  /** keeps a weak ref to {@link WindowImpl} implementations */
  private final ProcessorKeeper21 processors;

  private FlowController() {
    processors = new ProcessorKeeper21();
  }

  /**
   * Creates and registers a {@link Window} that provides flow control.
   *
   * @param region the region
   * @param sink the data recipient
   * @param windowSize the size of the sliding window
   *
   * @see #sendAbort(DistributionManager, int, DistributedMember)
   * @see #sendAck(DistributionManager, DistributedMember, int, String)
   */
  public <K, V> Window create(Region<K, V> region, DistributedMember sink, int windowSize) {
    WindowImpl<K, V> w = new WindowImpl<K, V>(region, sink, windowSize);
    int id = processors.put(w);

    w.setWindowId(id);
    return w;
  }

  /**
   * Sends an ACK to allow the source to continue sending messages.
   *
   * @param dmgr the distribution manager
   * @param member the data source
   * @param windowId the window
   * @param packetId the packet being ACK'd
   */
  public void sendAck(DistributionManager dmgr, DistributedMember member, int windowId,
      String packetId) {
    if (getLogger().fineEnabled())
      getLogger().fine("SNP: Sending ACK for packet " + packetId + " on window " + windowId
          + " to member " + member);

    if (dmgr.getDistributionManagerId().equals(member)) {
      WindowImpl<?, ?> win = (WindowImpl<?, ?>) processors.retrieve(windowId);
      if (win != null) {
        win.ack(packetId);
      }
    } else {
      FlowControlAckMessage ack = new FlowControlAckMessage(windowId, packetId);
      ack.setRecipient((InternalDistributedMember) member);
      dmgr.putOutgoing(ack);
    }
  }

  /**
   * Aborts further message processing.
   *
   * @param dmgr the distribution manager
   * @param windowId the window
   * @param member the data source
   */
  public void sendAbort(DistributionManager dmgr, int windowId, DistributedMember member) {
    if (getLogger().fineEnabled())
      getLogger().fine("SNP: Sending ABORT to member " + member + " for window " + windowId);

    if (dmgr.getDistributionManagerId().equals(member)) {
      WindowImpl<?, ?> win = (WindowImpl<?, ?>) processors.retrieve(windowId);
      if (win != null) {
        win.abort();
      }
    } else {
      FlowControlAbortMessage abort = new FlowControlAbortMessage(windowId);
      abort.setRecipient((InternalDistributedMember) member);
      dmgr.putOutgoing(abort);
    }
  }

  private static class WindowImpl<K, V> implements Window {
    /** controls access to the window */
    private final Semaphore permits;

    /** true if aborted */
    private final AtomicBoolean abort;

    /** the region (used to manage membership) */
    private final Region<K, V> region;

    /** the membership listener */
    private final RegionMembershipListener<K, V> crash;

    /** the window id */
    private volatile int windowId;

    public WindowImpl(Region<K, V> region, final DistributedMember sink, int size) {
      permits = new Semaphore(size);
      abort = new AtomicBoolean(false);

      this.region = region;
      crash = new RegionMembershipListenerAdapter<K, V>() {
        @Override
        public void afterRemoteRegionCrash(RegionEvent<K, V> event) {
          if (event.getDistributedMember().equals(sink)) {
            if (getLogger().fineEnabled())
              getLogger().fine("SNP: " + sink + " has crashed, closing window");

            abort();
          }
        }
      };
      region.getAttributesMutator().addCacheListener(crash);
    }

    @Override
    public void close() {
      instance.processors.remove(windowId);
      region.getAttributesMutator().removeCacheListener(crash);
      permits.release(MAX_PERMITS);
    }

    @Override
    public int getWindowId() {
      return windowId;
    }

    @Override
    public boolean isAborted() {
      return abort.get();
    }

    @Override
    public boolean isOpen() {
      return permits.availablePermits() > 0;
    }

    @Override
    public void waitForOpening() throws InterruptedException {
      permits.acquire();
    }

    private void ack(String packetId) {
      permits.release();
    }

    private void abort() {
      abort.set(true);
      permits.release(MAX_PERMITS);
    }

    private void setWindowId(int id) {
      windowId = id;
    }
  }

  /**
   * Sent to abort message processing.
   *
   * @see Window#isAborted()
   * @see FlowController#sendAbort(DistributionManager, int, DistributedMember)
   */
  public static class FlowControlAbortMessage extends DistributionMessage {
    /** the window id */
    private int windowId;

    public FlowControlAbortMessage(int windowId) {
      this.windowId = windowId;
    }

    /** for deserialization */
    public FlowControlAbortMessage() {}

    @Override
    public int getDSFID() {
      return FLOW_CONTROL_ACK;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.STANDARD_EXECUTOR;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      if (getLogger().fineEnabled())
        getLogger()
            .fine("SNP: Received ABORT on window " + windowId + " from member " + getSender());

      WindowImpl<?, ?> win =
          (WindowImpl<?, ?>) FlowController.getInstance().processors.retrieve(windowId);
      if (win != null) {
        win.abort();
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      windowId = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(windowId);
    }
  }

  /**
   * Sent to acknowledge receipt of a message packet.
   *
   * @see FlowController#sendAck(DistributionManager, DistributedMember, int, String)
   */
  public static class FlowControlAckMessage extends DistributionMessage {
    /** the window id */
    private int windowId;

    /** the packet id */
    private String packetId;

    public FlowControlAckMessage(int windowId, String packetId) {
      this.windowId = windowId;
      this.packetId = packetId;
    }

    /** for deserialization */
    public FlowControlAckMessage() {}

    @Override
    public int getDSFID() {
      return FLOW_CONTROL_ACK;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.STANDARD_EXECUTOR;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      if (getLogger().fineEnabled())
        getLogger().fine("SNP: Received ACK for packet " + packetId + " on window " + windowId
            + " from member " + getSender());

      WindowImpl<?, ?> win =
          (WindowImpl<?, ?>) FlowController.getInstance().processors.retrieve(windowId);
      if (win != null) {
        win.ack(packetId);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      windowId = in.readInt();
      packetId = InternalDataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(windowId);
      InternalDataSerializer.writeString(packetId, out);
    }
  }
}
