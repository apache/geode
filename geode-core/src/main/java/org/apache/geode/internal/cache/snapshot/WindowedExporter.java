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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.execute.InternalExecution;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.LocalResultCollector;
import org.apache.geode.internal.cache.snapshot.FlowController.Window;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl.ExportSink;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl.Exporter;
import org.apache.geode.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;

/**
 * Exports snapshot data using a sliding window to prevent the nodes in a partitioned region from
 * overrunning the exporter. When a {@link SnapshotPacket} is written to the {@link ExportSink}, an
 * ACK is sent back to the source node. The source node will continue to send data until it runs out
 * of permits; it must then wait for ACK's to resume.
 *
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class WindowedExporter<K, V> implements Exporter<K, V> {
  private static final int WINDOW_SIZE =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "WindowedExporter.WINDOW_SIZE", 10);

  @Override
  public long export(Region<K, V> region, ExportSink sink, SnapshotOptions<K, V> options)
      throws IOException {
    long count = 0;
    boolean error = true;
    LocalRegion local = RegionSnapshotServiceImpl.getLocalRegion(region);

    SnapshotPacket last = new SnapshotPacket();
    DistributedMember me = region.getCache().getDistributedSystem().getDistributedMember();

    WindowedArgs<K, V> args = new WindowedArgs<K, V>(me, options);
    WindowedExportCollector results = new WindowedExportCollector(local, last);
    try {
      // Since the ExportCollector already is a LocalResultsCollector it's ok not
      // to keep the reference to the ResultsCollector returned from execute().
      // Normally discarding the reference can cause issues if GC causes the
      // weak ref in ProcessorKeeper21 to be collected!!
      InternalExecution exec = (InternalExecution) FunctionService.onRegion(region)
          .setArguments(args).withCollector(results);

      // Ensure that our collector gets all exceptions so we can shut down the
      // queue properly.
      exec.setForwardExceptions(true);
      exec.execute(new WindowedExportFunction<K, V>());

      BlockingQueue<SnapshotPacket> queue = results.getResult();

      SnapshotPacket packet;
      while ((packet = queue.take()) != last) {
        results.ack(packet);
        sink.write(packet.getRecords());
        count += packet.getRecords().length;
      }

      error = false;
      FunctionException ex = results.getException();
      if (ex != null) {
        throw new IOException(ex);
      }
    } catch (FunctionException e) {
      throw new IOException(e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException().initCause(e);

    } finally {
      if (error) {
        results.abort();
      }
    }
    return count;
  }

  /**
   * Carries the arguments to the export function.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  private static class WindowedArgs<K, V> implements Serializable {
    private static final long serialVersionUID = 1;

    private final DistributedMember exporter;
    private final SnapshotOptions<K, V> options;

    public WindowedArgs(DistributedMember exporter, SnapshotOptions<K, V> options) {
      this.exporter = exporter;
      this.options = options;
    }

    public DistributedMember getExporter() {
      return exporter;
    }

    public SnapshotOptions<K, V> getOptions() {
      return options;
    }
  }

  /**
   * Gathers the local data on the region and sends it back to the {@link ResultCollector} in
   * serialized form as {@link SnapshotPacket}s. Uses a sliding window provided by the
   * {@link FlowController} to avoid over-running the exporting member.
   *
   * @param <K> the key type
   * @param <V> the value type
   *
   * @see FlowController
   */
  private static class WindowedExportFunction<K, V> implements InternalFunction {
    private static final long serialVersionUID = 1L;

    // We must keep a ref here since the ProcessorKeeper only has a weak ref. If
    // this object is GC'd it could cause a hang since we will no longer receive
    // ACK's for every packet.
    private transient volatile Window window;

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public void execute(FunctionContext context) {
      RegionFunctionContext ctx = (RegionFunctionContext) context;

      final WindowedArgs<K, V> args = (WindowedArgs<K, V>) ctx.getArguments();
      ResultSender<SnapshotPacket> rs = ctx.getResultSender();

      Region<K, V> region = ctx.getDataSet();
      if (PartitionRegionHelper.isPartitionedRegion(region)) {
        region = PartitionRegionHelper.getLocalDataForContext(ctx);
      }

      LocalRegion local = RegionSnapshotServiceImpl.getLocalRegion(region);
      window = FlowController.getInstance().create(region, args.getExporter(), WINDOW_SIZE);

      try {
        int bufferSize = 0;
        List<SnapshotRecord> buffer = new ArrayList<SnapshotRecord>();
        DistributedMember me = region.getCache().getDistributedSystem().getDistributedMember();
        for (Iterator<Entry<K, V>> iter = region.entrySet().iterator(); iter.hasNext()
            && !window.isAborted();) {
          Entry<K, V> entry = iter.next();
          try {
            SnapshotOptions<K, V> options = args.getOptions();
            if (options.getFilter() == null || options.getFilter().accept(entry)) {
              SnapshotRecord rec = new SnapshotRecord(local, entry);
              buffer.add(rec);
              bufferSize += rec.getSize();
            }
          } catch (EntryDestroyedException e) {
            // continue to next entry

          } catch (IOException e) {
            throw new FunctionException(e);
          }

          if (bufferSize > RegionSnapshotServiceImpl.BUFFER_SIZE) {
            window.waitForOpening();
            rs.sendResult(new SnapshotPacket(window.getWindowId(), me, buffer));
            buffer.clear();
            bufferSize = 0;
          }
        }

        window.waitForOpening();
        rs.lastResult(new SnapshotPacket(window.getWindowId(), me, buffer));
        if (getLogger().fineEnabled())
          getLogger().fine("SNP: Sent all entries in region " + region.getName());

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new FunctionException(e);

      } finally {
        window.close();
      }
    }

    @Override
    public String getId() {
      return "org.apache.geode.cache.snapshot.WindowedExport";
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  /**
   * Collects export results and places them in a queue for processing by the function invoker.
   */
  private static class WindowedExportCollector
      implements LocalResultCollector<Object, BlockingQueue<SnapshotPacket>> {
    /** the region being exported */
    private final LocalRegion region;

    /** marks the end of the queue */
    private final SnapshotPacket end;

    /** queue used to stream the snapshot entries */
    private final BlockingQueue<SnapshotPacket> entries;

    /** true if no more results are expected */
    private final AtomicBoolean done;

    /** the members involved in the export */
    private final Map<DistributedMember, Integer> members;

    /** set if there is an error during execution */
    private volatile FunctionException exception;

    /** store a ref to the processor to prevent the processor from being GC'd */
    private volatile ReplyProcessor21 processor;

    public WindowedExportCollector(LocalRegion region, SnapshotPacket end) {
      this.region = region;
      this.end = end;

      done = new AtomicBoolean(false);
      members = new ConcurrentHashMap<DistributedMember, Integer>();

      // cannot bound queue to exert back pressure
      entries = new LinkedBlockingQueue<SnapshotPacket>();
    }

    @Override
    public BlockingQueue<SnapshotPacket> getResult() throws FunctionException {
      return entries;
    }

    @Override
    public BlockingQueue<SnapshotPacket> getResult(long timeout, TimeUnit unit)
        throws FunctionException, InterruptedException {
      return getResult();
    }

    /**
     * Returns an exception that occurred during function exception.
     *
     * @return the exception, or null
     */
    public FunctionException getException() {
      return exception;
    }

    /**
     * Aborts any further collection of results and forwards the cancellation to the members
     * involved in the export.
     */
    public void abort() {
      try {
        if (done.compareAndSet(false, true)) {
          if (getLogger().fineEnabled())
            getLogger().fine("SNP: Aborting export of region");

          entries.clear();
          entries.put(end);

          for (Entry<DistributedMember, Integer> entry : members.entrySet()) {
            sendAbort(entry.getKey(), entry.getValue());
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public void ack(SnapshotPacket packet) {
      FlowController.getInstance().sendAck(region.getDistributionManager(), packet.getSender(),
          packet.getWindowId(), packet.getPacketId());
    }

    @Override
    public void addResult(DistributedMember memberID, Object result) {
      // need to track participants so we can send acks and aborts
      if (!(result instanceof Throwable)) {
        int flowId = ((SnapshotPacket) result).getWindowId();
        if (done.get()) {
          sendAbort(memberID, flowId);

        } else {
          members.put(memberID, flowId);
        }
      }

      if (!done.get()) {
        try {
          if (result instanceof Throwable) {
            setException((Throwable) result);
            endResults();

          } else {
            SnapshotPacket sp = (SnapshotPacket) result;
            entries.put(sp);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void endResults() {
      try {
        if (done.compareAndSet(false, true)) {
          if (getLogger().fineEnabled())
            getLogger()
                .fine("SNP: All results received for export of region " + region.getName());

          entries.put(end);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void clearResults() {
      entries.clear();
      done.set(false);
      exception = null;
    }

    @Override
    public void setException(Throwable ex) {
      exception =
          (ex instanceof FunctionException) ? (FunctionException) ex : new FunctionException(ex);
    }

    @Override
    public void setProcessor(ReplyProcessor21 processor) {
      this.processor = processor;
    }

    @Override
    public ReplyProcessor21 getProcessor() {
      return processor;
    }

    private void sendAbort(DistributedMember member, int flowId) {
      FlowController.getInstance().sendAbort(region.getDistributionManager(), flowId, member);
    }
  }
}
