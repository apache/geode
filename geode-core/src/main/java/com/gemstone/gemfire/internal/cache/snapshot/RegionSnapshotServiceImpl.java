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
package com.gemstone.gemfire.internal.cache.snapshot;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ProxyRegion;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.snapshot.GFSnapshot.GFSnapshotImporter;
import com.gemstone.gemfire.internal.cache.snapshot.GFSnapshot.SnapshotWriter;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.getLoggerI18n;

/**
 * Provides an implementation for region snapshots.
 * 
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class RegionSnapshotServiceImpl<K, V> implements RegionSnapshotService<K, V> {
  // controls number of concurrent putAll ops during an import
  private static final int IMPORT_CONCURRENCY = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "RegionSnapshotServiceImpl.IMPORT_CONCURRENCY", 10);
  
  // controls the size (in bytes) of the r/w buffer during imoprt and export
  static final int BUFFER_SIZE = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "RegionSnapshotServiceImpl.BUFFER_SIZE", 1024 * 1024);
  
  static final SnapshotFileMapper LOCAL_MAPPER = new SnapshotFileMapper() {
    private static final long serialVersionUID = 1L;

    @Override
    public File mapExportPath(DistributedMember member, File snapshot) {
      return snapshot;
    }

    @Override
    public File[] mapImportPath(DistributedMember member, File snapshot) {
      if (!snapshot.isDirectory()) {
        return new File[] { snapshot };
      }
      
      return snapshot.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return !pathname.isDirectory();
        }
      });
    }
  };

  /**
   * Provides a destination for snapshot data during an export.
   */
  public interface ExportSink {
    /**
     * Writes snapshot data to the destination sink.
     * 
     * @param records the snapshot records
     * @throws IOException error writing records
     */
    void write(SnapshotRecord... records) throws IOException;
  }
  
  /**
   * Provides a strategy for exporting a region.
   *  
   * @param <K> the key type
   * @param <V> the value type
   */
  public interface Exporter<K, V> {
    /**
     * Exports the requested region.
     * 
     * @param region the region to export
     * @param sink the sink for the snapshot data
     * @param options snapshot options
     * @return number of entries exported
     * @throws IOException error during export
     */
    long export(Region<K, V> region, ExportSink sink, SnapshotOptions<K, V> options) throws IOException; 
  }
  
  /** the region */
  private final Region<K, V> region;
  
  public RegionSnapshotServiceImpl(Region<K, V> region) {
    this.region = region;
  }

  @Override
  public SnapshotOptions<K, V> createOptions() {
    return new SnapshotOptionsImpl<K, V>();
  }

  @Override
  public void save(File snapshot, SnapshotFormat format) throws IOException {
    save(snapshot, format, createOptions());
  }

  @Override
  public void save(File snapshot, SnapshotFormat format, SnapshotOptions<K, V> options) 
      throws IOException {
    // we can't be in a transaction since that will inline the function execution
    if (region.getCache().getCacheTransactionManager().exists()) {
      throw new IllegalStateException("Unable to save snapshot during a transaction");
    }
    
    if (shouldRunInParallel(options)) {
      snapshotInParallel(new ParallelArgs<K, V>(snapshot, format, options), new ParallelExportFunction<K, V>());
      return;
      
    } else {
      exportOnMember(snapshot, format, options);
    }
  }

  @Override
  public void load(File snapshot, SnapshotFormat format) throws IOException,
      ClassNotFoundException {
    load(snapshot, format, createOptions());
  }

  @Override
  public void load(File snapshot, SnapshotFormat format, SnapshotOptions<K, V> options) 
      throws IOException, ClassNotFoundException {

    if (shouldRunInParallel(options)) {
      snapshotInParallel(new ParallelArgs<K, V>(snapshot, format, options), new ParallelImportFunction<K, V>());
      return;
      
    } else {
      importOnMember(snapshot, format, options);
    }
  }

  private boolean shouldRunInParallel(SnapshotOptions<K, V> options) {
    return ((SnapshotOptionsImpl<K, V>) options).isParallelMode() 
        && region.getAttributes().getDataPolicy().withPartitioning()
        && !(region instanceof LocalDataSet);
  }

  private void snapshotInParallel(ParallelArgs<K, V> args, Function fn) throws IOException {
    try {
      
      ResultCollector rc = FunctionService.onRegion(region).withArgs(args).execute(fn);
      List result = (List) rc.getResult();
      for (Object obj : result) {
        if (obj instanceof Exception) {
          throw new IOException((Exception) obj);
        }
      }
      
      return;
    } catch (FunctionException e) {
      throw new IOException(e);
    }
  }

  private void importOnMember(File snapshot, SnapshotFormat format, SnapshotOptions<K, V> options)
      throws IOException, ClassNotFoundException {
    final LocalRegion local = getLocalRegion(region);
    
    if (getLoggerI18n().infoEnabled())
      getLoggerI18n().info(LocalizedStrings.Snapshot_IMPORT_BEGIN_0, region.getName());
    
    long count = 0;
    long bytes = 0;
    long start = CachePerfStats.getStatTime();
    
    // Would be interesting to use a PriorityQueue ordered on isDone()
    // but this is probably close enough in practice.
    LinkedList<Future<?>> puts = new LinkedList<Future<?>>();
    GFSnapshotImporter in = new GFSnapshotImporter(snapshot);

    try {
      int bufferSize = 0;
      Map<K, V> buffer = new HashMap<K, V>();
      
      SnapshotRecord record;
      while ((record = in.readSnapshotRecord()) != null) {
        bytes += record.getSize();
        K key = record.getKeyObject();
        
        // Until we modify the semantics of put/putAll to allow null values we
        // have to subvert the API by using Token.INVALID.  Alternatively we could
        // invoke create/invalidate directly but that prevents us from using
        // bulk operations.  The ugly type coercion below is necessary to allow 
        // strong typing elsewhere.
        V val = (V) Token.INVALID;
        if (record.hasValue()) {
          byte[] data = record.getValue();
          // If the underlying object is a byte[], we can't wrap it in a
          // CachedDeserializable.  Somewhere along the line the header bytes
          // get lost and we start seeing serialization problems.
          if (data.length > 0 && data[0] == DSCODE.BYTE_ARRAY) {
            // It would be faster to use System.arraycopy() directly but since
            // length field is variable it's probably safest and simplest to
            // keep the logic in the InternalDataSerializer.
            val = record.getValueObject();
          } else {
            val = (V) CachedDeserializableFactory.create(record.getValue());
          }
        }

        if (includeEntry(options, key, val)) {
          buffer.put(key, val);
          bufferSize += record.getSize();
          count++;
          
          // Push entries into cache using putAll on a separate thread so we
          // can keep the disk busy. Throttle puts so we don't overwhelm the cache.
          if (bufferSize > BUFFER_SIZE) {
            if (puts.size() == IMPORT_CONCURRENCY) {
              puts.removeFirst().get();
            }
            
            final Map<K, V> copy = new HashMap<K, V>(buffer);
            Future<?> f = GemFireCacheImpl
                .getExisting("Importing region from snapshot")
                .getDistributionManager().getWaitingThreadPool()
                .submit(new Runnable() {
              @Override
              public void run() {
                local.basicImportPutAll(copy, true);
              }
            });
            
            puts.addLast(f);
            buffer.clear();
            bufferSize = 0;
          }
        }
      }

      // send off any remaining entries
      if (!buffer.isEmpty()) {
        local.basicImportPutAll(buffer, true);
      }
      
      // wait for completion and check for errors
      while (!puts.isEmpty()) {
        puts.removeFirst().get();
      }

      if (getLoggerI18n().infoEnabled()) {
        getLoggerI18n().info(LocalizedStrings.Snapshot_IMPORT_END_0_1_2_3, 
            new Object[] { count, bytes, region.getName(), snapshot });
      }
      
    } catch (InterruptedException e) {
      while (!puts.isEmpty()) {
        puts.removeFirst().cancel(true);
      }
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException().initCause(e);

    } catch (ExecutionException e) {
      while (!puts.isEmpty()) {
        puts.removeFirst().cancel(true);
      }
      throw new IOException(e);
      
    } finally {
      in.close();
      local.getCachePerfStats().endImport(count, start);
    }
  }

  private void exportOnMember(File snapshot, SnapshotFormat format, SnapshotOptions<K, V> options)
      throws IOException {
    LocalRegion local = getLocalRegion(region);
    Exporter<K, V> exp = createExporter(region, options);

    if (getLoggerI18n().fineEnabled()) {
      getLoggerI18n().fine("Writing to snapshot " + snapshot.getAbsolutePath());
    }
    
    long count = 0;
    long start = CachePerfStats.getStatTime();
    SnapshotWriter writer = GFSnapshot.create(snapshot, region.getFullPath());
    try {
      if (getLoggerI18n().infoEnabled())
        getLoggerI18n().info(LocalizedStrings.Snapshot_EXPORT_BEGIN_0, region.getName());

      SnapshotWriterSink sink = new SnapshotWriterSink(writer);
      count = exp.export(region, sink, options);
      
      if (getLoggerI18n().infoEnabled()) {
        getLoggerI18n().info(LocalizedStrings.Snapshot_EXPORT_END_0_1_2_3, 
            new Object[] { count, sink.getBytesWritten(), region.getName(), snapshot });
      }

    } finally {
      writer.snapshotComplete();
      local.getCachePerfStats().endExport(count, start);
    }
  }

  private boolean includeEntry(SnapshotOptions<K, V> options, final K key, final V val) {
    if (options.getFilter() != null) {
      Entry<K, V> entry = new Entry<K, V>() {
        @Override public V setValue(V value) { throw new UnsupportedOperationException(); }
        @Override public K getKey() { return key; }
        @Override public V getValue() { 
          if (val instanceof CachedDeserializable) {
            return (V) ((CachedDeserializable) val).getDeserializedForReading();
          }
          return null;
        }
      };
      
      return options.getFilter().accept(entry);
    }
    return true;
  }
  
  static <K, V> Exporter<K, V> createExporter(Region<?, ?> region, SnapshotOptions<K, V> options) {
    String pool = region.getAttributes().getPoolName();
    if (pool != null) {
      return new ClientExporter<K, V>(PoolManager.find(pool));
      
    } else if (InternalDistributedSystem.getAnyInstance().isLoner()
        || region.getAttributes().getDataPolicy().equals(DataPolicy.NORMAL)
        || region.getAttributes().getDataPolicy().equals(DataPolicy.PRELOADED)
        || region instanceof LocalDataSet
        || (((SnapshotOptionsImpl<K, V>) options).isParallelMode() 
            && region.getAttributes().getDataPolicy().withPartitioning())) {
      
      // Avoid function execution:
      //    for loner systems to avoid inlining fn execution
      //    for NORMAL/PRELOAD since they don't support fn execution
      //    for LocalDataSet since we're already running a fn
      //    for parallel ops since we're already running a fn
      return new LocalExporter<K, V>();
    }
    
    return new WindowedExporter<K, V>();
  }

  static LocalRegion getLocalRegion(Region<?, ?> region) {
    if (region instanceof LocalDataSet) {
      return ((LocalDataSet) region).getProxy();
    } else if (region instanceof ProxyRegion) {
      return (LocalRegion) ((ProxyRegion) region).getRealRegion();
    }
    return (LocalRegion) region;
  }
  
  /**
   * Writes snapshot data to a {@link SnapshotWriter}.  Caller is responsible
   * for invoking {@link SnapshotWriter#snapshotComplete()}.
   */
  static final class SnapshotWriterSink implements ExportSink {
    private final SnapshotWriter writer;
    private long bytes;
    
    public SnapshotWriterSink(SnapshotWriter writer) {
      this.writer = writer;
    }
    
    @Override
    public void write(SnapshotRecord... records) throws IOException {
      for (SnapshotRecord rec : records) {
        writer.snapshotEntry(rec);
        bytes += rec.getSize();
      }
    }
    
    public long getBytesWritten() {
      return bytes;
    }
  }
  
  /**
   * Forwards snapshot data to a {@link ResultSender}.  Caller is responsible for
   * invoking {@link ResultSender#lastResult(Object)}.
   */
  static final class ResultSenderSink implements ExportSink {
    /** the fowarding destination */
    private final ResultSender<SnapshotRecord[]> sender;
    
    public ResultSenderSink(ResultSender<SnapshotRecord[]> sender) {
      this.sender = sender;
    }
    
    @Override
    public void write(SnapshotRecord... records) throws IOException {
      sender.sendResult(records);
    }
  }
  
  /**
   * Carries the arguments to the export function.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  private static class ParallelArgs<K, V> implements Serializable {
    private static final long serialVersionUID = 1;
    
    private final File file;
    private final SnapshotFormat format;
    private final SnapshotOptionsImpl<K, V> options;
    
    public ParallelArgs(File f, SnapshotFormat format, SnapshotOptions<K, V> options) {
      this.file = f;
      this.format = format;
      
      // since we don't expose the parallel mode, we have to downcast...ugh
      this.options = (SnapshotOptionsImpl<K, V>) options;
    }
    
    public File getFile() {
      return file;
    }
    
    public SnapshotFormat getFormat() {
      return format;
    }
    
    public SnapshotOptionsImpl<K, V> getOptions() {
      return options;
    }
  }
  
  private static class ParallelExportFunction<K, V> implements Function {
    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public void execute(FunctionContext context) {
      try {
        Region<K, V> local = PartitionRegionHelper.getLocalDataForContext((RegionFunctionContext) context);
        ParallelArgs<K, V> args = (ParallelArgs<K, V>) context.getArguments();
        
        File f = args.getOptions().getMapper().mapExportPath(
            local.getCache().getDistributedSystem().getDistributedMember(), args.getFile());
        
        if (f == null || f.isDirectory()) {
          throw new IOException(LocalizedStrings.Snapshot_INVALID_EXPORT_FILE.toLocalizedString(f));
        }
        
        local.getSnapshotService().save(f, args.getFormat(), args.getOptions());
        context.getResultSender().lastResult(Boolean.TRUE);
        
      } catch (Exception e) {
        context.getResultSender().sendException(e);
      }
    }

    @Override
    public String getId() {
      return "com.gemstone.gemfire.cache.snapshot.ParallelExport";
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  private static class ParallelImportFunction<K, V> implements Function {
    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public void execute(FunctionContext context) {
      try {
        Region<K, V> local = PartitionRegionHelper.getLocalDataForContext((RegionFunctionContext) context);
        ParallelArgs<K, V> args = (ParallelArgs<K, V>) context.getArguments();

        File[] files = args.getOptions().getMapper().mapImportPath(
            local.getCache().getDistributedSystem().getDistributedMember(), args.getFile());
        
        if (files != null) {
          for (File f : files) {
            if (f.isDirectory() || !f.exists()) {
              throw new IOException(LocalizedStrings.Snapshot_INVALID_IMPORT_FILE.toLocalizedString(f));
            }
            local.getSnapshotService().load(f, args.getFormat(), args.getOptions());
          }
        }
        context.getResultSender().lastResult(Boolean.TRUE);
        
      } catch (Exception e) {
        context.getResultSender().sendException(e);
      }
    }

    @Override
    public String getId() {
      return "com.gemstone.gemfire.cache.snapshot.ParallelImport";
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }
}
