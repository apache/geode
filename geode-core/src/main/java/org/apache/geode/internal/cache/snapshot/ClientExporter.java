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

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl.ExportSink;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl.Exporter;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl.ResultSenderSink;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;

/**
 * Gathers snapshot data from the server using a proxy function.  If PRSingleHop
 * is enabled, the proxy function will use a {@link LocalExporter} to forward results
 * directly back to the client.  This relies on TCP queuing to provide back pressure
 * on the senders.  If PRSingleHop is not enabled, the proxy function will use a
 * {@link WindowedExporter} to rate the data gathering prior to forwarding back
 * to the client.  The client uses a custom {@link ResultCollector} to write
 * entries immediately into the snapshot file.
 * 
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ClientExporter<K, V> implements Exporter<K, V> {
  private final Pool pool;
  
  public ClientExporter(Pool p) {
    pool = p;
  }
  
  @Override
  public long export(Region<K, V> region, ExportSink sink, SnapshotOptions<K, V> options) throws IOException {
    try {
      ClientArgs<K, V> args = new ClientArgs<K, V>(region.getFullPath(), pool.getPRSingleHopEnabled(), options);
      ClientExportCollector results = new ClientExportCollector(sink);
      
      // For single hop we rely on tcp queuing to throttle the export; otherwise
      // we allow the WindowedExporter to provide back pressure. 
      Execution exec = pool.getPRSingleHopEnabled() 
          ? FunctionService.onRegion(region) 
          : FunctionService.onServer(pool);
          
      ResultCollector<?, ?> rc = exec
          .withArgs(args)
          .withCollector(results)
          .execute(new ProxyExportFunction<K, V>());
      
      // Our custom result collector is writing the data, but this will
      // check for errors.
      return (Long) rc.getResult();

    } catch (FunctionException e) {
      throw new IOException(e);
    }
  }
  
  /**
   * Carries the arguments to the export function.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  private static class ClientArgs<K, V> implements Serializable {
    private static final long serialVersionUID = 1;
    
    private final String region;
    private final boolean prSingleHop;
    private final SnapshotOptions<K, V> options;
    
    public ClientArgs(String region, boolean prSingleHop, SnapshotOptions<K, V> options) {
      this.region = region;
      this.prSingleHop = prSingleHop;
      this.options = options;
    }
    
    public String getRegion() {
      return region;
    }
    
    public boolean isPRSingleHop() {
      return prSingleHop;
    }
    
    public SnapshotOptions<K, V> getOptions() {
      return options;
    }
  }
  
  /**
   * Gathers snapshot data on the server and forwards it back to the client.
   *
   * @param <K> the key type
   * @param <V> the value type
   */
  private static class ProxyExportFunction<K, V> implements Function {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public void execute(FunctionContext context) {
      ClientArgs<K, V> args = (ClientArgs<K, V>) context.getArguments();
      ResultSender rs = context.getResultSender();
      ExportSink sink = new ResultSenderSink(rs);
      
      Region<K, V> region = GemFireCacheImpl.getExisting("Exporting snapshot").getRegion(args.getRegion());
      Exporter<K, V> exp = args.isPRSingleHop() 
          ? new LocalExporter<K, V>() 
          : RegionSnapshotServiceImpl.<K, V>createExporter(region, args.options);
      
      try {
        long count = exp.export(region, sink, args.getOptions());
        rs.lastResult(count);
        
      } catch (IOException e) {
        rs.sendException(e);
      }
    }

    @Override
    public String getId() {
      return "com.gemstone.gemfire.cache.snapshot.ClientExport";
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
   * Streams snapshot data into the supplied {@link ExportSink}.  Since
   * {@link Execution#execute(Function)} is a blocking call on clients, we need 
   * to write immediately.
   */
  private static class ClientExportCollector implements ResultCollector<Object, Long> {
    /** the number of records written */
    private final AtomicLong count;
    
    /** the sink for the snapshot data */
    private final ExportSink sink;
    
    /** the error, or null */
    private volatile Exception error;
    
    public ClientExportCollector(ExportSink sink) {
      this.sink = sink;
      count = new AtomicLong(0);
    }
    
    @Override
    public Long getResult() throws FunctionException {
      if (error != null) {
        throw new FunctionException(error);
      }
      return count.get();
    }

    @Override
    public Long getResult(long timeout, TimeUnit unit)
        throws FunctionException, InterruptedException {
      return getResult();
    }

    @Override
    public void addResult(DistributedMember memberID, Object result) {
      if (result instanceof Long)  {
        count.addAndGet((Long) result);
        
      } else if (result instanceof Exception) {
        error = (Exception) result;
        
      } else {
        try {
          sink.write((SnapshotRecord[]) result);
        } catch (IOException e) {
          error = e;
        }
      }
    }

    @Override
    public void endResults() {
    }

    @Override
    public void clearResults() {
    }
  }
}
