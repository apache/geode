package com.gemstone.gemfire.internal.offheap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EntryBits;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ConcurrentBag.Node;
import com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.internal.shared.StringPrintWriter;

/**
 * This allocator is somewhat like an Arena allocator.
 * We start out with an array of multiple large chunks of memory.
 * We also keep lists of any chunk that have been allocated and freed.
 * An allocation will always try to find a chunk in a free list that is a close fit to the requested size.
 * If no close fits exist then it allocates the next slice from the front of one the original large chunks.
 * If we can not find enough free memory then all the existing free memory is compacted.
 * If we still do not have enough to make the allocation an exception is thrown.
 * 
 * @author darrel
 * @author Kirk Lund
 * @since 9.0
 */
public final class SimpleMemoryAllocatorImpl implements MemoryAllocator, MemoryInspector {

  private static final Logger logger = LogService.getLogger();
  
  public static final String FREE_OFF_HEAP_MEMORY_PROPERTY = "gemfire.free-off-heap-memory";
  
  /**
   * How many extra allocations to do for each actual slab allocation.
   * Is this really a good idea?
   */
  public static final int BATCH_SIZE = Integer.getInteger("gemfire.OFF_HEAP_BATCH_ALLOCATION_SIZE", 1);
  /**
   * Every allocated chunk smaller than TINY_MULTIPLE*TINY_FREE_LIST_COUNT will allocate a chunk of memory that is a multiple of this value.
   * Sizes are always rounded up to the next multiple of this constant
   * so internal fragmentation will be limited to TINY_MULTIPLE-1 bytes per allocation
   * and on average will be TINY_MULTIPLE/2 given a random distribution of size requests.
   */
  public final static int TINY_MULTIPLE = Integer.getInteger("gemfire.OFF_HEAP_ALIGNMENT", 8);
  /**
   * Number of free lists to keep for tiny allocations.
   */
  public final static int TINY_FREE_LIST_COUNT = Integer.getInteger("gemfire.OFF_HEAP_FREE_LIST_COUNT", 16384);
  public final static int MAX_TINY = TINY_MULTIPLE*TINY_FREE_LIST_COUNT;
  public final static int HUGE_MULTIPLE = 256;
  
  private volatile OffHeapMemoryStats stats;
  
  private volatile OutOfOffHeapMemoryListener ooohml;
  
  /** The MemoryChunks that this allocator is managing by allocating smaller chunks of them.
   * The contents of this array never change.
   */
  private final UnsafeMemoryChunk[] slabs;
  private final long totalSlabSize;
  private final int largestSlab;
  
  public final FreeListManager freeList;
  
  private volatile MemoryUsageListener[] memoryUsageListeners = new MemoryUsageListener[0];
  
  private static SimpleMemoryAllocatorImpl singleton = null;
  private static final AtomicReference<Thread> asyncCleanupThread = new AtomicReference<Thread>();
  private final ChunkFactory chunkFactory;
  
  public static SimpleMemoryAllocatorImpl getAllocator() {
    SimpleMemoryAllocatorImpl result = singleton;
    if (result == null) {
      throw new CacheClosedException("Off Heap memory allocator does not exist.");
    }
    return result;
  }

  private static final boolean PRETOUCH = Boolean.getBoolean("gemfire.OFF_HEAP_PRETOUCH_PAGES");
  static final int OFF_HEAP_PAGE_SIZE = Integer.getInteger("gemfire.OFF_HEAP_PAGE_SIZE", UnsafeMemoryChunk.getPageSize());
  private static final boolean DO_EXPENSIVE_VALIDATION = Boolean.getBoolean("gemfire.OFF_HEAP_DO_EXPENSIVE_VALIDATION");;
  
  public static MemoryAllocator create(OutOfOffHeapMemoryListener ooohml, OffHeapMemoryStats stats, LogWriter lw, int slabCount, long offHeapMemorySize, long maxSlabSize) {
    SimpleMemoryAllocatorImpl result = singleton;
    boolean created = false;
    try {
    if (result != null) {
      result.reuse(ooohml, lw, stats, offHeapMemorySize);
      lw.config("Reusing " + result.getTotalMemory() + " bytes of off-heap memory. The maximum size of a single off-heap object is " + result.largestSlab + " bytes.");
      created = true;
      invokeAfterReuse(result);
    } else {
      // allocate memory chunks
      //SimpleMemoryAllocatorImpl.cleanupPreviousAllocator();
      lw.config("Allocating " + offHeapMemorySize + " bytes of off-heap memory. The maximum size of a single off-heap object is " + maxSlabSize + " bytes.");
      UnsafeMemoryChunk[] slabs = new UnsafeMemoryChunk[slabCount];
      long uncreatedMemory = offHeapMemorySize;
      for (int i=0; i < slabCount; i++) {
        try {
        if (uncreatedMemory >= maxSlabSize) {
          slabs[i] = new UnsafeMemoryChunk((int) maxSlabSize);
          uncreatedMemory -= maxSlabSize;
        } else {
          // the last slab can be smaller then maxSlabSize
          slabs[i] = new UnsafeMemoryChunk((int) uncreatedMemory);
        }
        } catch (OutOfMemoryError err) {
          if (i > 0) {
            lw.severe("Off-heap memory creation failed after successfully allocating " + (i*maxSlabSize) + " bytes of off-heap memory.");
          }
          for (int j=0; j < i; j++) {
            if (slabs[j] != null) {
              slabs[j].release();
            }
          }
          throw err;
        }
      }

      result = new SimpleMemoryAllocatorImpl(ooohml, stats, slabs);
      created = true;
      singleton = result;
      invokeAfterCreate(result);
    }
    } finally {
      if (!created) {
        stats.close();
        ooohml.close();
      }
    }
    return result;
  }
  // for unit tests
  public static SimpleMemoryAllocatorImpl create(OutOfOffHeapMemoryListener oooml, OffHeapMemoryStats stats, UnsafeMemoryChunk[] slabs) {
    SimpleMemoryAllocatorImpl result = new SimpleMemoryAllocatorImpl(oooml, stats, slabs);
    singleton = result;
    invokeAfterCreate(result);
    return result;
  }
  
  private void reuse(OutOfOffHeapMemoryListener oooml, LogWriter lw, OffHeapMemoryStats newStats, long offHeapMemorySize) {
    if (isClosed()) {
      throw new IllegalStateException("Can not reuse a closed off-heap memory manager.");
    }
    if (oooml == null) {
      throw new IllegalArgumentException("OutOfOffHeapMemoryListener is null");
    }
    if (getTotalMemory() != offHeapMemorySize) {
      lw.warning("Using " + getTotalMemory() + " bytes of existing off-heap memory instead of the requested " + offHeapMemorySize);
    }
    this.ooohml = oooml;
    newStats.initialize(this.stats);
    this.stats = newStats;
  }

  public static void cleanupPreviousAllocator() {
    Thread t = asyncCleanupThread.getAndSet(null);
    if (t != null) {
//      try {
//        // HACK to see if a delay fixes bug 47883
//        Thread.sleep(3000);
//      } catch (InterruptedException ignore) {
//      }
      t.interrupt();
      try {
        t.join(FREE_PAUSE_MILLIS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  private SimpleMemoryAllocatorImpl(final OutOfOffHeapMemoryListener oooml, final OffHeapMemoryStats stats, final UnsafeMemoryChunk[] slabs) {
    if (oooml == null) {
      throw new IllegalArgumentException("OutOfOffHeapMemoryListener is null");
    }
    if (TINY_MULTIPLE <= 0 || (TINY_MULTIPLE & 3) != 0) {
      throw new IllegalStateException("gemfire.OFF_HEAP_ALIGNMENT must be a multiple of 8.");
    }
    if (TINY_MULTIPLE > 256) {
      // this restriction exists because of the dataSize field in the object header.
      throw new IllegalStateException("gemfire.OFF_HEAP_ALIGNMENT must be <= 256 and a multiple of 8.");
    }
    if (BATCH_SIZE <= 0) {
      throw new IllegalStateException("gemfire.OFF_HEAP_BATCH_ALLOCATION_SIZE must be >= 1.");
    }
    if (TINY_FREE_LIST_COUNT <= 0) {
      throw new IllegalStateException("gemfire.OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
    }
    assert HUGE_MULTIPLE <= 256;
    
    this.ooohml = oooml;
    this.stats = stats;
    this.slabs = slabs;
    if(GemFireCacheImpl.sqlfSystem()) {
      throw new IllegalStateException("offheap sqlf not supported");
//       String provider = GemFireCacheImpl.SQLF_FACTORY_PROVIDER;
//       try {
//         Class<?> factoryProvider = Class.forName(provider);
//         Method method = factoryProvider.getDeclaredMethod("getChunkFactory");        
//         this.chunkFactory  = (ChunkFactory)method.invoke(null, (Object [])null);
//       }catch (Exception e) {
//         throw new IllegalStateException("Exception in obtaining ChunkFactory class",  e);
//       }

    }else {
      
      this.chunkFactory = new GemFireChunkFactory();
    }
    
    if (PRETOUCH) {
      final int tc;
      if (Runtime.getRuntime().availableProcessors() > 1) {
        tc = Runtime.getRuntime().availableProcessors() / 2;
      } else {
        tc = 1;
      }
      Thread[] threads = new Thread[tc];
      for (int i=0; i < tc; i++) {
        final int threadId = i;
        threads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            for (int slabId=threadId; slabId < slabs.length; slabId+=tc) {
              final int slabSize = slabs[slabId].getSize();
              for (int pageId=0; pageId < slabSize; pageId+=OFF_HEAP_PAGE_SIZE) {
                slabs[slabId].writeByte(pageId, (byte) 0);
              }
            }
          }
        });
        threads[i].start();
      }
      for (int i=0; i < tc; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    //OSProcess.printStacks(0, InternalDistributedSystem.getAnyInstance().getLogWriter(), false);
    this.stats.setFragments(slabs.length);
    largestSlab = slabs[0].getSize();
    this.stats.setLargestFragment(largestSlab);
    long total = 0;
    for (int i=0; i < slabs.length; i++) {
      //debugLog("slab"+i + " @" + Long.toHexString(slabs[i].getMemoryAddress()), false);
      //UnsafeMemoryChunk.clearAbsolute(slabs[i].getMemoryAddress(), slabs[i].getSize()); // HACK to see what this does to bug 47883
      total += slabs[i].getSize();
    }
    totalSlabSize = total;
    this.stats.incMaxMemory(this.totalSlabSize);
    this.stats.incFreeMemory(this.totalSlabSize);
    
    this.freeList = new FreeListManager();
  }
  
  public List<Chunk> getLostChunks() {
    List<Chunk> liveChunks = this.freeList.getLiveChunks();
    List<Chunk> regionChunks = getRegionLiveChunks();
    Set liveChunksSet = new HashSet(liveChunks);
    Set regionChunksSet = new HashSet(regionChunks);
    liveChunksSet.removeAll(regionChunksSet);
    return new ArrayList<Chunk>(liveChunksSet);
  }
  
  /**
   * Returns a possibly empty list that contains all the Chunks used by regions.
   */
  private List<Chunk> getRegionLiveChunks() {
    ArrayList<Chunk> result = new ArrayList<Chunk>();
    GemFireCacheImpl gfc = GemFireCacheImpl.getInstance();
    if (gfc != null) {
      Iterator rootIt = gfc.rootRegions().iterator();
      while (rootIt.hasNext()) {
        Region rr = (Region) rootIt.next();
        getRegionLiveChunks(rr, result);
        Iterator srIt = rr.subregions(true).iterator();
        while (srIt.hasNext()) {
          Region sr = (Region)srIt.next();
          getRegionLiveChunks(sr, result);
        }
      }
    }
    return result;
  }

  private void getRegionLiveChunks(Region r, List<Chunk> result) {
    if (r.getAttributes().getOffHeap()) {

      if (r instanceof PartitionedRegion) {
        PartitionedRegionDataStore prs = ((PartitionedRegion) r).getDataStore();
        if (prs != null) {
          Set<BucketRegion> brs = prs.getAllLocalBucketRegions();
          if (brs != null) {
            for (BucketRegion br : brs) {
              if (br != null && !br.isDestroyed()) {
                this.basicGetRegionLiveChunks(br, result);
              }

            }
          }
        }
      } else {
        this.basicGetRegionLiveChunks((LocalRegion) r, result);
      }

    }

  }
  
  private void basicGetRegionLiveChunks(LocalRegion r, List<Chunk> result) {
    for (Object key : r.keySet()) {
      RegionEntry re = ((LocalRegion) r).getRegionEntry(key);
      if (re != null) {
        /**
         * value could be GATEWAY_SENDER_EVENT_IMPL_VALUE or region entry value.
         */
        @Unretained(OffHeapIdentifier.GATEWAY_SENDER_EVENT_IMPL_VALUE)
        Object value = re._getValue();
        if (value instanceof Chunk) {
          result.add((Chunk) value);
        }
      }
    }
  }

  @Override
  public MemoryChunk allocate(int size, ChunkType chunkType) {
    //System.out.println("allocating " + size);
    Chunk result = this.freeList.allocate(size, chunkType);
    //("allocated off heap object of size " + size + " @" + Long.toHexString(result.getMemoryAddress()), true);
    if (trackReferenceCounts()) {
      refCountChanged(result.getMemoryAddress(), false, 1);
    }
    return result;
  }
  
  /**
   * Used to represent offheap addresses whose
   * value encodes actual data instead a memory
   * location.
   * Instances of this class have a very short lifetime.
   * 
   * @author darrel
   *
   */
  public static class DataAsAddress implements StoredObject {
    private final long address;
    
    public DataAsAddress(long addr) {
      this.address = addr;
    }
    
    public long getEncodedAddress() {
      return this.address;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof DataAsAddress) {
        return getEncodedAddress() == ((DataAsAddress) o).getEncodedAddress();
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      long value = getEncodedAddress();
      return (int)(value ^ (value >>> 32));
    }

    @Override
    public int getSizeInBytes() {
      return 0;
    }

    public byte[] getDecompressedBytes(RegionEntryContext r) {
      return OffHeapRegionEntryHelper.encodedAddressToBytes(this.address, true, r);
    }

    /**
     * If we contain a byte[] return it.
     * Otherwise return the serialize bytes in us in a byte array.
     */
    public byte[] getRawBytes() {
      return OffHeapRegionEntryHelper.encodedAddressToRawBytes(this.address);
    }
    
    @Override
    public byte[] getSerializedValue() {
      return OffHeapRegionEntryHelper.encodedAddressToBytes(this.address);
    }

    @Override
    public Object getDeserializedValue(Region r, RegionEntry re) {
      return OffHeapRegionEntryHelper.encodedAddressToObject(this.address);
    }

    @Override
    public Object getDeserializedForReading() {
      return getDeserializedValue(null,null);
    }
    
    @Override
    public Object getValueAsDeserializedHeapObject() {
      return getDeserializedValue(null,null);
    }
    
    @Override
    public byte[] getValueAsHeapByteArray() {
      if (isSerialized()) {
        return getSerializedValue();
      } else {
        return (byte[])getDeserializedForReading();
      }
    }

    @Override
    public String getStringForm() {
      try {
        return StringUtils.forceToString(getDeserializedForReading());
      } catch (RuntimeException ex) {
        return "Could not convert object to string because " + ex;
      }
    }

    @Override
    public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
      return getDeserializedValue(null,null);
    }

    @Override
    public Object getValue() {
      if (isSerialized()) {
        return getSerializedValue();
      } else {
        throw new IllegalStateException("Can not call getValue on StoredObject that is not serialized");
      }
    }

    @Override
    public void writeValueAsByteArray(DataOutput out) throws IOException {
      DataSerializer.writeByteArray(getSerializedValue(), out);
    }

    @Override
    public void fillSerializedValue(BytesAndBitsForCompactor wrapper,
        byte userBits) {
      byte[] value;
      if (isSerialized()) {
        value = getSerializedValue();
        userBits = EntryBits.setSerialized(userBits, true);
      } else {
        value = (byte[]) getDeserializedForReading();
      }
      wrapper.setData(value, userBits, value.length, true);
    }

    @Override
    public int getValueSizeInBytes() {
      return 0;
    }
    
    @Override
    public void sendTo(DataOutput out) throws IOException {
      if (isSerialized()) {
        out.write(getSerializedValue());
      } else {
        Object objToSend = (byte[]) getDeserializedForReading(); // deserialized as a byte[]
        DataSerializer.writeObject(objToSend, out);
      }
    }

    @Override
    public void sendAsByteArray(DataOutput out) throws IOException {
      byte[] bytes;
      if (isSerialized()) {
        bytes = getSerializedValue();
      } else {
        bytes = (byte[]) getDeserializedForReading();
      }
      DataSerializer.writeByteArray(bytes, out);
      
    }
    
    @Override
    public void sendAsCachedDeserializable(DataOutput out) throws IOException {
      if (!isSerialized()) {
        throw new IllegalStateException("sendAsCachedDeserializable can only be called on serialized StoredObjects");
      }
      InternalDataSerializer.writeDSFIDHeader(DataSerializableFixedID.VM_CACHED_DESERIALIZABLE, out);
      sendAsByteArray(out);
    }

    @Override
    public boolean isSerialized() {
      return OffHeapRegionEntryHelper.isSerialized(this.address);
    }

    @Override
    public boolean isCompressed() {
      return OffHeapRegionEntryHelper.isCompressed(this.address);
    }
    
    @Override
    public boolean retain() {
      // nothing needed
      return true;
    }
    @Override
    public void release() {
      // nothing needed
    }
  }

  @SuppressWarnings("unused")
  public static void debugLog(String msg, boolean logStack) {
    if (logStack) {
      logger.info(msg, new RuntimeException(msg));
    } else {
      logger.info(msg);
    }
  }
  
  @Override
  public StoredObject allocateAndInitialize(byte[] v, boolean isSerialized, boolean isCompressed, ChunkType chunkType) {
    long addr = OffHeapRegionEntryHelper.encodeDataAsAddress(v, isSerialized, isCompressed);
    if (addr != 0L) {
      return new DataAsAddress(addr);
    }
    if (chunkType == null) {
      chunkType = GemFireChunk.TYPE;
    }

    Chunk result = this.freeList.allocate(v.length, chunkType);
    //debugLog("allocated off heap object of size " + v.length + " @" + Long.toHexString(result.getMemoryAddress()), true);
    //debugLog("allocated off heap object of size " + v.length + " @" + Long.toHexString(result.getMemoryAddress()) +  "chunkSize=" + result.getSize() + " isSerialized=" + isSerialized + " v=" + Arrays.toString(v), true);
    if (trackReferenceCounts()) {
      refCountChanged(result.getMemoryAddress(), false, 1);
    }
    assert result.getChunkType() == chunkType: "chunkType=" + chunkType + " getChunkType()=" + result.getChunkType();
    result.setSerializedValue(v);
    result.setSerialized(isSerialized);
    result.setCompressed(isCompressed);
    return result;
  }
  
  @Override
  public long getFreeMemory() {
    return this.freeList.getFreeMemory();
  }

  @Override
  public long getUsedMemory() {
    return this.freeList.getUsedMemory();
  }

  @Override
  public long getTotalMemory() {
    return totalSlabSize;
  }
  
  @Override
  public void close() {
    try {
      invokeBeforeClose(this);
    } finally {
      this.ooohml.close();
      if (Boolean.getBoolean(FREE_OFF_HEAP_MEMORY_PROPERTY)) {
        realClose();
      }
    }
  }
  
  public static void freeOffHeapMemory() {
    SimpleMemoryAllocatorImpl ma = singleton;
    if (ma != null) {
      ma.realClose();
    }
  }
  
  private void realClose() {
    // Removing this memory immediately can lead to a SEGV. See 47885.
    if (setClosed()) {
      freeSlabsAsync(this.slabs);
      this.stats.close();
      singleton = null;
    }
  }
  
  private final AtomicBoolean closed = new AtomicBoolean();
  private boolean isClosed() {
    return this.closed.get();
  }
  /**
   * Returns true if caller is the one who should close; false if some other thread
   * is already closing.
   */
  private boolean setClosed() {
    return this.closed.compareAndSet(false, true);
  }
  

  private static final int FREE_PAUSE_MILLIS = Integer.getInteger("gemfire.OFF_HEAP_FREE_PAUSE_MILLIS", 90000);

  
  
  private static void freeSlabsAsync(final UnsafeMemoryChunk[] slabs) {
    //debugLog("called freeSlabsAsync", false);
    // since we no longer free off-heap memory on every cache close
    // and production code does not free it but instead reuses it
    // we should be able to free it sync.
    // If it turns out that it does need to be async then we need
    // to make sure we call cleanupPreviousAllocator.
    for (int i=0; i < slabs.length; i++) {
      slabs[i].release();
    }
//    Thread t = new Thread(new Runnable() {
//      @Override
//      public void run() {
//        // pause this many millis before freeing the slabs.
//        try {
//          Thread.sleep(FREE_PAUSE_MILLIS);
//        } catch (InterruptedException ignore) {
//          // If we are interrupted we should wakeup
//          // and free our slabs.
//        }
//        //debugLog("returning offheap memory to OS", false);
//        for (int i=0; i < slabs.length; i++) {
//          slabs[i].free();
//        }
//        //debugLog("returned offheap memory to OS", false);
//        asyncCleanupThread.compareAndSet(Thread.currentThread(), null);
//      }
//    }, "asyncSlabDeallocator");
//    t.setDaemon(true);
//    t.start();
//    asyncCleanupThread.set(t);    
  }
  
  private void freeChunk(long addr) {
    this.freeList.free(addr);
  }
  
  protected UnsafeMemoryChunk[] getSlabs() {
    return this.slabs;
  }
  
  /**
   * Return the slabId of the slab that contains the given addr.
   */
  protected int findSlab(long addr) {
    for (int i=0; i < this.slabs.length; i++) {
      UnsafeMemoryChunk slab = this.slabs[i];
      long slabAddr = slab.getMemoryAddress();
      if (addr >= slabAddr) {
        if (addr < slabAddr + slab.getSize()) {
          return i;
        }
      }
    }
    throw new IllegalStateException("could not find a slab for addr " + addr);
  }
  
  public OffHeapMemoryStats getStats() {
    return this.stats;
  }
  
  public ChunkFactory getChunkFactory() {
    return this.chunkFactory;
  }

  @Override
  public void addMemoryUsageListener(final MemoryUsageListener listener) {
    synchronized (this.memoryUsageListeners) {
      final MemoryUsageListener[] newMemoryUsageListeners = Arrays.copyOf(this.memoryUsageListeners, this.memoryUsageListeners.length + 1);
      newMemoryUsageListeners[this.memoryUsageListeners.length] = listener;
      this.memoryUsageListeners = newMemoryUsageListeners;
    }
  }
  
  @Override
  public void removeMemoryUsageListener(final MemoryUsageListener listener) {
    synchronized (this.memoryUsageListeners) {
      int listenerIndex = -1;
      for (int i = 0; i < this.memoryUsageListeners.length; i++) {
        if (this.memoryUsageListeners[i] == listener) {
          listenerIndex = i;
          break;
        }
      }

      if (listenerIndex != -1) {
        final MemoryUsageListener[] newMemoryUsageListeners = new MemoryUsageListener[this.memoryUsageListeners.length - 1];
        System.arraycopy(this.memoryUsageListeners, 0, newMemoryUsageListeners, 0, listenerIndex);
        System.arraycopy(this.memoryUsageListeners, listenerIndex + 1, newMemoryUsageListeners, listenerIndex,
            this.memoryUsageListeners.length - listenerIndex - 1);
        this.memoryUsageListeners = newMemoryUsageListeners;
      }
    }
  }
  
  private void notifyListeners() {
    final MemoryUsageListener[] savedListeners = this.memoryUsageListeners;
    
    if (savedListeners.length == 0) {
      return;
    }

    final long bytesUsed = getUsedMemory();
    for (int i = 0; i < savedListeners.length; i++) {
      savedListeners[i].updateMemoryUsed(bytesUsed);
    }
  }
  
  public class FreeListManager {
    private final AtomicReferenceArray<SyncChunkStack> tinyFreeLists = new AtomicReferenceArray<SyncChunkStack>(TINY_FREE_LIST_COUNT);
    // Deadcoding the BIG stuff. Idea is to have a bigger TINY list by default
//    /**
//     * Every allocated chunk smaller than BIG_MULTIPLE*BIG_FREE_LIST_COUNT but that is not tiny will allocate a chunk of memory that is a multiple of this value.
//     * Sizes are always rounded up to the next multiple of this constant
//     * so internal fragmentation will be limited to BIG_MULTIPLE-1 bytes per allocation
//     * and on average will be BIG_MULTIPLE/2 given a random distribution of size requests.
//     */
//    public final static int BIG_MULTIPLE = TINY_MULTIPLE*8;
//    /**
//     * Number of free lists to keep for big allocations.
//     */
//    private final static int BIG_FREE_LIST_COUNT = 2048;
//    private final static int BIG_OFFSET = (MAX_TINY/BIG_MULTIPLE*BIG_MULTIPLE);
//    public final static int MAX_BIG = (BIG_MULTIPLE*BIG_FREE_LIST_COUNT) + BIG_OFFSET;
//    private final AtomicReferenceArray<ConcurrentChunkStack> bigFreeLists = new AtomicReferenceArray<ConcurrentChunkStack>(BIG_FREE_LIST_COUNT);
    // hugeChunkSet is sorted by chunk size in ascending order. It will only contain chunks larger than MAX_TINY.
    private final ConcurrentSkipListSet<Chunk> hugeChunkSet = new ConcurrentSkipListSet<Chunk>();
    private final AtomicLong allocatedSize = new AtomicLong(0L);
   
    private int getNearestTinyMultiple(int size) {
      return (size-1)/TINY_MULTIPLE;
    }
    public List<Chunk> getLiveChunks() {
      ArrayList<Chunk> result = new ArrayList<Chunk>();
      UnsafeMemoryChunk[] slabs = getSlabs();
      for (int i=0; i < slabs.length; i++) {
        getLiveChunks(slabs[i], result);
      }
      return result;
    }
    private void getLiveChunks(UnsafeMemoryChunk slab, List<Chunk> result) {
      long addr = slab.getMemoryAddress();
      while (addr <= (slab.getMemoryAddress() + slab.getSize() - Chunk.MIN_CHUNK_SIZE)) {
        Fragment f = isAddrInFragmentFreeSpace(addr);
        if (f != null) {
          addr = f.getMemoryAddress() + f.getSize();
        } else {
          int curChunkSize = Chunk.getSize(addr);
          int refCount = Chunk.getRefCount(addr);
          if (refCount > 0) {
            result.add(SimpleMemoryAllocatorImpl.this.chunkFactory.newChunk(addr));
          }
          addr += curChunkSize;
        }
      }
    }
    /**
     * If addr is in the free space of a fragment then return that fragment; otherwise return null.
     */
    private Fragment isAddrInFragmentFreeSpace(long addr) {
      for (Fragment f: this.fragmentList) {
        if (addr >= (f.getMemoryAddress() + f.getFreeIndex()) && addr < (f.getMemoryAddress() + f.getSize())) {
          return f;
        }
      }
      return null;
    }
    public long getUsedMemory() {
      return this.allocatedSize.get();
    }
    public long getFreeMemory() {
      return getTotalMemory() - getUsedMemory();
//      long result = getFreeFragmentMemory();
//      result += getFreeTinyMemory();
//      result += getFreeHugeMemory();
//      return result;
    }
    public long getFreeFragmentMemory() {
      long result = 0;
      for (Fragment f: this.fragmentList) {
        int freeSpace = f.freeSpace();
        if (freeSpace >= Chunk.MIN_CHUNK_SIZE) {
          result += freeSpace;
        }
      }
      return result;
    }
    public long getFreeTinyMemory() {
      long tinyFree = 0;
      for (int i=0; i < this.tinyFreeLists.length(); i++) {
        SyncChunkStack cl = this.tinyFreeLists.get(i);
        if (cl != null) {
          tinyFree += cl.computeTotalSize();
        }
      }
      return tinyFree;
    }
//    public long getFreeBigMemory() {
//      long bigFree = 0;
//      for (int i=0; i < this.bigFreeLists.length(); i++) {
//        ConcurrentChunkStack cl = this.bigFreeLists.get(i);
//        if (cl != null) {
//          bigFree += cl.computeTotalSize();
//        }
//      }
//      return bigFree;
//    }
    public long getFreeHugeMemory() {
      long hugeFree = 0;
      for (Chunk c: this.hugeChunkSet) {
        hugeFree += c.getSize();
      }
      return hugeFree;
    }
//    private int getNearestBigMultiple(int size) {
//      return (size-1-BIG_OFFSET)/BIG_MULTIPLE;
//    }

    /**
     * Each long in this array tells us how much of the corresponding slab is allocated.
     */
    //private final AtomicIntegerArray slabOffsets = new AtomicIntegerArray(getSlabs().length);
    /**
     * The slab id of the last slab we allocated from.
     */
    private final AtomicInteger lastFragmentAllocation = new AtomicInteger(0);

    private final CopyOnWriteArrayList<Fragment> fragmentList;
    public FreeListManager() {
      UnsafeMemoryChunk[] slabs = getSlabs();
      Fragment[] tmp = new Fragment[slabs.length];
      for (int i=0; i < slabs.length; i++) {
        tmp[i] = new Fragment(slabs[i].getMemoryAddress(), slabs[i].getSize());
      }
      this.fragmentList = new CopyOnWriteArrayList<Fragment>(tmp);
      
      if(validateMemoryWithFill) {
        fillFragments();
      }
    }
    
    /**
     * Fills all fragments with a fill used for data integrity validation.
     */
    private void fillFragments() {
      for(Fragment fragment : this.fragmentList) {
        fragment.fill();
      }
    }
    
    /**
     * This is a bit of a hack. TODO add some timeout logic in case this thread never does another off heap allocation.
     */
//    private final ThreadLocal<Chunk> tlCache = new ThreadLocal<Chunk>();
    
    /**
     * Allocate a chunk of memory of at least the given size.
     * The basic algorithm is:
     * 1. Look for a previously allocated and freed chunk close to the size requested.
     * 2. See if the original chunk is big enough to split. If so do so.
     * 3. Look for a previously allocated and freed chunk of any size larger than the one requested.
     *    If we find one split it.
     * <p>
     * It might be better not to include step 3 since we expect and freed chunk to be reallocated in the future.
     * Maybe it would be better for 3 to look for adjacent free blocks that can be merged together.
     * For now we will just try 1 and 2 and then report out of mem.
     * @param size minimum bytes the returned chunk must have.
     * @param chunkType TODO
     * @return the allocated chunk
     * @throws IllegalStateException if a chunk can not be allocated.
     */
    @SuppressWarnings("synthetic-access")
    public Chunk allocate(int size, ChunkType chunkType) {
      Chunk result = null; /*tlCache.get();
      
      if (result != null && result.getDataSize() == size) {
        tlCache.set(null);
      } else */{
        assert size > 0;
        if (chunkType == null) {
          chunkType = GemFireChunk.TYPE;
        }
        result = basicAllocate(size, true, chunkType);
        result.setDataSize(size);
      }
      stats.incObjects(1);
      int resultSize = result.getSize();
      this.allocatedSize.addAndGet(resultSize);
      stats.incUsedMemory(resultSize);
      stats.incFreeMemory(-resultSize);
      result.initializeUseCount();
      notifyListeners();
      
      return result;
    }
    
    private Chunk basicAllocate(int size, boolean useSlabs, ChunkType chunkType) {
      if (useSlabs) {
        // Every object stored off heap has a header so we need
        // to adjust the size so that the header gets allocated.
        // If useSlabs is false then the incoming size has already
        // been adjusted.
        size += Chunk.OFF_HEAP_HEADER_SIZE;
      }
      if (size <= MAX_TINY) {
        return allocateTiny(size, useSlabs, chunkType);
//      } else if (size <= MAX_BIG) {
//        return allocateBig(size, useSlabs);
      } else {
        return allocateHuge(size, useSlabs, chunkType);
      }
    }
    
    private Chunk allocateFromFragments(int chunkSize, ChunkType chunkType) {
      do {
        final int lastAllocationId = this.lastFragmentAllocation.get();
        for (int i=lastAllocationId; i < this.fragmentList.size(); i++) {
          Chunk result = allocateFromFragment(i, chunkSize, chunkType);
          if (result != null) {
            return result;
          }
        }
        for (int i=0; i < lastAllocationId; i++) {
          Chunk result = allocateFromFragment(i, chunkSize, chunkType);
          if (result != null) {
            return result;
          }
        }
      } while (compact(chunkSize));
      // We tried all the fragments and didn't find any free memory.
      logOffHeapState(chunkSize);
      final OutOfOffHeapMemoryException failure = new OutOfOffHeapMemoryException("Out of off-heap memory. Could not allocate size of " + chunkSize);
      try {
        throw failure;
      } finally {
        SimpleMemoryAllocatorImpl.this.ooohml.outOfOffHeapMemory(failure);
      }
    }
    
    private void logOffHeapState(int chunkSize) {
      if (InternalDistributedSystem.getAnyInstance() != null) {
        LogWriter lw = InternalDistributedSystem.getAnyInstance().getLogWriter();
        lw.info("OutOfOffHeapMemory allocating size of " + chunkSize + ". allocated=" + this.allocatedSize.get() + " compactions=" + this.compactCount.get() + " objects=" + stats.getObjects() + " free=" + stats.getFreeMemory() + " fragments=" + stats.getFragments() + " largestFragment=" + stats.getLargestFragment() + " fragmentation=" + stats.getFragmentation());
        logFragmentState(lw);
        logTinyState(lw);
//        logBigState(lw);
        logHugeState(lw);
      }
    }

    private void logHugeState(LogWriter lw) {
      for (Chunk c: this.hugeChunkSet) {
        lw.info("Free huge of size " + c.getSize());
      }
    }
//    private void logBigState(LogWriter lw) {
//      for (int i=0; i < this.bigFreeLists.length(); i++) {
//        ConcurrentChunkStack cl = this.bigFreeLists.get(i);
//        if (cl != null) {
//          cl.logSizes(lw, "Free big of size ");
//        }
//      }
//    }
    private void logTinyState(LogWriter lw) {
      for (int i=0; i < this.tinyFreeLists.length(); i++) {
        SyncChunkStack cl = this.tinyFreeLists.get(i);
        if (cl != null) {
          cl.logSizes(lw, "Free tiny of size ");
        }
      }
    }
    private void logFragmentState(LogWriter lw) {
      for (Fragment f: this.fragmentList) {
        int freeSpace = f.freeSpace();
        if (freeSpace > 0) {
          lw.info("Fragment at " + f.getMemoryAddress() + " of size " + f.getSize() + " has " + freeSpace + " bytes free.");
        }
      }
    }

    private final AtomicInteger compactCount = new AtomicInteger();
    /**
     * Compacts memory and returns true if enough memory to allocate chunkSize
     * is freed. Otherwise returns false;
     * TODO OFFHEAP: what should be done about contiguous chunks that end up being bigger than 2G?
     * Currently if we are given slabs bigger than 2G or that just happen to be contiguous and add
     * up to 2G then the compactor may unify them together into a single Chunk and our 32-bit chunkSize
     * field will overflow. This code needs to detect this and just create a chunk of 2G and then start
     * a new one.
     * Or to prevent it from happening we could just check the incoming slabs and throw away a few bytes
     * to keep them from being contiguous.
     */
    private boolean compact(int chunkSize) {
      final long startCompactionTime = getStats().startCompaction();
      final int countPreSync = this.compactCount.get();
      try {
        synchronized (this) {
          if (this.compactCount.get() != countPreSync) {
            // someone else did a compaction while we waited on the sync.
            // So just return true causing the caller to retry the allocation.
            return true;
          }
          ArrayList<SyncChunkStack> freeChunks = new ArrayList<SyncChunkStack>();
          collectFreeChunks(freeChunks);
          final int SORT_ARRAY_BLOCK_SIZE = 128;
          long[] sorted = new long[SORT_ARRAY_BLOCK_SIZE];
          int sortedSize = 0;
          boolean result = false;
          int largestFragment = 0;
          for (SyncChunkStack l: freeChunks) {
            long addr = l.poll();
            while (addr != 0) {
              int idx = Arrays.binarySearch(sorted, 0, sortedSize, addr);
              //System.out.println("DEBUG addr=" + addr + " size=" + Chunk.getSize(addr) + " idx="+idx + " sortedSize=" + sortedSize);
              if (idx >= 0) {
                throw new IllegalStateException("duplicate memory address found during compaction!");
              }
              idx = -idx;
              idx--;
              if (idx == sortedSize) {
                // addr is > everything in the array
                if (sortedSize == 0) {
                  // nothing was in the array
                  sorted[0] = addr;
                  sortedSize++;
                } else {
                  // see if we can conflate into sorted[idx]
                  long lowAddr = sorted[idx-1];
                  int lowSize = Chunk.getSize(lowAddr);
                  if (lowAddr + lowSize == addr) {
                    // append the addr chunk to lowAddr
                    Chunk.setSize(lowAddr, lowSize + Chunk.getSize(addr));
                  } else {
                    if (sortedSize >= sorted.length) {
                      long[] newSorted = new long[sorted.length+SORT_ARRAY_BLOCK_SIZE];
                      System.arraycopy(sorted, 0, newSorted, 0, sorted.length);
                      sorted = newSorted;
                    }
                    sortedSize++;
                    sorted[idx] = addr;
                  }
                }
              } else {
                int addrSize = Chunk.getSize(addr);
                long highAddr = sorted[idx];
                if (addr + addrSize == highAddr) {
                  // append highAddr chunk to addr
                  Chunk.setSize(addr, addrSize + Chunk.getSize(highAddr));
                  sorted[idx] = addr;
                } else {
                  boolean insert = idx==0;
                  if (!insert) {
                    long lowAddr = sorted[idx-1];
  //                  if (lowAddr == 0L) {
  //                    long[] tmp = Arrays.copyOf(sorted, sortedSize);
  //                    throw new IllegalStateException("addr was zero at idx=" + (idx-1) + " sorted="+ Arrays.toString(tmp));
  //                  }
                    int lowSize = Chunk.getSize(lowAddr);
                    if (lowAddr + lowSize == addr) {
                      // append the addr chunk to lowAddr
                      Chunk.setSize(lowAddr, lowSize + addrSize);
                    } else {
                      insert = true;
                    }
                  }
                  if (insert) {
                    if (sortedSize >= sorted.length) {
                      long[] newSorted = new long[sorted.length+SORT_ARRAY_BLOCK_SIZE];
                      System.arraycopy(sorted, 0, newSorted, 0, idx);
                      newSorted[idx] = addr;
                      System.arraycopy(sorted, idx, newSorted, idx+1, sortedSize-idx);
                      sorted = newSorted;
                    } else {
                      System.arraycopy(sorted, idx, sorted, idx+1, sortedSize-idx);
                      sorted[idx] = addr;
                    }
                    sortedSize++;
                  }
                }
              }
              addr = l.poll();
            }
          }
          for (int i=sortedSize-1; i > 0; i--) {
            long addr = sorted[i];
            long lowAddr = sorted[i-1];
            int lowSize = Chunk.getSize(lowAddr);
            if (lowAddr + lowSize == addr) {
              // append addr chunk to lowAddr
              Chunk.setSize(lowAddr, lowSize + Chunk.getSize(addr));
              sorted[i] = 0L;
            }
          }
          this.lastFragmentAllocation.set(0);
          ArrayList<Fragment> tmp = new ArrayList<Fragment>();
          for (int i=sortedSize-1; i >= 0; i--) {
            long addr = sorted[i];
            if (addr == 0L) continue;
            int addrSize = Chunk.getSize(addr);
            Fragment f = new Fragment(addr, addrSize);
            if (addrSize >= chunkSize) {
              result = true;
            }
            if (addrSize > largestFragment) {
              largestFragment = addrSize;
              // TODO it might be better to sort them biggest first
              tmp.add(0, f);
            } else {
              tmp.add(f);
            }
          }
          this.fragmentList.addAll(tmp);
          
          // Reinitialize fragments with fill pattern data
          if(validateMemoryWithFill) {
            fillFragments();
          }
          
          // Signal any waiters that a compaction happened.
          this.compactCount.incrementAndGet();
          
          getStats().setLargestFragment(largestFragment);
          getStats().setFragments(tmp.size());        
          updateFragmentation();
          
          return result;
        } // sync
      } finally {
        getStats().endCompaction(startCompactionTime);
      }
    }
    
    private void updateFragmentation() {      
      long freeSize = getStats().getFreeMemory();

      // Calculate free space fragmentation only if there is free space available.
      if(freeSize > 0) {
        long largestFragment = getStats().getLargestFragment();
        long numerator = freeSize - largestFragment;
        
        double percentage = (double) numerator / (double) freeSize;
        percentage *= 100d;
        
        int wholePercentage = (int) Math.rint(percentage);
        getStats().setFragmentation(wholePercentage);
      } else {
        // No free space? Then we have no free space fragmentation.
        getStats().setFragmentation(0);
      }
    }
    
    private void collectFreeChunks(List<SyncChunkStack> l) {
      collectFreeFragmentChunks(l);
      collectFreeHugeChunks(l);
//      collectFreeBigChunks(l);
      collectFreeTinyChunks(l);
    }
    private void collectFreeFragmentChunks(List<SyncChunkStack> l) {
      if (this.fragmentList.size() == 0) return;
      SyncChunkStack result = new SyncChunkStack();
      for (Fragment f: this.fragmentList) {
        int offset;
        int diff;
        do {
          offset = f.getFreeIndex();
          diff = f.getSize() - offset;
        } while (diff >= Chunk.MIN_CHUNK_SIZE && !f.allocate(offset, offset+diff));
        if (diff < Chunk.MIN_CHUNK_SIZE) {
          if (diff > 0) {
            logger.debug("Lost memory of size {}", diff);
          }
          // fragment is too small to turn into a chunk
          // TODO we need to make sure this never happens
          // by keeping sizes rounded. I think I did this
          // by introducing MIN_CHUNK_SIZE and by rounding
          // the size of huge allocations.
          continue;
        }
        long chunkAddr = f.getMemoryAddress()+offset;
        Chunk.setSize(chunkAddr, diff);
        result.offer(chunkAddr);
      }
      // All the fragments have been turned in to chunks so now clear them
      // The compaction will create new fragments.
      this.fragmentList.clear();
      if (!result.isEmpty()) {
        l.add(result);
      }
    }
    private void collectFreeTinyChunks(List<SyncChunkStack> l) {
      for (int i=0; i < this.tinyFreeLists.length(); i++) {
        SyncChunkStack cl = this.tinyFreeLists.get(i);
        if (cl != null) {
          long head = cl.clear();
          if (head != 0L) {
            l.add(new SyncChunkStack(head));
          }
        }
      }
    }
//    private void collectFreeBigChunks(List<ConcurrentChunkStack> l) {
//      for (int i=0; i < this.bigFreeLists.length(); i++) {
//        ConcurrentChunkStack cl = this.bigFreeLists.get(i);
//        if (cl != null) {
//          long head = cl.clear();
//          if (head != 0L) {
//            l.add(new ConcurrentChunkStack(head));
//          }
//        }
//      }
//    }
    public void collectFreeHugeChunks(List<SyncChunkStack> l) {
      Chunk c = this.hugeChunkSet.pollFirst();
      SyncChunkStack result = null;
      while (c != null) {
        if (result == null) {
          result = new SyncChunkStack();
          l.add(result);
        }
        result.offer(c.getMemoryAddress());
        c = this.hugeChunkSet.pollFirst();
      }
    }
    
    private Chunk allocateFromFragment(final int fragIdx, final int chunkSize, ChunkType chunkType) {
      if (fragIdx >= this.fragmentList.size()) return null;
      final Fragment fragment;
      try {
        fragment = this.fragmentList.get(fragIdx);
      } catch (IndexOutOfBoundsException ignore) {
        // A concurrent compaction can cause this.
        return null;
      }
      boolean retryFragment;
      do {
        retryFragment = false;
        int oldOffset = fragment.getFreeIndex();
        int fragmentSize = fragment.getSize();
        int fragmentFreeSize = fragmentSize - oldOffset;
        if (fragmentFreeSize >= chunkSize) {
          // this fragment has room
          // Try to allocate up to BATCH_SIZE more chunks from it
          int allocSize = chunkSize * BATCH_SIZE;
          if (allocSize > fragmentFreeSize) {
            allocSize = (fragmentFreeSize / chunkSize) * chunkSize;
          }
          int newOffset = oldOffset + allocSize;
          int extraSize = fragmentSize - newOffset;
          if (extraSize < Chunk.MIN_CHUNK_SIZE) {
            // include these last few bytes of the fragment in the allocation.
            // If we don't then they will be lost forever.
            // The extraSize bytes only apply to the first chunk we allocate (not the batch ones).
            newOffset += extraSize;
          } else {
            extraSize = 0;
          }
          if (fragment.allocate(oldOffset, newOffset)) {
            // We did the allocate!
            this.lastFragmentAllocation.set(fragIdx);
            Chunk result = chunkFactory.newChunk(fragment.getMemoryAddress()+oldOffset, chunkSize+extraSize, chunkType);
            allocSize -= chunkSize+extraSize;
            oldOffset += extraSize;
            while (allocSize > 0) {
              oldOffset += chunkSize;
              // we add the batch ones immediately to the freelist
              result.readyForFree();
              free(result.getMemoryAddress(), false);
              result = chunkFactory.newChunk(fragment.getMemoryAddress()+oldOffset, chunkSize, chunkType);
              allocSize -= chunkSize;
            }
            
            if(validateMemoryWithFill) {
              result.validateFill();
            }
            
            return result;
          } else {
            // TODO OFFHEAP: if batch allocations are disabled should we not call basicAllocate here?
            // Since we know another thread did a concurrent alloc
            // that possibly did a batch check the free list again.
            Chunk result = basicAllocate(chunkSize, false, chunkType);
            if (result != null) {
              return result;
            }
            retryFragment = true;
          }
        }
      } while (retryFragment);
      return null; // did not find enough free space in this fragment
    }

    private int round(int multiple, int value) {
      return (int) ((((long)value + (multiple-1)) / multiple) * multiple);
    }
    private Chunk allocateTiny(int size, boolean useFragments, ChunkType chunkType) {
      return basicAllocate(getNearestTinyMultiple(size), TINY_MULTIPLE, 0, this.tinyFreeLists, useFragments, chunkType);
    }
//    private Chunk allocateBig(int size, boolean useFragments) {
//      return basicAllocate(getNearestBigMultiple(size), BIG_MULTIPLE, BIG_OFFSET, this.bigFreeLists, useFragments);
//    }
    private Chunk basicAllocate(int idx, int multiple, int offset, AtomicReferenceArray<SyncChunkStack> freeLists, boolean useFragments, ChunkType chunkType) {
      SyncChunkStack clq = freeLists.get(idx);
      if (clq != null) {
        long memAddr = clq.poll();
        if (memAddr != 0) {
          Chunk result = SimpleMemoryAllocatorImpl.this.chunkFactory.newChunk(memAddr, chunkType);
          
          // Data integrity check.
          if(validateMemoryWithFill) {          
            result.validateFill();
          }
          
          result.readyForAllocation(chunkType);
          return result;
        }
      }
      if (useFragments) {
        return allocateFromFragments(((idx+1)*multiple)+offset, chunkType);
      } else {
        return null;
      }
    }
    private Chunk allocateHuge(int size, boolean useFragments, ChunkType chunkType) {
      // sizeHolder is a fake Chunk used to search our sorted hugeChunkSet.
      Chunk sizeHolder = newFakeChunk(size);
      NavigableSet<Chunk> ts = this.hugeChunkSet.tailSet(sizeHolder);
      Chunk result = ts.pollFirst();
      if (result != null) {
        if (result.getSize() - (HUGE_MULTIPLE - Chunk.OFF_HEAP_HEADER_SIZE) < size) {
          // close enough to the requested size; just return it.
          
          // Data integrity check.
          if(validateMemoryWithFill) {          
            result.validateFill();
          }
          if (chunkType.getSrcType() != Chunk.getSrcType(result.getMemoryAddress())) {
            // The java wrapper class that was cached in the huge chunk list is the wrong type.
            // So allocate a new one and garbage collect the old one.
            result = SimpleMemoryAllocatorImpl.this.chunkFactory.newChunk(result.getMemoryAddress(), chunkType);
          }
          result.readyForAllocation(chunkType);
          return result;
        } else {
          this.hugeChunkSet.add(result);
        }
      }
      if (useFragments) {
        // We round it up to the next multiple of TINY_MULTIPLE to make
        // sure we always have chunks allocated on an 8 byte boundary.
        return allocateFromFragments(round(TINY_MULTIPLE, size), chunkType);
      } else {
        return null;
      }
    }
    
    @SuppressWarnings("synthetic-access")
    public void free(long addr) {
      free(addr, true);
    }
    
    private void free(long addr, boolean updateStats) {
      int cSize = Chunk.getSize(addr);
      if (updateStats) {
        stats.incObjects(-1);
        this.allocatedSize.addAndGet(-cSize);
        stats.incUsedMemory(-cSize);
        stats.incFreeMemory(cSize);
        notifyListeners();
      }
      /*Chunk oldTlChunk = this.tlCache.get();
      this.tlCache.set(c);
      if (oldTlChunk != null) {
        int oldTlcSize = oldTlChunk.getSize();
        if (oldTlcSize <= MAX_TINY) {
          freeTiny(oldTlChunk);
        } else if (oldTlcSize <= MAX_BIG) {
          freeBig(oldTlChunk);
        } else {
          freeHuge(oldTlChunk);
        }
      }*/
      if (cSize <= MAX_TINY) {
        freeTiny(addr, cSize);
//      } else if (cSize <= MAX_BIG) {
//        freeBig(addr, cSize);
      } else {
        freeHuge(addr, cSize);
      }
    }
    private void freeTiny(long addr, int cSize) {
      basicFree(addr, getNearestTinyMultiple(cSize), this.tinyFreeLists);
    }
//    private void freeBig(long addr, int cSize) {
//      basicFree(addr, getNearestBigMultiple(cSize), this.bigFreeLists);
//    }
    private void basicFree(long addr, int idx, AtomicReferenceArray<SyncChunkStack> freeLists) {
      SyncChunkStack clq = freeLists.get(idx);
      if (clq != null) {
        clq.offer(addr);
      } else {
        clq = new SyncChunkStack();
        clq.offer(addr);
        if (!freeLists.compareAndSet(idx, null, clq)) {
          clq = freeLists.get(idx);
          clq.offer(addr);
        }
      }
      
    }
    private void freeHuge(long addr, int cSize) {
      this.hugeChunkSet.add(SimpleMemoryAllocatorImpl.this.chunkFactory.newChunk(addr)); // TODO make this a collection of longs
    }
  }
  
  
  
  
  /*private Chunk newChunk(long addr, int chunkSize) {
    return this.chunkFactory.newChunk(addr, chunkSize);
  }*/
  
  private Chunk newFakeChunk(int chunkSize) {
    return new FakeChunk(chunkSize);
  }
  
  
  public static interface ChunkFactory  {
    /**
     * Create a new chunk of the given size and type at the given address.
     */
    Chunk newChunk(long address, int chunkSize, ChunkType chunkType);
    /**
     * Create a new chunk for a block of memory (identified by address)
     * that has already been allocated.
     * The size and type are derived from the existing object header.
     */
    Chunk newChunk(long address);
    /**
     * Create a new chunk of the given type for a block of memory (identified by address)
     * that has already been allocated.
     * The size is derived from the existing object header.
     */
    Chunk newChunk(long address, ChunkType chunkType);
    /**
     * Given the address of an existing chunk return its ChunkType.
     */
    ChunkType getChunkTypeForAddress(long address);
    /**
     * Given the rawBits from the object header of an existing chunk
     * return its ChunkType.
     */
    ChunkType getChunkTypeForRawBits(int bits);
  }
  
  private static class GemFireChunkFactory implements ChunkFactory {
    @Override
    public Chunk newChunk(long address, int chunkSize, ChunkType chunkType) {
      assert chunkType.equals(GemFireChunk.TYPE);
      return new GemFireChunk(address,chunkSize);
    }

    @Override
    public Chunk newChunk(long address) {
      return new GemFireChunk(address);
    }

    @Override
    public Chunk newChunk(long address, ChunkType chunkType) {
      assert chunkType.equals(GemFireChunk.TYPE);
      return new GemFireChunk(address);
    }

    @Override
    public ChunkType getChunkTypeForAddress(long address) {
      assert Chunk.getSrcType(address) == Chunk.SRC_TYPE_GFE;
      return GemFireChunk.TYPE;
    }

    @Override
    public ChunkType getChunkTypeForRawBits(int bits) {
      assert Chunk.getSrcTypeFromRawBits(bits) == Chunk.SRC_TYPE_GFE;
      return GemFireChunk.TYPE;
    }
  }
  
  
  /**
   * Used to keep the heapForm around while an operation is still in progress.
   * This allows the operation to access the serialized heap form instead of copying
   * it from offheap. See bug 48135.
   * 
   * @author darrel
   *
   */
  public static class ChunkWithHeapForm extends GemFireChunk {
    private final byte[] heapForm;
    
    public ChunkWithHeapForm(GemFireChunk chunk, byte[] heapForm) {
      super(chunk);
      this.heapForm = heapForm;
    }

    @Override
    protected byte[] getRawBytes() {
      return this.heapForm;
    }
    
    public Chunk getChunkWithoutHeapForm() {
      return new GemFireChunk(this);
    }
  }
  
  public static abstract class ChunkType {
    public abstract int getSrcType();
    public abstract Chunk newChunk(long memoryAddress);
    public abstract Chunk newChunk(long memoryAddress, int chunkSize);
  }
  
  public static class GemFireChunkType extends ChunkType {
    private static final GemFireChunkType singleton = new GemFireChunkType();
    public static GemFireChunkType singleton() { return singleton; }
    
    private GemFireChunkType() {}

    @Override
    public int getSrcType() {
      return Chunk.SRC_TYPE_GFE;
    }

    @Override
    public Chunk newChunk(long memoryAddress) {      
      return new GemFireChunk(memoryAddress);
    }

    @Override
    public Chunk newChunk(long memoryAddress, int chunkSize) {     
      return new GemFireChunk(memoryAddress, chunkSize);
    }
  }
  public static class GemFireChunk extends Chunk {
    public static final ChunkType TYPE = new ChunkType() {
      @Override
      public int getSrcType() {
        return Chunk.SRC_TYPE_GFE;
      }
      @Override
      public Chunk newChunk(long memoryAddress) {
        return new GemFireChunk(memoryAddress);
      }
      @Override
      public Chunk newChunk(long memoryAddress, int chunkSize) {
        return new GemFireChunk(memoryAddress, chunkSize);
      }
    };
    public GemFireChunk(long memoryAddress, int chunkSize) {
      super(memoryAddress, chunkSize, TYPE);
    }

    public GemFireChunk(long memoryAddress) {
      super(memoryAddress);
      // chunkType may be set by caller when it calls readyForAllocation
    }
    public GemFireChunk(GemFireChunk chunk) {
      super(chunk);
    }
    @Override
    public Chunk slice(int position, int limit) {
      return new GemFireChunkSlice(this, position, limit);
    }
  }
  public static class GemFireChunkSlice extends GemFireChunk {
    private final int offset;
    private final int capacity;
    public GemFireChunkSlice(GemFireChunk gemFireChunk, int position, int limit) {
      super(gemFireChunk);
      this.offset = gemFireChunk.getBaseDataOffset() + position;
      this.capacity = limit - position;
    }
    @Override
    public int getDataSize() {
      return this.capacity;
    }
    
    @Override
    protected long getBaseDataAddress() {
      return super.getBaseDataAddress() + this.offset;
    }
    @Override
    protected int getBaseDataOffset() {
      return this.offset;
    }
  }
  /**
   * Note: this class has a natural ordering that is inconsistent with equals.
   * Instances of this class should have a short lifetime. We do not store references
   * to it in the cache. Instead the memoryAddress is stored in a primitive field in
   * the cache and if used it will then, if needed, create an instance of this class.
   */
  public static abstract class Chunk extends OffHeapCachedDeserializable implements Comparable<Chunk>, ConcurrentBag.Node, MemoryBlock {
    /**
     * The unsafe memory address of the first byte of this chunk
     */
    private final long memoryAddress;
    
    /**
     * The useCount, chunkSize, dataSizeDelta, isSerialized, and isCompressed
     * are all stored in off heap memory in a HEADER. This saves heap memory
     * by using off heap.
     */
    public final static int OFF_HEAP_HEADER_SIZE = 4 + 4;
    /**
     * We need to smallest chunk to at least have enough room for a hdr
     * and for an off heap ref (which is a long).
     */
    public final static int MIN_CHUNK_SIZE = OFF_HEAP_HEADER_SIZE + 8;
    /**
     * int field.
     * The number of bytes in this chunk.
     */
    private final static int CHUNK_SIZE_OFFSET = 0;
    /**
     * Volatile int field
     * The upper two bits are used for the isSerialized
     * and isCompressed flags.
     * The next three bits are used to encode the SRC_TYPE enum.
     * The lower 3 bits of the most significant byte contains a magic number to help us detect
     * if we are changing the ref count of an object that has been released.
     * The next byte contains the dataSizeDelta.
     * The number of bytes of logical data in this chunk.
     * Since the number of bytes of logical data is always <= chunkSize
     * and since chunkSize never changes, we have dataSize be
     * a delta whose max value would be HUGE_MULTIPLE-1.
     * The lower two bytes contains the use count.
     */
    private final static int REF_COUNT_OFFSET = 4;
    /**
     * The upper two bits are used for the isSerialized
     * and isCompressed flags.
     */
    private final static int IS_SERIALIZED_BIT =    0x80000000;
    private final static int IS_COMPRESSED_BIT =    0x40000000;
    private final static int SRC_TYPE_MASK = 0x38000000;
    private final static int SRC_TYPE_SHIFT = 16/*refCount*/+8/*dataSize*/+3/*magicSize*/;
    private final static int MAGIC_MASK = 0x07000000;
    private final static int MAGIC_NUMBER = 0x05000000;
    private final static int DATA_SIZE_DELTA_MASK = 0x00ff0000;
    private final static int DATA_SIZE_SHIFT = 16;
    private final static int REF_COUNT_MASK =       0x0000ffff;
    private final static int MAX_REF_COUNT = 0xFFFF;
    final static long FILL_PATTERN = 0x3c3c3c3c3c3c3c3cL;
    final static byte FILL_BYTE = 0x3c;
    
    public final static int SRC_TYPE_NO_LOB_NO_DELTA = 0 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_WITH_LOBS = 1 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_WITH_SINGLE_DELTA = 2 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_WITH_MULTIPLE_DELTAS = 3 << SRC_TYPE_SHIFT;
    //public final static int SRC_TYPE_IS_LOB = 4 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_GFE = 4 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED1 = 5 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED2 = 6 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED3 = 7 << SRC_TYPE_SHIFT;
    
    protected Chunk(long memoryAddress, int chunkSize, ChunkType chunkType) {
      validateAddressAndSize(memoryAddress, chunkSize);
      this.memoryAddress = memoryAddress;
      setSize(chunkSize);
      UnsafeMemoryChunk.writeAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET, MAGIC_NUMBER|chunkType.getSrcType());
    }
    public void readyForFree() {
      UnsafeMemoryChunk.writeAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET, 0);
    }
    public void readyForAllocation(ChunkType chunkType) {
      if (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET, 0, MAGIC_NUMBER|chunkType.getSrcType())) {
        throw new IllegalStateException("Expected 0 but found " + Integer.toHexString(UnsafeMemoryChunk.readAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET)));
      }
    }
    /**
     * Should only be used by FakeChunk subclass
     */
    protected Chunk() {
      this.memoryAddress = 0L;
    }
    
    /**
     * Used to create a Chunk given an existing, already allocated,
     * memoryAddress. The off heap header has already been initialized.
     */
    protected Chunk(long memoryAddress) {
      validateAddress(memoryAddress);
      this.memoryAddress = memoryAddress;
    }
    
    protected Chunk(Chunk chunk) {
      this.memoryAddress = chunk.memoryAddress;
    }
    
    /**
     * Throw an exception if this chunk is not allocated
     */
    public void checkIsAllocated() {
      int originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
      if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
        throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
      }
    }
    
    public void incSize(int inc) {
      setSize(getSize()+inc);
    }
    
    protected void beforeReturningToAllocator() {
      
    }

    @Override
    public int getSize() {
      return getSize(this.memoryAddress);
    }

    public void setSize(int size) {
      setSize(this.memoryAddress, size);
    }

    public long getMemoryAddress() {
      return this.memoryAddress;
    }
    
    public int getDataSize() {
      /*int dataSizeDelta = UnsafeMemoryChunk.readAbsoluteInt(this.memoryAddress+REF_COUNT_OFFSET);
      dataSizeDelta &= DATA_SIZE_DELTA_MASK;
      dataSizeDelta >>= DATA_SIZE_SHIFT;
      return getSize() - dataSizeDelta;*/
      return getDataSize(this.memoryAddress);
    }
    
    protected static int getDataSize(long memoryAdress) {
      int dataSizeDelta = UnsafeMemoryChunk.readAbsoluteInt(memoryAdress+REF_COUNT_OFFSET);
      dataSizeDelta &= DATA_SIZE_DELTA_MASK;
      dataSizeDelta >>= DATA_SIZE_SHIFT;
      return getSize(memoryAdress) - dataSizeDelta;
    }
    
    protected long getBaseDataAddress() {
      return this.memoryAddress+OFF_HEAP_HEADER_SIZE;
    }
    protected int getBaseDataOffset() {
      return 0;
    }
    
    /**
     * Creates and returns a direct ByteBuffer that contains the contents of this Chunk.
     * Note that the returned ByteBuffer has a reference to this chunk's
     * off-heap address so it can only be used while this Chunk is retained.
     * @return the created direct byte buffer or null if it could not be created.
     */
    @Unretained
    public ByteBuffer createDirectByteBuffer() {
      return basicCreateDirectByteBuffer(getBaseDataAddress(), getDataSize());
    }
    @Override
    public void sendTo(DataOutput out) throws IOException {
      if (!this.isCompressed() && out instanceof HeapDataOutputStream) {
        ByteBuffer bb = createDirectByteBuffer();
        if (bb != null) {
          HeapDataOutputStream hdos = (HeapDataOutputStream) out;
          if (this.isSerialized()) {
            hdos.write(bb);
          } else {
            hdos.writeByte(DSCODE.BYTE_ARRAY);
            InternalDataSerializer.writeArrayLength(bb.remaining(), hdos);
            hdos.write(bb);
          }
          return;
        }
      }
      super.sendTo(out);
    }
    
    @Override
    public void sendAsByteArray(DataOutput out) throws IOException {
      if (!isCompressed() && out instanceof HeapDataOutputStream) {
        ByteBuffer bb = createDirectByteBuffer();
        if (bb != null) {
          HeapDataOutputStream hdos = (HeapDataOutputStream) out;
          InternalDataSerializer.writeArrayLength(bb.remaining(), hdos);
          hdos.write(bb);
          return;
        }
      }
      super.sendAsByteArray(out);
    }
       
    private static volatile Class dbbClass = null;
    private static volatile Constructor dbbCtor = null;
    private static volatile boolean dbbCreateFailed = false;
    
    /**
     * @return the created direct byte buffer or null if it could not be created.
     */
    private static ByteBuffer basicCreateDirectByteBuffer(long baseDataAddress, int dataSize) {
      if (dbbCreateFailed) {
        return null;
      }
      Constructor ctor = dbbCtor;
      if (ctor == null) {
        Class c = dbbClass;
        if (c == null) {
          try {
            c = Class.forName("java.nio.DirectByteBuffer");
          } catch (ClassNotFoundException e) {
            //throw new IllegalStateException("Could not find java.nio.DirectByteBuffer", e);
            dbbCreateFailed = true;
            dbbAddressFailed = true;
            return null;
          }
          dbbClass = c;
        }
        try {
          ctor = c.getDeclaredConstructor(long.class, int.class);
        } catch (NoSuchMethodException | SecurityException e) {
          //throw new IllegalStateException("Could not get constructor DirectByteBuffer(long, int)", e);
          dbbClass = null;
          dbbCreateFailed = true;
          return null;
        }
        ctor.setAccessible(true);
        dbbCtor = ctor;
      }
      try {
        return (ByteBuffer)ctor.newInstance(baseDataAddress, dataSize);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        //throw new IllegalStateException("Could not create an instance using DirectByteBuffer(long, int)", e);
        dbbClass = null;
        dbbCtor = null;
        dbbCreateFailed = true;
        return null;
      }
    }
    private static volatile Method dbbAddressMethod = null;
    private static volatile boolean dbbAddressFailed = false;
    
    /**
     * Returns the address of the Unsafe memory for the first byte of a direct ByteBuffer.
     * If the buffer is not direct or the address can not be obtained return 0.
     */
    public static long getDirectByteBufferAddress(ByteBuffer bb) {
      if (!bb.isDirect()) {
        return 0L;
      }
      if (dbbAddressFailed) {
        return 0L;
      }
      Method m = dbbAddressMethod;
      if (m == null) {
        Class c = dbbClass;
        if (c == null) {
          try {
            c = Class.forName("java.nio.DirectByteBuffer");
          } catch (ClassNotFoundException e) {
            //throw new IllegalStateException("Could not find java.nio.DirectByteBuffer", e);
            dbbCreateFailed = true;
            dbbAddressFailed = true;
            return 0L;
          }
          dbbClass = c;
        }
        try {
          m = c.getDeclaredMethod("address");
        } catch (NoSuchMethodException | SecurityException e) {
          //throw new IllegalStateException("Could not get method DirectByteBuffer.address()", e);
          dbbClass = null;
          dbbAddressFailed = true;
          return 0L;
        }
        m.setAccessible(true);
        dbbAddressMethod = m;
      }
      try {
        return (Long)m.invoke(bb);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        //throw new IllegalStateException("Could not create an invoke DirectByteBuffer.address()", e);
        dbbClass = null;
        dbbAddressMethod = null;
        dbbAddressFailed = true;
        return 0L;
      }
    }
    /**
     * Returns an address that can be used with unsafe apis to access this chunks memory.
     * @param offset the offset from this chunk's first byte of the byte the returned address should point to. Must be >= 0.
     * @param size the number of bytes that will be read using the returned address. Assertion will use this to verify that all the memory accessed belongs to this chunk. Must be > 0.
     * @return a memory address that can be used with unsafe apis
     */
    public long getUnsafeAddress(int offset, int size) {
      assert offset >= 0 && offset + size <= getDataSize(): "Offset=" + offset + ",size=" + size + ",dataSize=" + getDataSize() + ", chunkSize=" + getSize() + ", but offset + size must be <= " + getDataSize();
      assert size > 0;
      long result = getBaseDataAddress() + offset;
      // validateAddressAndSizeWithinSlab(result, size);
      return result;
    }
    
    @Override
    public byte readByte(int offset) {
      assert offset < getDataSize();
      return UnsafeMemoryChunk.readAbsoluteByte(getBaseDataAddress() + offset);
    }

    @Override
    public void writeByte(int offset, byte value) {
      assert offset < getDataSize();
      UnsafeMemoryChunk.writeAbsoluteByte(getBaseDataAddress() + offset, value);
    }

    @Override
    public void readBytes(int offset, byte[] bytes) {
      readBytes(offset, bytes, 0, bytes.length);
    }

    @Override
    public void writeBytes(int offset, byte[] bytes) {
      writeBytes(offset, bytes, 0, bytes.length);
    }

    public long getAddressForReading(int offset, int size) {
      assert offset+size <= getDataSize();
      return getBaseDataAddress() + offset;
    }
    
    @Override
    public void readBytes(int offset, byte[] bytes, int bytesOffset, int size) {
      assert offset+size <= getDataSize();
      UnsafeMemoryChunk.readAbsoluteBytes(getBaseDataAddress() + offset, bytes, bytesOffset, size);
    }

    @Override
    public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size) {
      assert offset+size <= getDataSize();
      validateAddressAndSizeWithinSlab(getBaseDataAddress() + offset, size);
      UnsafeMemoryChunk.writeAbsoluteBytes(getBaseDataAddress() + offset, bytes, bytesOffset, size);
    }
    
    @Override
    public void release() {
      release(this.memoryAddress, true);
     }

    @Override
    public int compareTo(Chunk o) {
      int result = Integer.signum(getSize() - o.getSize());
      if (result == 0) {
        // For the same sized chunks we really don't care about their order
        // but we need compareTo to only return 0 if the two chunks are identical
        result = Long.signum(getMemoryAddress() - o.getMemoryAddress());
      }
      return result;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof Chunk) {
        return getMemoryAddress() == ((Chunk) o).getMemoryAddress();
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      long value = this.getMemoryAddress();
      return (int)(value ^ (value >>> 32));
    }

    // OffHeapCachedDeserializable methods 
    
    @Override
    public void setSerializedValue(byte[] value) {
      writeBytes(0, value);
    }
    
    public byte[] getDecompressedBytes(RegionEntryContext context) {
      byte[] result = getCompressedBytes();
      long time = context.getCachePerfStats().startDecompression();
      result = context.getCompressor().decompress(result);
      context.getCachePerfStats().endDecompression(time);      
      return result;
    }
    
    /**
     * Returns the raw possibly compressed bytes of this chunk
     */
    public byte[] getCompressedBytes() {
      byte[] result = new byte[getDataSize()];
      readBytes(0, result);
      //debugLog("reading", true);
      getAllocator().getStats().incReads();
      return result;
    }
    protected byte[] getRawBytes() {
      byte[] result = getCompressedBytes();
      // TODO OFFHEAP: change the following to assert !isCompressed();
      if (isCompressed()) {
        throw new UnsupportedOperationException();
      }
      return result;
    }

    @Override
    public byte[] getSerializedValue() {
      byte [] result = getRawBytes();
      if (!isSerialized()) {
        // The object is a byte[]. So we need to make it look like a serialized byte[] in our result
        result = EntryEventImpl.serialize(result);
      }
      return result;
    }
    
    @Override
    public Object getDeserializedValue(Region r, RegionEntry re) {
      if (isSerialized()) {
        // TODO OFFHEAP: debug deserializeChunk
        return EntryEventImpl.deserialize(getRawBytes());
        //assert !isCompressed();
        //return EntryEventImpl.deserializeChunk(this);
      } else {
        return getRawBytes();
      }
    }
    
    /**
     * We want this to include memory overhead so use getSize() instead of getDataSize().
     */
    @Override
    public int getSizeInBytes() {
      // Calling getSize includes the off heap header size.
      // We do not add anything to this since the size of the reference belongs to the region entry size
      // not the size of this object.
      return getSize();
    }

    @Override
    public int getValueSizeInBytes() {
      return getDataSize();
    }

    @Override
    public void copyBytes(int src, int dst, int size) {
      throw new UnsupportedOperationException("Implement if used");
//      assert src+size <= getDataSize();
//      assert dst+size < getDataSize();
//      getSlabs()[this.getSlabIdx()].copyBytes(getBaseDataAddress()+src, getBaseDataAddress()+dst, size);
    }

    @Override
    public boolean isSerialized() {
      return (UnsafeMemoryChunk.readAbsoluteInt(this.memoryAddress+REF_COUNT_OFFSET) & IS_SERIALIZED_BIT) != 0;
    }

    @Override
    public boolean isCompressed() {
      return (UnsafeMemoryChunk.readAbsoluteInt(this.memoryAddress+REF_COUNT_OFFSET) & IS_COMPRESSED_BIT) != 0;
    }

    @Override
    public boolean retain() {
      return retain(this.memoryAddress);
    }

    @Override
    public int getRefCount() {
      return getRefCount(this.memoryAddress);
    }

    // By adding this one object ref to Chunk we are able to have free lists that only have memory overhead of a single objref per free item.
    //private Node cbNodeNext;
    @Override
    public void setNextCBNode(Node next) {
      throw new UnsupportedOperationException();
      //this.cbNodeNext = next;
    }

    @Override
    public Node getNextCBNode() {
      throw new UnsupportedOperationException();
      //return this.cbNodeNext;
    }
    public static int getSize(long memAddr) {
      validateAddress(memAddr);
      return UnsafeMemoryChunk.readAbsoluteInt(memAddr+CHUNK_SIZE_OFFSET);
    }
    public static void setSize(long memAddr, int size) {
      validateAddressAndSize(memAddr, size);
      UnsafeMemoryChunk.writeAbsoluteInt(memAddr+CHUNK_SIZE_OFFSET, size);
    }
    public static long getNext(long memAddr) {
      validateAddress(memAddr);
      return UnsafeMemoryChunk.readAbsoluteLong(memAddr+OFF_HEAP_HEADER_SIZE);
    }
    public static void setNext(long memAddr, long next) {
      validateAddress(memAddr);
      UnsafeMemoryChunk.writeAbsoluteLong(memAddr+OFF_HEAP_HEADER_SIZE, next);
    }
    @Override
    public ChunkType getChunkType() {
      return getAllocator().getChunkFactory().getChunkTypeForAddress(getMemoryAddress());
    }
    public static int getSrcTypeOrdinal(long memAddr) {
      return getSrcType(memAddr) >> SRC_TYPE_SHIFT;
    }
    public static int getSrcType(long memAddr) {
      return getSrcTypeFromRawBits(UnsafeMemoryChunk.readAbsoluteInt(memAddr+REF_COUNT_OFFSET));
    }
    public static int getSrcTypeFromRawBits(int rawBits) {
      return rawBits & SRC_TYPE_MASK;
    }
    public static int getSrcTypeOrdinalFromRawBits(int rawBits) {
      return getSrcTypeFromRawBits(rawBits) >> SRC_TYPE_SHIFT;
    }
    
    /**
     * Fills the chunk with a repeated byte fill pattern.
     * @param baseAddress the starting address for a {@link Chunk}.
     */
    public static void fill(long baseAddress) {
      long startAddress = baseAddress + MIN_CHUNK_SIZE;
      int size = getSize(baseAddress) - MIN_CHUNK_SIZE;
      
      UnsafeMemoryChunk.fill(startAddress, size, FILL_BYTE);
    }
    
    /**
     * Validates that the fill pattern for this chunk has not been disturbed.  This method
     * assumes the TINY_MULTIPLE is 8 bytes.
     * @throws IllegalStateException when the pattern has been violated.
     */
    public void validateFill() {
      assert TINY_MULTIPLE == 8;
      
      long startAddress = getMemoryAddress() + MIN_CHUNK_SIZE;
      int size = getSize() - MIN_CHUNK_SIZE;
      
      for(int i = 0;i < size;i += TINY_MULTIPLE) {
        if(UnsafeMemoryChunk.readAbsoluteLong(startAddress + i) != FILL_PATTERN) {
          throw new IllegalStateException("Fill pattern violated for chunk " + getMemoryAddress() + " with size " + getSize());
        }        
      }
    }

    public void setSerialized(boolean isSerialized) {
      if (isSerialized) {
        int bits;
        int originalBits;
        do {
          originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
          if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
            throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
          }
          bits = originalBits | IS_SERIALIZED_BIT;
        } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, originalBits, bits));
      }
    }
    public void setCompressed(boolean isCompressed) {
      if (isCompressed) {
        int bits;
        int originalBits;
        do {
          originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
          if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
            throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
          }
          bits = originalBits | IS_COMPRESSED_BIT;
        } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, originalBits, bits));
      }
    }
    public void setDataSize(int dataSize) { // KIRK
      assert dataSize <= getSize();
      int delta = getSize() - dataSize;
      assert delta <= (DATA_SIZE_DELTA_MASK >> DATA_SIZE_SHIFT);
      delta <<= DATA_SIZE_SHIFT;
      int bits;
      int originalBits;
      do {
        originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
        if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
          throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
        }
        bits = originalBits;
        bits &= ~DATA_SIZE_DELTA_MASK; // clear the old dataSizeDelta bits
        bits |= delta; // set the dataSizeDelta bits to the new delta value
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, originalBits, bits));
    }
    
    public void initializeUseCount() {
      int rawBits;
      do {
        rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
        if ((rawBits&MAGIC_MASK) != MAGIC_NUMBER) {
          throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(rawBits));
        }
        int uc = rawBits & REF_COUNT_MASK;
        if (uc != 0) {
          throw new IllegalStateException("Expected use count to be zero but it was: " + uc + " rawBits=0x" + Integer.toHexString(rawBits));
        }
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, rawBits, rawBits+1));
    }

    public static int getRefCount(long memAddr) {
      return UnsafeMemoryChunk.readAbsoluteInt(memAddr+REF_COUNT_OFFSET) & REF_COUNT_MASK;
    }

    public static boolean retain(long memAddr) {
      validateAddress(memAddr);
      int uc;
      int rawBits;
      int retryCount = 0;
      do {
        rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET);
        if ((rawBits&MAGIC_MASK) != MAGIC_NUMBER) {
          // same as uc == 0
          // TODO MAGIC_NUMBER rethink its use and interaction with compactor fragments
          return false;
        }
        uc = rawBits & REF_COUNT_MASK;
        if (uc == MAX_REF_COUNT) {
          throw new IllegalStateException("Maximum use count exceeded. rawBits=" + Integer.toHexString(rawBits));
        } else if (uc == 0) {
          return false;
        }
        retryCount++;
        if (retryCount > 1000) {
          throw new IllegalStateException("tried to write " + (rawBits+1) + " to @" + Long.toHexString(memAddr) + " 1,000 times.");
        }
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET, rawBits, rawBits+1));
      //debugLog("use inced ref count " + (uc+1) + " @" + Long.toHexString(memAddr), true);
      if (trackReferenceCounts()) {
        refCountChanged(memAddr, false, uc+1);
      }

      return true;
    }
    public static void release(final long memAddr, boolean issueOnReturnCallback) {
      validateAddress(memAddr);
      int newCount;
      int rawBits;
      boolean returnToAllocator;
      do {
        returnToAllocator = false;
        rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET);
        if ((rawBits&MAGIC_MASK) != MAGIC_NUMBER) {
          String msg = "It looks like off heap memory @" + Long.toHexString(memAddr) + " was already freed. rawBits=" + Integer.toHexString(rawBits) + " history=" + getFreeRefCountInfo(memAddr);
          //debugLog(msg, true);
          throw new IllegalStateException(msg);
        }
        int curCount = rawBits&REF_COUNT_MASK;
        if ((curCount) == 0) {
          //debugLog("too many frees @" + Long.toHexString(memAddr), true);
          throw new IllegalStateException("Memory has already been freed." + " history=" + getFreeRefCountInfo(memAddr) /*+ System.identityHashCode(this)*/);
        }
        if (curCount == 1) {
          newCount = 0; // clear the use count, bits, and the delta size since it will be freed.
          returnToAllocator = true;
        } else {
          newCount = rawBits-1;
        }
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET, rawBits, newCount));
      //debugLog("free deced ref count " + (newCount&USE_COUNT_MASK) + " @" + Long.toHexString(memAddr), true);
      if (returnToAllocator ) {
        /*
        if(issueOnReturnCallback) {
         final GemFireCacheImpl.StaticSystemCallbacks sysCb =
              GemFireCacheImpl.FactoryStatics.systemCallbacks;
          if(sysCb != null ) {
            ChunkType ct = SimpleMemoryAllocatorImpl.getAllocator().getChunkFactory().getChunkTypeForRawBits(rawBits);
            int dataSizeDelta = computeDataSizeDelta(rawBits);
            sysCb.beforeReturningOffHeapMemoryToAllocator(memAddr, ct, dataSizeDelta);
          }
        }
        */
       
        if (trackReferenceCounts()) {
          if (trackFreedReferenceCounts()) {
            refCountChanged(memAddr, true, newCount&REF_COUNT_MASK);
          }
          freeRefCountInfo(memAddr);
        }
        
        // Use fill pattern for free list data integrity check.
        if(SimpleMemoryAllocatorImpl.getAllocator().validateMemoryWithFill) {
          fill(memAddr);
        }
        
        getAllocator().freeChunk(memAddr);
      } else {
        if (trackReferenceCounts()) {
          refCountChanged(memAddr, true, newCount&REF_COUNT_MASK);
        }
      }
    }
    
    private static int computeDataSizeDelta(int rawBits) {
      int dataSizeDelta = rawBits;
      dataSizeDelta &= DATA_SIZE_DELTA_MASK;
      dataSizeDelta >>= DATA_SIZE_SHIFT;
      return dataSizeDelta;
    }
    
    @Override
    public String toString() {
      return toStringForOffHeapByteSource();
      // This old impl is not safe because it calls getDeserializedForReading and we have code that call toString that does not inc the refcount.
      // Also if this Chunk is compressed we don't know how to decompress it.
      //return super.toString() + ":<dataSize=" + getDataSize() + " refCount=" + getRefCount() + " addr=" + getMemoryAddress() + " storedObject=" + getDeserializedForReading() + ">";
    }
    
    protected String toStringForOffHeapByteSource() {
      return super.toString() + ":<dataSize=" + getDataSize() + " refCount=" + getRefCount() + " addr=" + Long.toHexString(getMemoryAddress()) + ">";
    }
    
    @Override
    public State getState() {
      if (getRefCount() > 0) {
        return State.ALLOCATED;
      } else {
        return State.DEALLOCATED;
      }
    }
    @Override
    public MemoryBlock getNextBlock() {
      throw new UnsupportedOperationException();
    }
    @Override
    public int getBlockSize() {
      return getSize();
    }
    @Override
    public int getSlabId() {
      throw new UnsupportedOperationException();
    }
    @Override
    public int getFreeListId() {
      return -1;
    }
    @Override
    public String getDataType() {
      return null;
    }
    @Override
    public Object getDataValue() {
      return null;
    }
    public Chunk slice(int position, int limit) {
      throw new UnsupportedOperationException();
    }
  }
  public static class FakeChunk extends Chunk {
    private final int size;
    public FakeChunk(int size) {
      super();
      this.size = size;
    }
    @Override
    public int getSize() {
      return this.size;
    }
  }
  /**
   * Simple stack structure.
   * The chunk in the top of this stack is pointed to by topAddr.
   * Each next chunk is found be reading a long from the data in the previous chunk.
   * An address of 0L means it is then end of the stack.
   * This class has a subtle race condition in it between
   * one thread doing a poll, allocating data into the chunk returned by poll,
   * and then offering it back. Meanwhile another thread did a poll of the same head chunk,
   * read some of the allocating data as the "next" address and then did the compareAndSet
   * call and it worked because the first thread had already put it back in.
   * So this class should not be used. Instead use SyncChunkStack.
   * 
   * @author darrel
   *
   */
  public static class BuggyConcurrentChunkStack {
    // all uses of topAddr are done using topAddrUpdater
    @SuppressWarnings("unused")
    private volatile long topAddr;
    private static final AtomicLongFieldUpdater<BuggyConcurrentChunkStack> topAddrUpdater = AtomicLongFieldUpdater.newUpdater(BuggyConcurrentChunkStack.class, "topAddr");
    
    public BuggyConcurrentChunkStack(long addr) {
      if (addr != 0L) validateAddress(addr);
      this.topAddr = addr;
    }
    public BuggyConcurrentChunkStack() {
      this.topAddr = 0L;
    }
    public boolean isEmpty() {
      return topAddrUpdater.get(this) == 0L;
    }
    public void offer(long e) {
      assert e != 0;
      validateAddress(e);
      long curHead;
      do {
        curHead = topAddrUpdater.get(this);
        Chunk.setNext(e, curHead);
      } while (!topAddrUpdater.compareAndSet(this, curHead, e));
    }
    public long poll() {
      long result;
      long newHead;
      do {
        result = topAddrUpdater.get(this);
        if (result == 0L) return 0L;
        newHead = Chunk.getNext(result);
        
      } while (!topAddrUpdater.compareAndSet(this, result, newHead));
      if (newHead != 0L) validateAddress(newHead);
      return result;
    }
    /**
     * Removes all the Chunks from this stack
     * and returns the address of the first chunk.
     * The caller owns all the Chunks after this call.
     */
    public long clear() {
      long result;
      do {
        result = topAddrUpdater.get(this);
        if (result == 0L) return 0L;
      } while (!topAddrUpdater.compareAndSet(this, result, 0L));
      return result;
    }
    public void logSizes(LogWriter lw, String msg) {
      long headAddr = topAddrUpdater.get(this);
      long addr;
      boolean concurrentModDetected;
      do {
        concurrentModDetected = false;
        addr = headAddr;
        while (addr != 0L) {
          int curSize = Chunk.getSize(addr);
          addr = Chunk.getNext(addr);
          long curHead = topAddrUpdater.get(this);
          if (curHead != headAddr) {
            headAddr = curHead;
            concurrentModDetected = true;
            // Someone added or removed from the stack.
            // So we break out of the inner loop and start
            // again at the new head.
            break;
          }
          // TODO construct a single log msg
          // that gets reset on the concurrent mad.
          lw.info(msg + curSize);
        }
      } while (concurrentModDetected);
    }
    public long computeTotalSize() {
      long result;
      long headAddr = topAddrUpdater.get(this);
      long addr;
      boolean concurrentModDetected;
      do {
        concurrentModDetected = false;
        result = 0;
        addr = headAddr;
        while (addr != 0L) {
          result += Chunk.getSize(addr);
          addr = Chunk.getNext(addr);
          long curHead = topAddrUpdater.get(this);
          if (curHead != headAddr) {
            headAddr = curHead;
            concurrentModDetected = true;
            // Someone added or removed from the stack.
            // So we break out of the inner loop and start
            // again at the new head.
            break;
          }
        }
      } while (concurrentModDetected);
      return result;
    }
  }
  public static class SyncChunkStack {
    // Ok to read without sync but must be synced on write
    private volatile long topAddr;
    
    public SyncChunkStack(long addr) {
      if (addr != 0L) validateAddress(addr);
      this.topAddr = addr;
    }
    public SyncChunkStack() {
      this.topAddr = 0L;
    }
    public boolean isEmpty() {
      return this.topAddr == 0L;
    }
    public void offer(long e) {
      assert e != 0;
      validateAddress(e);
      synchronized (this) {
        Chunk.setNext(e, this.topAddr);
        this.topAddr = e;
      }
    }
    public long poll() {
      long result;
      synchronized (this) {
        result = this.topAddr;
        if (result != 0L) {
          this.topAddr = Chunk.getNext(result);
        }
      }
      return result;
    }
    /**
     * Removes all the Chunks from this stack
     * and returns the address of the first chunk.
     * The caller owns all the Chunks after this call.
     */
    public long clear() {
      long result;
      synchronized (this) {
        result = this.topAddr;
        if (result != 0L) {
          this.topAddr = 0L;
        }
      }
      return result;
    }
    public void logSizes(LogWriter lw, String msg) {
      long headAddr = this.topAddr;
      long addr;
      boolean concurrentModDetected;
      do {
        concurrentModDetected = false;
        addr = headAddr;
        while (addr != 0L) {
          int curSize = Chunk.getSize(addr);
          addr = Chunk.getNext(addr);
          long curHead = this.topAddr;
          if (curHead != headAddr) {
            headAddr = curHead;
            concurrentModDetected = true;
            // Someone added or removed from the stack.
            // So we break out of the inner loop and start
            // again at the new head.
            break;
          }
          // TODO construct a single log msg
          // that gets reset on the concurrent mad.
          lw.info(msg + curSize);
        }
      } while (concurrentModDetected);
    }
    public long computeTotalSize() {
      long result;
      long headAddr = this.topAddr;
      long addr;
      boolean concurrentModDetected;
      do {
        concurrentModDetected = false;
        result = 0;
        addr = headAddr;
        while (addr != 0L) {
          result += Chunk.getSize(addr);
          addr = Chunk.getNext(addr);
          long curHead = this.topAddr;
          if (curHead != headAddr) {
            headAddr = curHead;
            concurrentModDetected = true;
            // Someone added or removed from the stack.
            // So we break out of the inner loop and start
            // again at the new head.
            break;
          }
        }
      } while (concurrentModDetected);
      return result;
    }
  }
  
  private static void validateAddress(long addr) {
    validateAddressAndSize(addr, -1);
  }
  
  private static void validateAddressAndSize(long addr, int size) {
    // if the caller does not have a "size" to provide then use -1
    if ((addr & 7) != 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("address was not 8 byte aligned: 0x").append(Long.toString(addr, 16));
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.singleton;
      if (ma != null) {
        sb.append(". Valid addresses must be in one of the following ranges: ");
        for (int i=0; i < ma.slabs.length; i++) {
          long startAddr = ma.slabs[i].getMemoryAddress();
          long endAddr = startAddr + ma.slabs[i].getSize();
          sb.append("[").append(Long.toString(startAddr, 16)).append("..").append(Long.toString(endAddr, 16)).append("] ");
        }
      }
      throw new IllegalStateException(sb.toString());
    }
    if (addr >= 0 && addr < 1024) {
      throw new IllegalStateException("addr was smaller than expected 0x" + addr);
    }
    validateAddressAndSizeWithinSlab(addr, size);
  }

  private static void validateAddressAndSizeWithinSlab(long addr, int size) {
    if (DO_EXPENSIVE_VALIDATION) {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.singleton;
      if (ma != null) {
        for (int i=0; i < ma.slabs.length; i++) {
          if (ma.slabs[i].getMemoryAddress() <= addr && addr < (ma.slabs[i].getMemoryAddress() + ma.slabs[i].getSize())) {
            // validate addr + size is within the same slab
            if (size != -1) { // skip this check if size is -1
              if (!(ma.slabs[i].getMemoryAddress() <= (addr+size-1) && (addr+size-1) < (ma.slabs[i].getMemoryAddress() + ma.slabs[i].getSize()))) {
                throw new IllegalStateException(" address 0x" + Long.toString(addr+size-1, 16) + " does not address the original slab memory");
              }
            }
            return;
          }
        }
        throw new IllegalStateException(" address 0x" + Long.toString(addr, 16) + " does not address the original slab memory");
      }
    }
  }
  
  /**
   * A fragment is a chunk of memory that can have chunks allocated from it.
   * The allocations are always from the front so the free memory is always
   * at the end. The freeIdx keeps track of the first byte of free memory in
   * the fragment.
   * The base memory address and the total size of a fragment never change.
   * During compaction fragments go away and are recreated.
   * 
   * @author darrel
   *
   */
  public static class Fragment implements MemoryBlock {
    private static long FILL_PATTERN = Chunk.FILL_PATTERN;
    private static byte FILL_BYTE = Chunk.FILL_BYTE;
    private final long baseAddr;
    private final int size;
    private volatile int freeIdx;
    private static AtomicIntegerFieldUpdater<Fragment> freeIdxUpdater = AtomicIntegerFieldUpdater.newUpdater(Fragment.class, "freeIdx");
    
    public Fragment(long addr, int size) {
      validateAddress(addr);
      this.baseAddr = addr;
      this.size = size;
      freeIdxUpdater.set(this, 0);
    }
    
    public int freeSpace() {
      return getSize() - getFreeIndex();
    }

    public boolean allocate(int oldOffset, int newOffset) {
      return freeIdxUpdater.compareAndSet(this, oldOffset, newOffset);
    }

    public int getFreeIndex() {
      return freeIdxUpdater.get(this);
    }

    public int getSize() {
      return this.size;
    }

    public long getMemoryAddress() {
      return this.baseAddr;
    }

    @Override
    public State getState() {
      return State.UNUSED;
    }

    @Override
    public MemoryBlock getNextBlock() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public int getBlockSize() {
      return freeSpace();
    }
    
    @Override
    public int getSlabId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getFreeListId() {
      return -1;
    }

    @Override
    public int getRefCount() {
      return 0;
    }

    @Override
    public String getDataType() {
      return "N/A";
    }

    @Override
    public boolean isSerialized() {
      return false;
    }

    @Override
    public boolean isCompressed() {
      return false;
    }

    @Override
    public Object getDataValue() {
      return null;
    }
    
    public void fill() {
      UnsafeMemoryChunk.fill(this.baseAddr, this.size, FILL_BYTE);
    }

    @Override
    public ChunkType getChunkType() {
      return null;
    }
  }

  public static class ConcurrentBag<E extends ConcurrentBag.Node> implements Iterable<E> {
    public interface Node {
      public void setNextCBNode(Node next);
      public Node getNextCBNode();
    }
    private final AtomicReference<E> root = new AtomicReference<E>(null);
    
    public ConcurrentBag() {
    }
    public ConcurrentBag(int initialSize) {
    }
    @Override
    public Iterator<E> iterator() {
      // TODO this iterator is used a bunch in the compactor.
      // If a concurrent poll is done while iterating then the
      // iterator may return a element that has been removed from
      // the bag by the poll call. This is ok currently because
      // the next field on Node is only used by the bag and is not
      // nulled out by the poll call.
      return new Iterator<E>() {
        private E nextElem = root.get();

        @Override
        public boolean hasNext() {
          return this.nextElem != null;
        }

        @Override
        public E next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          E result = this.nextElem;
          this.nextElem = (E)this.nextElem.getNextCBNode();
          return result;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
    public void offer(E e) {
      E tmp;
      do {
        tmp = this.root.get();
        e.setNextCBNode(tmp);
      } while (!this.root.compareAndSet(tmp, e));
    }
    public E poll() {
      E result;
      do {
        result = this.root.get();
        if (result == null) return null;
      } while (!this.root.compareAndSet(result, (E)result.getNextCBNode()));
      // we should probably do
      // result.setNextCBNode(null);
      // but this could mess up a concurrent iterator so for now leave it set.
      // Since the objects added to the bag are never garbage having an extra
      // reference to one is no big deal.
      return result;
    }
  }
  
  public static class ConcurrentBagABQ<E> implements Iterable<E> {
    private static final int FREE_LIST_SIZE = Integer.getInteger("gemfire.FREE_LIST_SIZE", 1024);
    private final ArrayBlockingQueue<E> data;
    
    public ConcurrentBagABQ() {
      this.data = new ArrayBlockingQueue<E>(FREE_LIST_SIZE);
    }
    public ConcurrentBagABQ(int initialSize) {
      this.data = new ArrayBlockingQueue<E>(initialSize);
    }
    @Override
    public Iterator<E> iterator() {
      return this.data.iterator();
    }
    public void offer(E e) {
      this.data.add(e);
    }
    public E poll() {
      return this.data.poll();
    }
  }
  /**
   * The idea of this data structure is for it to be somewhat concurrent but to also not produce garbage.
   * Since this bag is used to implement each free list it can contain entries that get promoted to oldgen.
   * Note when used concurrently this structure does not maintain a pure FIFO ordering. That is why it is
   * called a Bag instead of a List.
   * 
   * @author darrel
   */
  public static class ConcurrentBagBroken<E> implements Iterable<E> {
    private static final int FREE_LIST_SIZE = Integer.getInteger("gemfire.FREE_LIST_SIZE", 1024);
    private volatile AtomicReferenceArray<E> data;
    private final AtomicInteger size = new AtomicInteger(0);
    
    public ConcurrentBagBroken() {
      this.data = new AtomicReferenceArray<E>(FREE_LIST_SIZE);
    }
    public ConcurrentBagBroken(int initialSize) {
      this.data = new AtomicReferenceArray<E>(initialSize);
    }
    @Override
    public Iterator<E> iterator() {
      return new Iterator<E>() {
        private final AtomicReferenceArray<E> itData = data;
        private int idx = 0;
        private E nextElem = null;

        @Override
        public boolean hasNext() {
          E next = null;
          do {
            if (this.idx >= this.itData.length()) {
              next = null;
              break;
            }
            next = this.itData.get(this.idx);
            if (next == null) {
              this.idx++;
            }
          } while (next == null);
          this.nextElem = next;
          return  next != null;
        }

        @Override
        public E next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          this.idx++;
          return this.nextElem;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
    public void offer(E e) {
      int idx = this.size.get();
      int dataLength = this.data.length();
      while (idx >= dataLength) {
        grow(dataLength);
        idx = this.size.get();
        dataLength = this.data.length();
      }
      while (!this.data.compareAndSet(idx, null, e)) {
        dataLength = this.data.length();
        if (idx >= dataLength) {
          grow(dataLength);
        }
        int oldIdx = idx;
        idx = this.size.get();
        if (oldIdx == idx && idx < dataLength-1) {
          idx++;
        }
      }
      // At this point we have put e in the data array.
      // Now update the size pointer to help the next guy find
      // the next slot.
      // If concurrent offers have already happened then they will
      // have set size to something even greater.
      // if concurrent polls have already happened then they will
      // have set size to something even less.
      this.size.compareAndSet(idx, idx+1);
    }
    public E poll() {
      int idx = -1;
      E result;
      do {
        int oldIdx = idx;
        idx = this.size.get();
        if (idx == oldIdx && idx > 0) {
          idx--;
        }
        if (idx == 0) {
          // sync in case a concurrent grow is happening.
          // this allows us to switch over to the new data array.
          synchronized (this) {
            idx = this.size.get();
            if (idx == 0) {
              return null;
            }
          }
        }
        result = this.data.getAndSet(idx-1, null);
      } while (result == null);
      // At this point we have taken result from the data array.
      // Now update the size pointer to help the next guy.
      // If concurrent polls have already happened then they will
      // have set size to something even less.
      // If concurrent offers have already happened then they will
      // have set size to something even greater.
      this.size.compareAndSet(idx, idx-1);
      return result;
    }
    private synchronized void grow(int oldDataLength) {
      // Note that concurrent polls may happen while we are in grow.
      // So we need to use the atomic operations to copy the data.
      if (this.data.length() != oldDataLength) return;
      AtomicReferenceArray<E> newData = new AtomicReferenceArray<E>(oldDataLength*2);
      int idx = 0;
      for (int i=0; i < oldDataLength; i++) {
        E e = this.data.getAndSet(i, null);
        if (e != null) {
          newData.lazySet(idx++, e);
        }
      }
      this.data = newData;
      this.size.set(idx);
    }
  }
  public static class ConcurrentBagLQ<E> implements Iterable<E> {
        private final ConcurrentLinkedQueue<E> delegate = new ConcurrentLinkedQueue<E>();
//    private final CopyOnWriteArrayList<ArrayBlockingQueue<E>> metaList = new CopyOnWriteArrayList<ArrayBlockingQueue<E>>();
//    private final AtomicInteger offerIdx = new AtomicInteger(0);
//    private final AtomicInteger pollIdx = new AtomicInteger(0);
//    private static final int CHUNK_SIZE = 1024;
    
    public ConcurrentBagLQ() {
//      this.metaList.add(new ArrayBlockingQueue<E>(CHUNK_SIZE));
    }
    public ConcurrentBagLQ(int size) {
    }

    @Override
    public Iterator<E> iterator() {
      // This impl does not support concurrent updates
//      return new Iterator<E>() {
//        private final Iterator<ArrayBlockingQueue<E>> metaListIterator = metaList.iterator();
//        private Iterator<E> qIterator = null;
//
//        @Override
//        public boolean hasNext() {
//          do {
//            if (this.qIterator != null && this.qIterator.hasNext()) {
//              return true;
//            }
//            if (this.metaListIterator.hasNext()) {
//              this.qIterator = metaListIterator.next().iterator();
//            } else {
//              this.qIterator = null;
//            }
//          } while (this.qIterator != null);
//          return false;
//        }
//
//        @Override
//        public E next() {
//          if (!hasNext()) {
//            throw new NoSuchElementException("No more values in iterator");
//          }
//          return this.qIterator.next();
//        }
//
//        @Override
//        public void remove() {
//          if (this.qIterator == null) {
//            throw new IllegalStateException("next has not been called.");
//          }
//          this.qIterator.remove();
//        }
//      };
      return this.delegate.iterator();
    }
    
    public void offer(E e) {
//      int start = this.offerIdx.get();
//      for (int i=start; i >= 0; i--) {
//        ArrayBlockingQueue<E> q = this.metaList.get(i);
//        if (q.offer(e)) {
//          if (i != start) {
//            this.offerIdx.set(i);
//          }
//          return;
//        }
//      }
//      for (int i=this.metaList.size()-1; i > start; i--) {
//        ArrayBlockingQueue<E> q = this.metaList.get(i);
//        if (q.offer(e)) {
//          this.offerIdx.set(i);
//          return;
//        }
//      }
//      ArrayBlockingQueue<E> q = new ArrayBlockingQueue<E>(CHUNK_SIZE);
//      q.offer(e);
//      this.metaList.add(q);
//      this.offerIdx.set(this.metaList.size()-1);
      if (!this.delegate.offer(e)) {
        throw new IllegalStateException("offer returned false");
      }
    }
    public E poll() {
//      int start = this.pollIdx.get();
//      int end = this.metaList.size()-1;
//      for (int i=start; i <= end; i++) {
//        ArrayBlockingQueue<E> q = this.metaList.get(i);
//        E result = q.poll();
//        if (result != null) {
//          if (i != start) {
//            this.pollIdx.set(i);
//          }
//          return result;
//        }
//      }
//      for (int i=0; i < start; i++) {
//        ArrayBlockingQueue<E> q = this.metaList.get(i);
//        E result = q.poll();
//        if (result != null) {
//          this.pollIdx.set(i);
//          return result;
//        }
//      }
//      return null;
      return this.delegate.poll();
    }
  }
  
  private void printSlabs() {
    for (int i =0; i < this.slabs.length; i++) {
      logger.info(slabs[i]);
    }
  }

  /** The inspection snapshot for MemoryInspector */
  private List<MemoryBlock> memoryBlocks;
  
  @Override
  public MemoryInspector getMemoryInspector() {
    return this;
  }
  
  @Override
  public synchronized void clearInspectionSnapshot() {
    this.memoryBlocks = null;
  }
  
  @Override
  public synchronized void createInspectionSnapshot() {
    List<MemoryBlock> value = this.memoryBlocks;
    if (value == null) {
      value = getOrderedBlocks();
      this.memoryBlocks = value;
    }
  }

  synchronized List<MemoryBlock> getInspectionSnapshot() {
    List<MemoryBlock> value = this.memoryBlocks;
    if (value == null) {
      return Collections.<MemoryBlock>emptyList();
    } else {
      return value;
    }
  }
  
  @Override
  public synchronized List<MemoryBlock> getOrphans() {
    List<Chunk> liveChunks = this.freeList.getLiveChunks();
    List<Chunk> regionChunks = getRegionLiveChunks();
    liveChunks.removeAll(regionChunks);
    List<MemoryBlock> orphans = new ArrayList<MemoryBlock>();
    for (Chunk chunk: liveChunks) {
      orphans.add(new MemoryBlockNode(chunk));
    }
    Collections.sort(orphans, 
        new Comparator<MemoryBlock>() {
          @Override
          public int compare(MemoryBlock o1, MemoryBlock o2) {
            return Long.valueOf(o1.getMemoryAddress()).compareTo(o2.getMemoryAddress());
          }
    });
    //this.memoryBlocks = new WeakReference<List<MemoryBlock>>(orphans);
    return orphans;
  }
  
  @Override
  public MemoryBlock getFirstBlock() {
    final List<MemoryBlock> value = getInspectionSnapshot();
    if (value.isEmpty()) {
      return null;
    } else {
      return value.get(0);
    }
  }
  
  @Override
  public List<MemoryBlock> getAllBlocks() {
    return getOrderedBlocks();
  }
  
  @Override
  public List<MemoryBlock> getAllocatedBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    addBlocksFromChunks(this.freeList.getLiveChunks(), value); // used chunks
    Collections.sort(value, 
        new Comparator<MemoryBlock>() {
          @Override
          public int compare(MemoryBlock o1, MemoryBlock o2) {
            return Long.valueOf(o1.getMemoryAddress()).compareTo(o2.getMemoryAddress());
          }
    });
    return value;
  }

  @Override
  public List<MemoryBlock> getDeallocatedBlocks() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<MemoryBlock> getUnusedBlocks() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public MemoryBlock getBlockContaining(long memoryAddress) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public MemoryBlock getBlockAfter(MemoryBlock block) {
    if (block == null) {
      return null;
    }
    List<MemoryBlock> blocks = getInspectionSnapshot();
    int nextBlock = blocks.indexOf(block) + 1;
    if (nextBlock > 0 && blocks.size() > nextBlock) {
      return blocks.get(nextBlock);
    } else {
      return null;
    }
  }

  private List<MemoryBlock> getOrderedBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    addBlocksFromFragments(this.freeList.fragmentList, value); // unused fragments
    addBlocksFromChunks(this.freeList.getLiveChunks(), value); // used chunks
    addBlocksFromChunks(this.freeList.hugeChunkSet, value);    // huge free chunks
    addMemoryBlocks(getTinyFreeBlocks(), value);           // tiny free chunks
    Collections.sort(value, 
        new Comparator<MemoryBlock>() {
          @Override
          public int compare(MemoryBlock o1, MemoryBlock o2) {
            return Long.valueOf(o1.getMemoryAddress()).compareTo(o2.getMemoryAddress());
            /*if (o1.getMemoryAddress() < o2.getMemoryAddress()) {
              return -1;
            } else if (o1.getMemoryAddress() == o2.getMemoryAddress()) {
              return 0;
            } else {
              return 1;
            }*/
          }
    });
    return value;
  }
  
  private void addBlocksFromFragments(Collection<Fragment> src, List<MemoryBlock> dest) {
    for (MemoryBlock block : src) {
      dest.add(new MemoryBlockNode(block));
    }
  }
  
  private void addBlocksFromChunks(Collection<Chunk> src, List<MemoryBlock> dest) {
    for (Chunk chunk : src) {
      dest.add(new MemoryBlockNode(chunk));
    }
  }
  
  private void addMemoryBlocks(Collection<MemoryBlock> src, List<MemoryBlock> dest) {
    for (MemoryBlock block : src) {
      dest.add(new MemoryBlockNode(block));
    }
  }
  
  private List<MemoryBlock> getTinyFreeBlocks() {
    List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    AtomicReferenceArray<SyncChunkStack> chunkStacks = this.freeList.tinyFreeLists;
    for (int i = 0; i < chunkStacks.length(); i++) {
      if (chunkStacks.get(i) == null) continue;
      long addr = chunkStacks.get(i).topAddr;
      final int size = Chunk.getSize(addr);
      final long address = addr;
      final int freeListId = i;
      while (addr != 0L) {
        value.add(new MemoryBlockNode(new MemoryBlock() {
          @Override
          public State getState() {
            return State.DEALLOCATED;
          }
          @Override
          public long getMemoryAddress() {
            return address;
          }
          @Override
          public int getBlockSize() {
            return size;
          }
          @Override
          public MemoryBlock getNextBlock() {
            throw new UnsupportedOperationException();
          }
          @Override
          public int getSlabId() {
            throw new UnsupportedOperationException();
          }
          @Override
          public int getFreeListId() {
            return freeListId;
          }
          @Override
          public int getRefCount() {
            return 0;
          }
          @Override
          public String getDataType() {
            return "N/A";
          }
          @Override
          public boolean isSerialized() {
            return false;
          }
          @Override
          public boolean isCompressed() {
            return false;
          }
          @Override
          public Object getDataValue() {
            return null;
          }
          @Override
          public ChunkType getChunkType() {
            return null;
          }
        }));
        addr = Chunk.getNext(addr);
      }
    }
    return value;
  }
  
  public class MemoryBlockNode implements MemoryBlock {
    private final MemoryBlock block;
    MemoryBlockNode(MemoryBlock block) {
      this.block = block;
    }
    @Override
    public State getState() {
      return this.block.getState();
    }
    @Override
    public long getMemoryAddress() {
      return this.block.getMemoryAddress();
    }
    @Override
    public int getBlockSize() {
      return this.block.getBlockSize();
    }
    @Override
    public MemoryBlock getNextBlock() {
      return getBlockAfter(this);
    }
    public int getSlabId() {
      return findSlab(getMemoryAddress());
    }
    @Override
    public int getFreeListId() {
      return this.block.getFreeListId();
    }
    public int getRefCount() {
      return Chunk.getRefCount(getMemoryAddress());
    }
    public String getDataType() {
      if (this.block.getDataType() != null) {
        return this.block.getDataType();
      }
      if (!isSerialized()) {
        // byte array
        if (isCompressed()) {
          return "compressed byte[" + ((Chunk)this.block).getDataSize() + "]";
        } else {
          return "byte[" + ((Chunk)this.block).getDataSize() + "]";
        }
      } else if (isCompressed()) {
        return "compressed object of size " + ((Chunk)this.block).getDataSize();
      }
      //Object obj = EntryEventImpl.deserialize(((Chunk)this.block).getRawBytes());
      byte[] bytes = ((Chunk)this.block).getRawBytes();
      return DataType.getDataType(bytes);
    }
    public boolean isSerialized() {
      return this.block.isSerialized();
    }
    public boolean isCompressed() {
      return this.block.isCompressed();
    }
    @Override
    public Object getDataValue() {
      String dataType = getDataType();
      if (dataType == null || dataType.equals("N/A")) {
        return null;
      } else if (isCompressed()) {
        return ((Chunk)this.block).getCompressedBytes();
      } else if (!isSerialized()) {
        // byte array
        //return "byte[" + ((Chunk)this.block).getDataSize() + "]";
        return ((Chunk)this.block).getRawBytes();
      } else {
        try {
          byte[] bytes = ((Chunk)this.block).getRawBytes();
          return DataSerializer.readObject(DataType.getDataInput(bytes));
        } catch (IOException e) {
          e.printStackTrace();
          return "IOException:" + e.getMessage();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
          return "ClassNotFoundException:" + e.getMessage();
        } catch (CacheClosedException e) {
          e.printStackTrace();
          return "CacheClosedException:" + e.getMessage();
        }
      }
    }
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(MemoryBlock.class.getSimpleName());
      sb.append("{");
      sb.append("MemoryAddress=").append(getMemoryAddress());
      sb.append(", State=").append(getState());
      sb.append(", BlockSize=").append(getBlockSize());
      sb.append(", SlabId=").append(getSlabId());
      sb.append(", FreeListId=");
      if (getState() == State.UNUSED || getState() == State.ALLOCATED) {
        sb.append("NONE");
      } else if (getFreeListId() == -1) {
        sb.append("HUGE");
      } else {
        sb.append(getFreeListId());
      }
      sb.append(", RefCount=").append(getRefCount());
      ChunkType ct = this.getChunkType();
      if (ct != null) {
        sb.append(", " + ct);
      }
      sb.append(", isSerialized=").append(isSerialized());
      sb.append(", isCompressed=").append(isCompressed());
      sb.append(", DataType=").append(getDataType());
      {
        sb.append(", DataValue=");
        Object dataValue = getDataValue();
        if (dataValue instanceof byte[]) {
          byte[] ba = (byte[]) dataValue;
          if (ba.length < 1024) {
            sb.append(Arrays.toString(ba));
          } else {
            sb.append("<byte array of length " + ba.length + ">");
          }
        } else {
          sb.append(dataValue);
        }
      }
      sb.append("}");
      return sb.toString();
    }
    @Override
    public ChunkType getChunkType() {
      return this.block.getChunkType();
    }
  }
  
  /*
   * Set this to "true" to perform data integrity checks on allocated and reused Chunks.  This may clobber 
   * performance so turn on only when necessary.
   */
  private final boolean validateMemoryWithFill = Boolean.getBoolean("gemfire.validateOffHeapWithFill");
  
  private final static boolean trackRefCounts = Boolean.getBoolean("gemfire.trackOffHeapRefCounts");
  private final static boolean trackFreedRefCounts = Boolean.getBoolean("gemfire.trackOffHeapFreedRefCounts");
  private final static ConcurrentMap<Long, List<RefCountChangeInfo>> stacktraces;
  private final static ConcurrentMap<Long, List<RefCountChangeInfo>> freedStacktraces;
  private final static ThreadLocal<Object> refCountOwner;
  private final static ThreadLocal<AtomicInteger> refCountReenterCount;
  static {
    if (trackRefCounts) {
      stacktraces = new ConcurrentHashMap<Long, List<RefCountChangeInfo>>();
      if (trackFreedRefCounts) {
        freedStacktraces = new ConcurrentHashMap<Long, List<RefCountChangeInfo>>();
      } else {
        freedStacktraces = null;
      }
      refCountOwner = new ThreadLocal<Object>();
      refCountReenterCount = new ThreadLocal<AtomicInteger>();
    } else {
      stacktraces = null;
      freedStacktraces = null;
      refCountOwner = null;
      refCountReenterCount = null;
    }
  }
  
  public static boolean trackReferenceCounts() {
    return trackRefCounts;
  }
  public static boolean trackFreedReferenceCounts() {
    return trackFreedRefCounts;
  }
  public static void setReferenceCountOwner(Object owner) {
    if (trackReferenceCounts()) {
      if (refCountOwner.get() != null) {
        AtomicInteger ai = refCountReenterCount.get();
        if (owner != null) {
          ai.incrementAndGet();
        } else {
          if (ai.decrementAndGet() <= 0) {
            refCountOwner.set(null);
            ai.set(0);
          }
        }
      } else {
        AtomicInteger ai = refCountReenterCount.get();
        if (ai == null) {
          ai = new AtomicInteger(0);
          refCountReenterCount.set(ai);
        }
        if (owner != null) {
          ai.set(1);
        } else {
          ai.set(0);
        }
        refCountOwner.set(owner);
      }
    }
  }
  public static Object createReferenceCountOwner() {
    Object result = null;
    if (trackReferenceCounts()) {
      result = new Object();
      setReferenceCountOwner(result);
    }
    return result;
  }
  
  @SuppressWarnings("serial")
  public static class RefCountChangeInfo extends Throwable {
    private final String threadName;
    private final int rc;
    private final Object owner;
    private int dupCount;
    
    public RefCountChangeInfo(boolean decRefCount, int rc) {
      super(decRefCount ? "FREE" : "USED");
      this.threadName = Thread.currentThread().getName();
      this.rc = rc;
      this.owner = refCountOwner.get();
    }
    
    public Object getOwner() {
      return this.owner;
    }

    @Override
    public String toString() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(64*1024);
      PrintStream ps = new PrintStream(baos);
      ps.print(this.getMessage());
      ps.print(" rc=");
      ps.print(this.rc);
      if (this.dupCount > 0) {
        ps.print(" dupCount=");
        ps.print(this.dupCount);
      }
      ps.print(" by ");
      ps.print(this.threadName);
      if (this.owner != null) {
        ps.print(" owner=");
        ps.print(this.owner.getClass().getName());
        ps.print("@");
        ps.print(System.identityHashCode(this.owner));
      }
      ps.println(": ");
      StackTraceElement[] trace = getStackTrace();
      // skip the initial elements from SimpleMemoryAllocatorImpl
      int skip=0;
      for (int i=0; i < trace.length; i++) {
        if (!trace[i].getClassName().contains("SimpleMemoryAllocatorImpl")) {
          skip = i;
          break;
        }
      }
      for (int i=skip; i < trace.length; i++) {
        ps.println("\tat " + trace[i]);
      }
      ps.flush();
      return baos.toString();
    }
    
    public boolean isDuplicate(RefCountChangeInfo other) {
      if (!getMessage().equals(other.getMessage())) return false;
      String trace = getStackTraceString();
      String traceOther = other.getStackTraceString();
      if (trace.hashCode() != traceOther.hashCode()) return false;
      if (trace.equals(traceOther)) {
        this.dupCount++;
        return true;
      } else {
        return false;
      }
    }

    private String stackTraceString;
    private String getStackTraceString() {
      String result = this.stackTraceString;
      if (result == null) {
        StringPrintWriter spr = new StringPrintWriter();
        printStackTrace(spr);
        result = spr.getBuilder().toString();
        this.stackTraceString = result;
      }
      return result;
    }
  }
  
  private static final Object SKIP_REF_COUNT_TRACKING = new Object();
  
  public static void skipRefCountTracking() {
    setReferenceCountOwner(SKIP_REF_COUNT_TRACKING);
  }
  public static void unskipRefCountTracking() {
    setReferenceCountOwner(null);
  }
  
  private static void refCountChanged(Long address, boolean decRefCount, int rc) {
    final Object owner = refCountOwner.get();
    if (owner == SKIP_REF_COUNT_TRACKING) {
      return;
    }
    List<RefCountChangeInfo> list = stacktraces.get(address);
    if (list == null) {
      List<RefCountChangeInfo> newList = new ArrayList<RefCountChangeInfo>();
      List<RefCountChangeInfo> old = stacktraces.putIfAbsent(address, newList);
      if (old == null) {
        list = newList;
      } else {
        list = old;
      }
    }
    if (decRefCount) {
      if (owner != null) {
        synchronized (list) {
          for (int i=0; i < list.size(); i++) {
            RefCountChangeInfo info = list.get(i);
            if (owner instanceof RegionEntry) {
              // use identity comparison on region entries since sqlf does some wierd stuff in the equals method
              if (owner == info.owner) {
                if (info.dupCount > 0) {
                  info.dupCount--;
                } else {
                  list.remove(i);
                }
                return;
              }
            } else if (owner.equals(info.owner)) {
              if (info.dupCount > 0) {
                info.dupCount--;
              } else {
                list.remove(i);
              }
              return;
            }
          }
        }
      }
    }
    if (list == LOCKED) {
      debugLog("refCount " + (decRefCount ? "deced" : "inced") + " after orphan detected for @" + Long.toHexString(address), true);
      return;
    }
    RefCountChangeInfo info = new RefCountChangeInfo(decRefCount, rc);
    synchronized (list) {
//      if (list.size() == 16) {
//        debugLog("dumping @" + Long.toHexString(address) + " history=" + list, false);
//        list.clear();
//      }
      for (RefCountChangeInfo e: list) {
        if (e.isDuplicate(info)) {
          // No need to add it
          return;
        }
      }
      list.add(info);
    }
  }
  
  private static List<RefCountChangeInfo> LOCKED = Collections.emptyList();
  
  public static List<RefCountChangeInfo> getRefCountInfo(long address) {
    if (!trackReferenceCounts()) return null;
    List<RefCountChangeInfo> result = stacktraces.get(address);
    while (result != null && !stacktraces.replace(address, result, LOCKED)) {
      result = stacktraces.get(address);
    }
    return result;
  }
  public static List<RefCountChangeInfo> getFreeRefCountInfo(long address) {
    if (!trackReferenceCounts() || !trackFreedReferenceCounts()) return null;
    return freedStacktraces.get(address);
  }
  
  public static void freeRefCountInfo(Long address) {
    if (!trackReferenceCounts()) return;
    List<RefCountChangeInfo> freedInfo = stacktraces.remove(address);
    if (freedInfo == LOCKED) {
      debugLog("freed after orphan detected for @" + Long.toHexString(address), true);
    } else if (trackFreedReferenceCounts()) {
      if (freedInfo != null) {
        freedStacktraces.put(address, freedInfo);
      } else {
        freedStacktraces.remove(address);
      }
    }
  }
  
  /** Used by tests to stress off-heap memory compaction.
   * 
   */
  public static void forceCompaction() {
    getAllocator().freeList.compact(0);
  }
  
  private static final List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<LifecycleListener>();
  
  /**
   * Adds a LifecycleListener.
   * @param listener the instance to add
   */
  public static void addLifecycleListener(LifecycleListener listener) {
    lifecycleListeners.add(listener);
  }
  
  /**
   * Removes a LifecycleListener. Does nothing if the instance has not been added.
   * @param listener the instance to remove
   */
  public static void removeLifecycleListener(LifecycleListener listener) {
    lifecycleListeners.remove(listener);
  }
  
  static void invokeAfterCreate(SimpleMemoryAllocatorImpl allocator) {
    for (Iterator<LifecycleListener> iter = lifecycleListeners.iterator(); iter.hasNext();) {
      LifecycleListener listener = iter.next();
      listener.afterCreate(allocator);
    }
  }
  
  static void invokeAfterReuse(SimpleMemoryAllocatorImpl allocator) {
    for (Iterator<LifecycleListener> iter = lifecycleListeners.iterator(); iter.hasNext();) {
      LifecycleListener listener = iter.next();
      listener.afterReuse(allocator);
    }
  }
  
  static  void invokeBeforeClose(SimpleMemoryAllocatorImpl allocator) {
    for (Iterator<LifecycleListener> iter = lifecycleListeners.iterator(); iter.hasNext();) {
      LifecycleListener listener = iter.next();
      listener.beforeClose(allocator);
    }
  }
  
  /**
   * Used by tests to get notifications about the lifecycle of a 
   * SimpleMemoryAllocatorImpl.
   * 
   * @author Kirk Lund
   */
  public interface LifecycleListener {
    /**
     * Callback is invoked after creating a new SimpleMemoryAllocatorImpl. 
     * 
     * Create occurs during the first initialization of an 
     * InternalDistributedSystem within the JVM.
     * 
     * @param allocator the instance that has just been created
     */
    public void afterCreate(SimpleMemoryAllocatorImpl allocator);
    /**
     * Callback is invoked after reopening an existing SimpleMemoryAllocatorImpl 
     * for reuse. 
     * 
     * Reuse occurs during any intialization of an 
     * InternalDistributedSystem after the first one was connected and then
     * disconnected within the JVM.
     * 
     * @param allocator the instance that has just been reopened for reuse
     */
    public void afterReuse(SimpleMemoryAllocatorImpl allocator);
    /**
     * Callback is invoked before closing the SimpleMemoryAllocatorImpl
     * 
     * Close occurs after the InternalDistributedSystem and DistributionManager 
     * have completely disconnected. 
     * 
     * @param allocator the instance that is about to be closed
     */
    public void beforeClose(SimpleMemoryAllocatorImpl allocator);
  }
}
