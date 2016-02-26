package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.offheap.MemoryBlock.State;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TinyMemoryBlockJUnitTest {

  private SimpleMemoryAllocatorImpl ma;
  private OutOfOffHeapMemoryListener ooohml;
  private OffHeapMemoryStats stats;

  private AddressableMemoryChunk[] slabs = {
      new UnsafeMemoryChunk((int)OffHeapStorage.MIN_SLAB_SIZE),
      new UnsafeMemoryChunk((int)OffHeapStorage.MIN_SLAB_SIZE),
      new UnsafeMemoryChunk((int)OffHeapStorage.MIN_SLAB_SIZE)
  };

  private static class TestableFreeListManager extends FreeListManager {
    TestableFreeListManager(SimpleMemoryAllocatorImpl ma, final AddressableMemoryChunk[] slabs) {
      super (ma, slabs);
    }
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public JUnitSoftAssertions softly = new JUnitSoftAssertions();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    ooohml = mock(OutOfOffHeapMemoryListener.class);
    stats = mock(OffHeapMemoryStats.class);
    ma = (SimpleMemoryAllocatorImpl) SimpleMemoryAllocatorImpl.createForUnitTest(ooohml, stats, slabs);
  }

  @After
  public void tearDown() throws Exception {
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
  }

  protected Object getValue() {
    return Long.valueOf(Long.MAX_VALUE);
  }

  private MemoryChunkWithRefCount createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    MemoryChunkWithRefCount chunk = (MemoryChunkWithRefCount) ma.allocateAndInitialize(v, isSerialized, isCompressed);
    return chunk;
  }

  private MemoryChunkWithRefCount createValueAsSerializedStoredObject(Object value, boolean isCompressed) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;

    MemoryChunkWithRefCount createdObject = createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
    return createdObject;
  }

  private byte[] convertValueToByteArray(Object value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) value).array();
  }

  private MemoryChunkWithRefCount createValueAsUnserializedStoredObject(Object value, boolean isCompressed) {
    byte[] valueInByteArray;
    if (value instanceof Long) {
      valueInByteArray = convertValueToByteArray(value);
    } else {
      valueInByteArray = (byte[]) value;
    }

    boolean isSerialized = false;

    MemoryChunkWithRefCount createdObject = createChunk(valueInByteArray, isSerialized, isCompressed);
    return createdObject;
  }

   @Test
  public void constructorReturnsNonNullMemoryBlock() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb).isNotNull();
  }

  @Test
  public void stateAlwaysEqualsDeallocated() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.getState()).isEqualTo(State.DEALLOCATED);
  }

  @Test
  public void getMemoryAddressReturnsAddressBlockWasContructedFrom() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.getMemoryAddress()).isEqualTo(slabs[0].getMemoryAddress());
  }

  @Test
  public void sizeOfBlockConstructedFromRawMemoryReturnsZero() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.getBlockSize()).isEqualTo(0);
  }

  @Test
  public void sizeOfBlockConstructedFromChunkReturnsChunkSize() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(new ObjectChunk(slabs[0].getMemoryAddress(), slabs[0].getSize()).getMemoryAddress(), 0);
    softly.assertThat(mb.getBlockSize()).isEqualTo(slabs[0].getSize());
  }

  @Test
  public void getNextBlockThrowsExceptionForFragment() {
    expectedException.expect(UnsupportedOperationException.class);

    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(new ObjectChunk(slabs[0].getMemoryAddress(), slabs[0].getSize()).getMemoryAddress(), 0);
    mb.getNextBlock();
    fail("getNextBlock failed to throw UnsupportedOperationException");
  }

  @Test
  public void getFreeListIdReturnsIdBlockWasConstructedFrom() {
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(new ObjectChunk(slabs[0].getMemoryAddress(), slabs[0].getSize()).getMemoryAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(new ObjectChunk(slabs[1].getMemoryAddress(), slabs[1].getSize()).getMemoryAddress(), 1);
    softly.assertThat(mb0.getFreeListId()).isEqualTo(0);
    softly.assertThat(mb1.getFreeListId()).isEqualTo(1);
  }

  @Test
  public void getRefCountReturnsZero() {
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(new ObjectChunk(slabs[0].getMemoryAddress(), slabs[0].getSize()).getMemoryAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(new ObjectChunk(slabs[1].getMemoryAddress(), slabs[1].getSize()).getMemoryAddress(), 1);
    softly.assertThat(mb0.getRefCount()).isEqualTo(0);
    softly.assertThat(mb1.getRefCount()).isEqualTo(0);
  }

  @Test
  public void getDataTypeReturnsNA() {
    Object obj = getValue();
    boolean compressed = false;

    MemoryChunkWithRefCount storedObject0 = createValueAsSerializedStoredObject(obj, compressed);
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock)storedObject0).getMemoryAddress(), 0);
    softly.assertThat(mb.getDataType()).isEqualTo("N/A");
  }

  @Test
  public void getDataValueReturnsNull() {
    Object obj = getValue();
    boolean compressed = false;

    MemoryChunkWithRefCount storedObject0 = createValueAsSerializedStoredObject(obj, compressed);
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock)storedObject0).getMemoryAddress(), 0);
    softly.assertThat(mb.getDataValue()).isNull();
  }

  @Test
  public void isSerializedReturnsFalse() {
    Object obj = getValue();
    boolean compressed = false;

    MemoryChunkWithRefCount storedObject0 = createValueAsSerializedStoredObject(obj, compressed);
    MemoryChunkWithRefCount storedObject1 = createValueAsUnserializedStoredObject(obj, compressed);
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock)storedObject0).getMemoryAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock)storedObject1).getMemoryAddress(), 0);
    softly.assertThat(mb0.isSerialized()).isFalse();
    softly.assertThat(mb1.isSerialized()).isFalse();
  }

  @Test
  public void isCompressedReturnsFalse() {
    Object obj = getValue();
    boolean compressed = false;
    MemoryChunkWithRefCount storedObject0 = createValueAsUnserializedStoredObject(obj, compressed);
    MemoryChunkWithRefCount storedObject1 = createValueAsUnserializedStoredObject(obj, compressed = true);
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock)storedObject0).getMemoryAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock)storedObject1).getMemoryAddress(), 0);
    softly.assertThat(mb0.isCompressed()).isFalse();
    softly.assertThat(mb1.isCompressed()).isFalse();
  }

  @Test
  public void equalsComparesAddressesOfTinyMemoryBlocks() {
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    MemoryBlock mb2 = new TestableFreeListManager.TinyMemoryBlock(slabs[1].getMemoryAddress(), 0);
    softly.assertThat(mb0.equals(mb1)).isTrue();
    softly.assertThat(mb0.equals(mb2)).isFalse();
  }

  @Test
  public void equalsNotTinyMemoryBlockReturnsFalse() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.equals(slabs[0])).isFalse();
  }

  @Test
  public void hashCodeReturnsHashOfUnderlyingMemory() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.hashCode()).isEqualTo(new ObjectChunk(slabs[0].getMemoryAddress(), slabs[0].getSize()).hashCode());
  }

}
