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

package com.gemstone.gemfire.internal.offheap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GemFireChunkFactoryJUnitTest {
	
	private MemoryAllocator ma;
	
	@Before
	public void beforeClass() {
		OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
		OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
		LogWriter lw = mock(LogWriter.class);
		
		ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, 1, OffHeapStorage.MIN_SLAB_SIZE * 1, OffHeapStorage.MIN_SLAB_SIZE);
	}
	
	@After
	public void tearDown() {
	  SimpleMemoryAllocatorImpl.freeOffHeapMemory();
	}
	
    private GemFireChunk createChunk(Object value) {
        byte[] v = EntryEventImpl.serialize(value);
       
        boolean isSerialized = true;
        boolean isCompressed = false;
        
    	GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(v, isSerialized, isCompressed, GemFireChunk.TYPE);
        chunk.setSerializedValue(v);
        chunk.setCompressed(isCompressed);
        chunk.setSerialized(isSerialized);
        
        return chunk;
    }
	
	@Test
	public void factoryShouldCreateNewChunkWithGivenAddress() {
		GemFireChunk chunk = createChunk(Long.MAX_VALUE);
		
		ChunkFactory factory = new GemFireChunkFactory();
		Chunk newChunk = factory.newChunk(chunk.getMemoryAddress());
		
		assertNotNull(newChunk);
		assertEquals(GemFireChunk.class, newChunk.getClass());
		
		assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());
		
		chunk.release();
	}
	
	@Test
	public void factoryShouldCreateNewChunkWithGivenAddressAndType() {
		GemFireChunk chunk = createChunk(Long.MAX_VALUE);
		
		ChunkFactory factory = new GemFireChunkFactory();
		Chunk newChunk = factory.newChunk(chunk.getMemoryAddress(), GemFireChunk.TYPE);
		
		assertNotNull(newChunk);
		assertEquals(GemFireChunk.class, newChunk.getClass());
		
		assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());
		assertThat(newChunk.getChunkType()).isEqualTo(GemFireChunk.TYPE);
		
		chunk.release();
	}
	
	@Test
	public void shouldGetChunkTypeFromAddress() {
		byte[] v = EntryEventImpl.serialize(Long.MAX_VALUE);
	       
        boolean isSerialized = true;
        boolean isCompressed = false;
        
		GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(v, isSerialized, isCompressed, GemFireChunk.TYPE);
        chunk.setSerializedValue(v);
        chunk.setCompressed(isCompressed);
        chunk.setSerialized(isSerialized);
		
        ChunkFactory factory = new GemFireChunkFactory();
		ChunkType actualType = factory.getChunkTypeForAddress(chunk.getMemoryAddress());
		
		assertEquals(GemFireChunk.TYPE, actualType);
		
		chunk.release();
	}
	
	@Test
	public void shouldGetChunkTypeFromRawBits() {
		byte[] v = EntryEventImpl.serialize(Long.MAX_VALUE);
	       
        boolean isSerialized = true;
        boolean isCompressed = false;
        
		GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(v, isSerialized, isCompressed, GemFireChunk.TYPE);
        chunk.setSerializedValue(v);
        chunk.setCompressed(isCompressed);
        chunk.setSerialized(isSerialized);
		
        int rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + 4 /*REF_COUNT_OFFSET*/);
        
        ChunkFactory factory = new GemFireChunkFactory();
		ChunkType actualType = factory.getChunkTypeForRawBits(rawBits);
		assertEquals(GemFireChunk.TYPE, actualType);
		
		chunk.release();
	}
}
