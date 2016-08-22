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
package com.gemstone.gemfire.internal.io;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for CompositeOutputStream.
 * 
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class CompositeOutputStreamJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testNewCompositeOutputStreamWithNoStreams() throws IOException {
    final CompositeOutputStream cos = new CompositeOutputStream();
    assertTrue(cos.isEmpty());
    assertEquals(0, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testMockOutputStream() throws IOException {
    final OutputStream mockOutputStream = mockContext.mock(OutputStream.class, "OutputStream");

    mockContext.checking(new Expectations() {{
      oneOf(mockOutputStream).write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
      oneOf(mockOutputStream).write(new byte[]{0,1});
      oneOf(mockOutputStream).write(9);
      oneOf(mockOutputStream).flush();
      oneOf(mockOutputStream).close();
    }});

    mockOutputStream.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    mockOutputStream.write(new byte[]{0,1});
    mockOutputStream.write(9);
    mockOutputStream.flush();
    mockOutputStream.close();
  }
  
  @Test
  public void testNewCompositeOutputStreamWithOneStream() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final CompositeOutputStream cos = new CompositeOutputStream(streamOne);

    assertFalse(cos.isEmpty());
    assertEquals(1, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }

  @Test
  public void testNewCompositeOutputStreamWithTwoStreams() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final Sequence seqStreamTwo = mockContext.sequence("seqStreamTwo");
    final OutputStream streamTwo = mockContext.mock(OutputStream.class, "streamTwo");
    mockContext.checking(new Expectations() {{
      oneOf(streamTwo).write(2); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(3); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(4); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(0); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(1); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(9); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).close(); inSequence(seqStreamTwo);
    }});

    final CompositeOutputStream cos = new CompositeOutputStream(streamOne, streamTwo);

    assertFalse(cos.isEmpty());
    assertEquals(2, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testAddOutputStreamWithTwoStreams() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final Sequence seqStreamTwo = mockContext.sequence("seqStreamTwo");
    final OutputStream streamTwo = mockContext.mock(OutputStream.class, "streamTwo");
    mockContext.checking(new Expectations() {{
      oneOf(streamTwo).write(2); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(3); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(4); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(0); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(1); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(9); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).close(); inSequence(seqStreamTwo);
    }});
    
    final Sequence seqStreamThree = mockContext.sequence("seqStreamThree");
    final OutputStream streamThree = mockContext.mock(OutputStream.class, "streamThree");
    mockContext.checking(new Expectations() {{
      oneOf(streamThree).write(2); inSequence(seqStreamThree);
      oneOf(streamThree).write(3); inSequence(seqStreamThree);
      oneOf(streamThree).write(4); inSequence(seqStreamThree);
      oneOf(streamThree).write(0); inSequence(seqStreamThree);
      oneOf(streamThree).write(1); inSequence(seqStreamThree);
      oneOf(streamThree).write(9); inSequence(seqStreamThree);
      oneOf(streamThree).flush(); inSequence(seqStreamThree);
      oneOf(streamThree).flush(); inSequence(seqStreamThree);
      oneOf(streamThree).close(); inSequence(seqStreamThree);
    }});
    
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne, streamTwo);

    assertFalse(cos.isEmpty());
    assertEquals(2, cos.size());
    
    cos.addOutputStream(streamThree);
    assertEquals(3, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testAddOutputStreamWithOneStream() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final Sequence seqStreamTwo = mockContext.sequence("seqStreamTwo");
    final OutputStream streamTwo = mockContext.mock(OutputStream.class, "streamTwo");
    mockContext.checking(new Expectations() {{
      oneOf(streamTwo).write(2); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(3); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(4); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(0); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(1); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(9); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).close(); inSequence(seqStreamTwo);
    }});
    
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne);

    assertFalse(cos.isEmpty());
    assertEquals(1, cos.size());
    
    cos.addOutputStream(streamTwo);
    assertEquals(2, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testAddOneOutputStreamWhenEmpty() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final CompositeOutputStream cos = new CompositeOutputStream();

    assertTrue(cos.isEmpty());
    assertEquals(0, cos.size());
    
    cos.addOutputStream(streamOne);
    assertFalse(cos.isEmpty());
    assertEquals(1, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testAddTwoOutputStreamsWhenEmpty() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final Sequence seqStreamTwo = mockContext.sequence("seqStreamTwo");
    final OutputStream streamTwo = mockContext.mock(OutputStream.class, "streamTwo");
    mockContext.checking(new Expectations() {{
      oneOf(streamTwo).write(2); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(3); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(4); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(0); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(1); inSequence(seqStreamTwo);
      oneOf(streamTwo).write(9); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).flush(); inSequence(seqStreamTwo);
      oneOf(streamTwo).close(); inSequence(seqStreamTwo);
    }});
    
    final CompositeOutputStream cos = new CompositeOutputStream();

    assertTrue(cos.isEmpty());
    assertEquals(0, cos.size());
    
    cos.addOutputStream(streamOne);
    cos.addOutputStream(streamTwo);
    assertFalse(cos.isEmpty());
    assertEquals(2, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testRemoveOutputStreamWithTwoStreams() throws IOException {
    final Sequence seqStreamOne = mockContext.sequence("seqStreamOne");
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      oneOf(streamOne).write(2); inSequence(seqStreamOne);
      oneOf(streamOne).write(3); inSequence(seqStreamOne);
      oneOf(streamOne).write(4); inSequence(seqStreamOne);
      oneOf(streamOne).write(0); inSequence(seqStreamOne);
      oneOf(streamOne).write(1); inSequence(seqStreamOne);
      oneOf(streamOne).write(9); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).flush(); inSequence(seqStreamOne);
      oneOf(streamOne).close(); inSequence(seqStreamOne);
    }});

    final OutputStream streamTwo = mockContext.mock(OutputStream.class, "streamTwo");
    mockContext.checking(new Expectations() {{
      never(streamTwo).write(2);
      never(streamTwo).write(3);
      never(streamTwo).write(4);
      never(streamTwo).write(0);
      never(streamTwo).write(1);
      never(streamTwo).write(9);
      never(streamTwo).flush();
      never(streamTwo).flush();
      never(streamTwo).close();
    }});

    final CompositeOutputStream cos = new CompositeOutputStream(streamOne, streamTwo);

    assertFalse(cos.isEmpty());
    assertEquals(2, cos.size());
    
    cos.removeOutputStream(streamTwo);
    
    assertFalse(cos.isEmpty());
    assertEquals(1, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }

  @Test
  public void testRemoveOutputStreamWithOneStream() throws IOException {
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      never(streamOne).write(2);
      never(streamOne).write(3);
      never(streamOne).write(4);
      never(streamOne).write(0);
      never(streamOne).write(1);
      never(streamOne).write(9);
      never(streamOne).flush();
      never(streamOne).flush();
      never(streamOne).close();
    }});

    final CompositeOutputStream cos = new CompositeOutputStream(streamOne);

    assertFalse(cos.isEmpty());
    assertEquals(1, cos.size());
    
    cos.removeOutputStream(streamOne);
    
    assertTrue(cos.isEmpty());
    assertEquals(0, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
  
  @Test
  public void testRemoveOutputStreamWhenEmpty() throws IOException {
    final OutputStream streamOne = mockContext.mock(OutputStream.class, "streamOne");
    mockContext.checking(new Expectations() {{
      never(streamOne).write(2);
      never(streamOne).write(3);
      never(streamOne).write(4);
      never(streamOne).write(0);
      never(streamOne).write(1);
      never(streamOne).write(9);
      never(streamOne).flush();
      never(streamOne).flush();
      never(streamOne).close();
    }});

    final CompositeOutputStream cos = new CompositeOutputStream();

    assertTrue(cos.isEmpty());
    assertEquals(0, cos.size());
    
    cos.removeOutputStream(streamOne);
    
    assertTrue(cos.isEmpty());
    assertEquals(0, cos.size());
    
    cos.write(new byte[]{0,1,2,3,4,5,6,7,8,9}, 2, 3);
    cos.write(new byte[]{0,1});
    cos.write(9);
    cos.flush();
    cos.close();
  }
}
