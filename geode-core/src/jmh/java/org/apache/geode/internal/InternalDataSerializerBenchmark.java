/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DSCODE;


/**
 * Test throughput of InternalDataSerializer.readString
 */

@State(Scope.Thread)
@Fork(1)
public class InternalDataSerializerBenchmark {

  private final ByteArrayDataInput dataInput = new ByteArrayDataInput();
  private byte[] serializedBytes;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeString("12345678901234567890123456789012345", hdos);
    byte[] bytes = hdos.toByteArray();
    if (bytes[0] != DSCODE.STRING_BYTES.toByte()) {
      throw new IllegalStateException(
          "expected first byte to be " + DSCODE.STRING_BYTES.toByte() + " but it was " + bytes[0]);
    }
    serializedBytes = Arrays.copyOfRange(bytes, 1, bytes.length);
  }

  @Benchmark
  @Measurement(iterations = 10)
  @Warmup(iterations = 3)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public String readStringBenchmark() throws IOException {
    dataInput.initialize(serializedBytes, Version.CURRENT_ORDINAL);
    String result = InternalDataSerializer.readString(dataInput, DSCODE.STRING_BYTES.toByte());
    return result;
  }

}
