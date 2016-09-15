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
package org.apache.geode.internal;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface to implement if your class supports sending itself directly to a DataOutput
 * during serialization.
 * Note that you are responsible for sending all the bytes that represent your instance,
 * even bytes describing your class name if those are required.
 * 
 * @since GemFire 6.6
 */
public interface Sendable {
  /**
   * Take all the bytes in the object and write them to the data output. It needs
   * to be written in the GemFire wire format so that it will deserialize correctly 
   * if DataSerializer.readObject is called.
   * 
   * @param out
   *          the data output to send this object to
   * @throws IOException
   */
  void sendTo(DataOutput out) throws IOException;
}
