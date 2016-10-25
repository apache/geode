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

import java.io.*;


/** ObjToByteArraySerializer allows an object to be serialized as a byte array
 * so that the other side sees a byte array arrive.
 *
 *  @since GemFire 5.0.2
 * 
 */
public interface ObjToByteArraySerializer extends DataOutput {
  /**
   * Serialize the given object v as a byte array
   * @throws IOException if something goes wrong during serialization
   */
  public void writeAsSerializedByteArray(Object v) throws IOException;
}
