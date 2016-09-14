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
package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList651;
import org.apache.geode.internal.cache.tier.sockets.SerializedObjectPartList;

/**
 * A version of GetAll which in which the response contains the objects in
 * serialized byte array form, so that they can be separated into individual
 * values without being deserialized. The standard GetAll requires us to
 * deserialize the value of every object.
 * 
 * [bruce] this class is superceded by GetAll70, which merges GetAll651 and
 * GetAllForRI
 * 
 * 
 */
public class GetAllForRI extends GetAll651 {
  private final static GetAllForRI singleton = new GetAllForRI();
  
  public static Command getCommand() {
    return singleton;
  }
  
  protected GetAllForRI() {
  }

  @Override
  protected ObjectPartList651 getObjectPartsList(boolean includeKeys) {
    return new SerializedObjectPartList(maximumChunkSize, includeKeys);
  }
  
  

}
