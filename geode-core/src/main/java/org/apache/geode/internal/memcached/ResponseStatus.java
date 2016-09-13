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
package org.apache.geode.internal.memcached;

/**
 * encapsulate ResponseOpCodes for binary reply messages.
 * 
 */
public enum ResponseStatus {

  NO_ERROR {
    @Override
    public short asShort() {
      return 0x0000;
    }
  },
  KEY_NOT_FOUND {
    @Override
    public short asShort() {
      return 0x0001;
    }
  },
  KEY_EXISTS {
    @Override
    public short asShort() {
      return 0x0002;
    }
  },
  ITEM_NOT_STORED {
    @Override
    public short asShort() {
      return 0x0005;
    }
  },
  NOT_SUPPORTED {
    @Override
    public short asShort() {
      return 0x0083;
    }
  },
  INTERNAL_ERROR {
    @Override
    public short asShort() {
      return 0x0084;
    }
  };
  
  public abstract short asShort();
}
