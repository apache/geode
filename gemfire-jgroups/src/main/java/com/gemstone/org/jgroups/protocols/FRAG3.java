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
package com.gemstone.org.jgroups.protocols;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;

/**
 * FRAG3 is a version of FRAG2 that is used below GMS to allow for larger
 * membership view messages than will fit in a single packet.
 * 
 * @author bruces
 *
 */
public class FRAG3 extends FRAG2 {

  @Override // GemStoneAddition  
  public String getName() {
      return "FRAG3";
  }

  @Override
  public void down(Event evt) {
    if (evt.getType() == Event.MSG) {
      Message msg=(Message)evt.getArg();
      if (msg.getHeader("FRAG2") != null) {
        // the message is already fragmented - don't mess with it
        passDown(evt);
        return;
      }
    }
    super.down(evt);
  }
}
