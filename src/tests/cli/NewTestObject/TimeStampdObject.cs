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

using System;

namespace Apache.Geode.Client.Tests
{
  using Apache.Geode.Client;
  public class TimeStampdObject
    : IGFSerializable
  {

    public virtual UInt32 ObjectSize
    {
      get
      {
        return 0;
      }
    }
    public virtual UInt32 ClassId
    {
      get
      {
        return 0;
      }
    }
    public virtual IGFSerializable FromData(DataInput input)
    {
      return this;
    }
    public virtual void ToData(DataOutput output)
    {
    }
    public virtual long GetTimestamp()
    {
      return 0;
    }
    public virtual void ResetTimestamp() { }
    
     
  }
}
