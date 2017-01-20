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

namespace Apache.Geode.DUnitFramework
{
  public static class ServerConnection<TComm>
  {
    public static TComm Connect(string serverUrl)
    {
      if (serverUrl == null || serverUrl.Length == 0)
      {
        throw new IllegalArgException("ServerConnection::ctor: " +
          "The serverUrl cannot be null or empty!!");
      }
      TComm serverComm = (TComm)Activator.GetObject(typeof(TComm), serverUrl);
      if (serverComm == null)
      {
        throw new ServerNotFoundException("Server at URL '" + serverUrl +
          "' not found.");
      }
      return serverComm;
    }
  }
}
