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

﻿using System;
using System.Collections.Generic;
using System.Reflection;

namespace Apache.Geode.Client.UnitTests.NewAPI
{
    using NUnit.Framework;
    using Apache.Geode.DUnitFramework;
    using Apache.Geode.Client;

    [TestFixture]
    [Category("unicast_only")]
    public class DataOutputTests : UnitTests
    {
        XmlNodeReaderWriter settings = Util.DefaultSettings;

        protected override ClientBase[] GetClients()
        {
            return null;
        }

        [Test]
        public void StringExcedesBufferCapacity()
        {

            DataOutput dataOutput = new DataOutput();

            // Chcek that native buffer is unused and get initial capacity.
            Assert.AreEqual(0, dataOutput.BufferLength);
            int bufferSize = dataOutput.GetRemainingBufferLength();

            // New string equal to buffer capacity.
            string s = "".PadRight(bufferSize, 'a');
            dataOutput.WriteUTF(s);

            // Checks native buffer capacity, remaining length should be capacity since wrapper has not flushed to native yet.
            Assert.GreaterOrEqual(dataOutput.GetRemainingBufferLength(), bufferSize + 2, "Buffer should have been resized to account for string + 2 bytes of length");
            // Forces native buffer to be updated and gets length of used buffers.
            Assert.AreEqual(bufferSize + 2, dataOutput.BufferLength, "Buffer length should be string plus 2 bytes for length.");
        }
    }
}