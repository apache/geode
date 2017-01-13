using System;
using System.Collections.Generic;
using System.Reflection;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
    using NUnit.Framework;
    using GemStone.GemFire.DUnitFramework;
    using GemStone.GemFire.Cache.Generic;

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