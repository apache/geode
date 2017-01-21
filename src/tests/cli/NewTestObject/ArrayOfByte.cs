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
using System.IO;
namespace Apache.Geode.Client.Tests
{
  using Apache.Geode.Client;
  public class ArrayOfByte
  {
    private static DataOutput dos = new DataOutput();
    public static byte[] Init(Int32 size, bool encodeKey, bool encodeTimestamp)
    {
      if (encodeKey)
      {
        //using (DataOutput dos = new DataOutput())
        //{
          dos.Reset();
          try
          {
            Int32 index = 1234;
            dos.WriteInt32(index);
            //dos.Write(index);
            if (encodeTimestamp)
            {
              DateTime startTime = DateTime.Now;
              long timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
              //dos.Write(timestamp);
              dos.WriteInt64(timestamp);
            }
          }
          catch (Exception e)
          {
            //FwkException("Unable to write to stream {0}", e.Message);
            throw new Exception(e.Message);
          }

          /*
          byte[] b = baos.GetBuffer();
          if (b.Length > size)
          {
            throw new Exception("Unable to encode into byte array of size");
          }
          byte[] result = new byte[size];
          System.Array.Copy(b, 0, result, 0, b.Length);
          return CacheableBytes.Create(result);
          //return result;
           */

          Int32 bufSize = size;
          byte[] buf = new byte[bufSize];
          
            for (int i = 0; i < bufSize; i++)
            {
              buf[i] = 123;
            }
            //buf.CopyTo(dos.GetBuffer(), (int)dos.BufferLength);
            System.Array.Copy(dos.GetBuffer(), 0, buf, 0, dos.BufferLength);
            //Console.WriteLine("rjk: size of byte array is {0} , dataoutput lenght {1} and object is {2}", sizeof(byte) * buf.Length, dos.BufferLength, buf.ToString());
            Int32 rsiz = (bufSize <= 20) ? bufSize : 20;
            return buf;
          
        //}
        
      }
      else if (encodeTimestamp)
      {
        throw new Exception("Should not happen");
        //FwkException("Should not happen");
      }
      else
      {
        return new byte[size];
        //return new byte[size];
      }
    }

    public static long GetTimestamp(byte[] bytes)
    {
      if (bytes == null)
      {
        throw new IllegalArgumentException("the bytes arg was null");
      }
      //Console.WriteLine("rjk: CacheableBytes value = {0} and length = {1}", bytes.Value, bytes.Length);

      //using (DataInput di = new DataInput(bytes.Value, bytes.Length))
      DataInput di = new DataInput(bytes, bytes.Length);
      //{
        try
        {
          //Int32 index = di.ReadInt32();
          di.AdvanceCursor(4);
          long timestamp = di.ReadInt64();
          if (timestamp == 0)
          {
            throw new Exception("Object is not configured to encode timestamp");
          }
          return timestamp;
        }
        catch (Exception e)
        {
          //FwkException("Unable to read from stream {0}", e.Message);
          throw new Exception(e.Message);
        }
      //}
    }

    public static void ResetTimestamp(byte[] bytes)
    {
      DataInput di = new DataInput(bytes, bytes.Length);
      Int32 index;
      try
      {
        index = di.ReadInt32();
        long timestamp = di.ReadInt64();
        if (timestamp == 0)
        {
          return;
        }
      }
      catch (Exception e)
      {
        throw new Exception(e.Message);
        //FwkException("Unable to read from stream {0}", e.Message);
      }
      DataOutput dos = new DataOutput();
      try
      {
        dos.WriteInt32(index);
        DateTime startTime = DateTime.Now;
        long timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
        dos.WriteInt64(timestamp);
      }
      catch (Exception e)
      {
        throw new Exception(e.Message);
        //FwkException("Unable to write to stream {0}", e.Message);
      }
    }
    /*
    public override string ToString()
    {
      string portStr = string.Format("ArrayOfBytes [Index={0} ", index);
      return portStr;
    }*/
  }
}
