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

#pragma once

#include "../gf_defs.hpp"
#include "../DataOutput.hpp"
#include "../ExceptionTypes.hpp"

using namespace System;
using namespace System::IO;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      ref class GFDataOutputStream : public Stream
      {
      public:

        GFDataOutputStream(DataOutput^ output)
        {
          m_buffer = output;
        }

        virtual property bool CanSeek { bool get() override { return false; } }
        virtual property bool CanRead { bool get() override { return false; } }
        virtual property bool CanWrite { bool get() override { return true; } }

        virtual void Close() override { Stream::Close(); }

        virtual property int64_t Length
        {
          int64_t get() override
          {
            return (int64_t) m_buffer->BufferLength;
          }
        }

        virtual property int64_t Position
        {
          int64_t get() override
          {
            return (int64_t) m_position;
          }

          void set(int64_t value) override
          {
            m_position = (int) value;
          }
        }

        virtual int64_t Seek(int64_t offset, SeekOrigin origin) override
        {
          throw gcnew System::NotSupportedException("Seek not supported by GFDataOutputStream");
        }

        virtual void SetLength(int64_t value) override
        { 
          //TODO: overflow check
          //m_buffer->NativePtr->ensureCapacity((uint32_t)value);
        }

        virtual void Write(array<Byte> ^ buffer, int offset, int count) override
        {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          /*
          array<Byte> ^ chunk = gcnew array<Byte>(count);
          array<Byte>::ConstrainedCopy(buffer, offset, chunk, 0, count);
          m_buffer->WriteBytesOnly(chunk, count);
          */
          //pin_ptr<const Byte> pin_bytes = &buffer[offset];
          //m_buffer->NativePtr->writeBytesOnly((const uint8_t*)pin_bytes, count);
          m_buffer->WriteBytesOnly(buffer, count, offset);
          m_position += count;
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }

        virtual void WriteByte(unsigned char value) override
        {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          m_buffer->WriteByte(value);
          m_position++;
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }

        virtual int Read(array<Byte> ^ buffer, int offset, int count) override
        {
          throw gcnew System::NotSupportedException("Read not supported by GFDataOutputStream");
        }

        virtual void Flush() override { /* do nothing */ }

      private:
        int m_position;
        DataOutput ^ m_buffer;
      };
    }
  }
}
}
