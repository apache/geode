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
#include "../DataInput.hpp"
#include "../ExceptionTypes.hpp"

using namespace System;
using namespace System::IO;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      ref class GFDataInputStream : public Stream
      {
      public:

        GFDataInputStream(DataInput^ input)
        {
          m_buffer = input;
          m_maxSize = input->BytesRemaining;
        }

        GFDataInputStream(DataInput^ input, int maxSize)
        {
          m_buffer = input;
          m_maxSize = maxSize;
          m_buffer->AdvanceUMCursor();
          m_buffer->SetBuffer();
        }

        virtual property bool CanSeek { bool get() override { return false; } }
        virtual property bool CanRead { bool get() override { return true; } }
        virtual property bool CanWrite { bool get() override { return false; } }

        virtual void Close() override { Stream::Close(); }

        virtual property int64_t Length
        {
          int64_t get() override
          {
            //return (int64_t) m_buffer->BytesRead + m_buffer->BytesRemaining;
            return (int64_t) m_maxSize;
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
          throw gcnew System::NotSupportedException("Seek not supported by GFDataInputStream");
        }

        virtual void SetLength(int64_t value) override { /* do nothing */ }

        virtual void Write(array<Byte> ^ buffer, int offset, int count) override
        {
          throw gcnew System::NotSupportedException("Write not supported by GFDataInputStream");
        }

        virtual void WriteByte(unsigned char value) override
        {
          throw gcnew System::NotSupportedException("WriteByte not supported by GFDataInputStream");
        }

        virtual int Read(array<Byte> ^ buffer, int offset, int count) override
        {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          int bytesRemaining = m_maxSize - (int) m_buffer->BytesReadInternally;
					if(bytesRemaining == 0)
						return bytesRemaining;
          int actual =  bytesRemaining < count ? bytesRemaining : count;
					if (actual > 0)
          {
            /*
            array<Byte>::ConstrainedCopy(m_buffer->ReadBytesOnly(actual), 0,
              buffer, offset, actual);
              */
            //pin_ptr<Byte> pin_buffer = &buffer[offset];
            //m_buffer->NativePtr->readBytesOnly((uint8_t*)pin_buffer, actual);
            m_buffer->ReadBytesOnly(buffer, offset, actual);
            m_position += actual;
          }
          return actual;
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }

        virtual void Flush() override { /* do nothing */ }

        property uint32_t BytesRead
        {
          uint32_t get()
          {
            return m_buffer->BytesReadInternally;
          }
        }

      private:
        int m_position;
        int m_maxSize;
        DataInput ^ m_buffer;
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache
