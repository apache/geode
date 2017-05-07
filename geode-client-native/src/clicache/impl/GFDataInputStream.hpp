/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "../DataInputM.hpp"

using namespace System;
using namespace System::IO;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      ref class GFDataInputStream : public Stream
      {
      public:

        GFDataInputStream(DataInput^ input)
        {
          m_buffer = input;
        }

        virtual property bool CanSeek { bool get() override { return false; } }
        virtual property bool CanRead { bool get() override { return true; } }
        virtual property bool CanWrite { bool get() override { return false; } }

        virtual void Close() override { Stream::Close(); }

        virtual property int64_t Length
        {
          int64_t get() override
          {
            return (int64_t) m_buffer->BytesRead + m_buffer->BytesRemaining;
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
          _GF_MG_EXCEPTION_TRY
          int actual = (int) m_buffer->BytesRemaining < count ?
            (int) m_buffer->BytesRemaining : count;
          if (actual > 0)
          {
            /*
            array<Byte>::ConstrainedCopy(m_buffer->ReadBytesOnly(actual), 0,
              buffer, offset, actual);
              */
           // pin_ptr<Byte> pin_buffer = &buffer[offset];
           // m_buffer->NativePtr->readBytesOnly((uint8_t*)pin_buffer, actual);
            m_buffer->ReadBytesOnly(buffer, offset, actual);
            m_position += actual;
          }
          return actual;
          _GF_MG_EXCEPTION_CATCH_ALL
        }

        virtual void Flush() override { /* do nothing */ }

        property uint32_t BytesRead
        {
          uint32_t get()
          {
            return m_buffer->BytesRead;
          }
        }

      private:
        int m_position;
        DataInput ^ m_buffer;
      };
    }
  }
}
