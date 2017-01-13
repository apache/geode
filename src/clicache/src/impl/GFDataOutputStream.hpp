/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
