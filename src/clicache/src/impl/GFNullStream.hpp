/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"

using namespace System;
using namespace System::IO;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      ref class GFNullStream : public Stream
      {
      public:

        virtual property bool CanSeek { bool get() override { return false; } }
        virtual property bool CanRead { bool get() override { return false; } }
        virtual property bool CanWrite { bool get() override { return true; } }

        virtual void Close() override { Stream::Close(); }

        virtual property int64_t Length
        {
          int64_t get() override
          {
            return (int64_t) m_position;
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
          throw gcnew System::NotSupportedException("Seek not supported by GFNullStream");
          /*
          int actual = 0;
          switch (origin)
          {
          case SeekOrigin::Begin:
            actual = (int) offset;
            m_position = (int) actual;
            break;

          case SeekOrigin::Current:
            actual = (int) offset;
            m_position += (int) actual;
            break;

          case SeekOrigin::End:
            actual = (int) offset;
            m_position += (int) actual;
            break;
          }
          // Seek is meaningless here?
          return m_position;
          */
        }

        virtual void SetLength(int64_t value) override { /* do nothing */ }

        virtual void Write(array<Byte> ^ buffer, int offset, int count) override
        {
          m_position += count;
        }

        virtual void WriteByte(unsigned char value) override
        {
          m_position++;
        }

        virtual int Read(array<Byte> ^ buffer, int offset, int count) override
        {
          throw gcnew System::NotSupportedException("Seek not supported by GFNullStream");
          /*
          int actual = count;
          m_position += actual;
          return actual;
          */
        }

        virtual void Flush() override { /* do nothing */ }

      private:
        int m_position;
      };
    }
  }
}
}