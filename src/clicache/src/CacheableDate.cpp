/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/



//#include "gf_includes.hpp"
#include "CacheableDate.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "Log.hpp"
#include "GemFireClassIds.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      CacheableDate::CacheableDate(DateTime dateTime)
        : m_dateTime(dateTime),m_hashcode(0)
      {

        // Round off dateTime to the nearest millisecond.
        int64_t ticksToAdd = m_dateTime.Ticks % TimeSpan::TicksPerMillisecond;
        ticksToAdd = (ticksToAdd >= (TimeSpan::TicksPerMillisecond / 2) ?
          (TimeSpan::TicksPerMillisecond - ticksToAdd) : -ticksToAdd);
        m_dateTime = m_dateTime.AddTicks(ticksToAdd);
        
      }

      void CacheableDate::ToData(DataOutput^ output)
      {
        //put as universal time
        TimeSpan epochSpan = m_dateTime.ToUniversalTime() - EpochTime;
        int64_t millisSinceEpoch =
          epochSpan.Ticks / TimeSpan::TicksPerMillisecond;
        output->WriteInt64(millisSinceEpoch);

        //Log::Fine("CacheableDate::Todata time " + m_dateTime.Ticks);
      }

      IGFSerializable^ CacheableDate::FromData(DataInput^ input)
      {
        DateTime epochTime = EpochTime;
        int64_t millisSinceEpoch = input->ReadInt64();
        m_dateTime = epochTime.AddTicks(
          millisSinceEpoch * TimeSpan::TicksPerMillisecond);
        m_dateTime = m_dateTime.ToLocalTime();
        //Log::Fine("CacheableDate::Fromadata time " + m_dateTime.Ticks);
        return this;
      }

      uint32_t CacheableDate::ObjectSize::get()
      { 
        return (uint32_t)sizeof(DateTime); 
      }

      uint32_t CacheableDate::ClassId::get()
      {
        return GemFireClassIds::CacheableDate;
      }

      String^ CacheableDate::ToString()
      {
        return m_dateTime.ToString(
          System::Globalization::CultureInfo::CurrentCulture);
      }

      int32_t CacheableDate::GetHashCode()
      {
        if (m_hashcode == 0) {
          TimeSpan epochSpan = m_dateTime - EpochTime;
          int64_t millitime =
            epochSpan.Ticks / TimeSpan::TicksPerMillisecond;
          m_hashcode =  (int) millitime ^ (int) ((int64)millitime >> 32);
        }
        return m_hashcode;
      }

      bool CacheableDate::Equals(ICacheableKey^ other)
      {
        if (other == nullptr ||
          other->ClassId != GemFireClassIds::CacheableDate) {
          return false;
        }
        return m_dateTime.Equals(static_cast<CacheableDate^>(
          other)->m_dateTime);
      }

      bool CacheableDate::Equals(Object^ obj)
      {
        CacheableDate^ otherDate =
          dynamic_cast<CacheableDate^>(obj);

        if (otherDate != nullptr) {
          return (m_dateTime == otherDate->m_dateTime);
        }
        return false;
      }
    }
  }
}
 } //namespace 

