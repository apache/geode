/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "CacheableVector.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// A mutable <c>IGFSerializable</c> vector wrapper that can serve as
      /// a distributable object for caching. This class extends .NET generic
      /// <c>List</c> class.
      /// </summary>
      ref class CacheableLinkedList
        : public IGFSerializable
      {
        System::Collections::Generic::LinkedList<Object^>^ m_linkedList;
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableLinkedList(System::Collections::Generic::LinkedList<Object^>^ list)
        {
          m_linkedList = list;
        }


        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableLinkedList^ Create()
        {
          return gcnew CacheableLinkedList(gcnew System::Collections::Generic::LinkedList<Object^>());
        }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableLinkedList^ Create(System::Collections::Generic::LinkedList<Object^>^ list)
        {
          return gcnew CacheableLinkedList(list);
        }


        // Region: IGFSerializable Members

        /// <summary>
        /// Returns the classId of the instance being serialized.
        /// This is used by deserialization to determine what instance
        /// type to create and deserialize into.
        /// </summary>
        /// <returns>the classId</returns>
        virtual property uint32_t ClassId
        {
          virtual uint32_t get()
          {
            return GemFireClassIds::CacheableLinkedList;
          }
        }

        // Region: IGFSerializable Members

      virtual void ToData(DataOutput^ output)
      {
        if(m_linkedList != nullptr)
        {
          output->WriteArrayLen(m_linkedList->Count);
          for each (Object^ obj in m_linkedList) {
						//TODO::split
            output->WriteObject(obj);
          }
        }
        else
          output->WriteByte(0xFF);
      }

      virtual IGFSerializable^ FromData(DataInput^ input)
      {
        int len = input->ReadArrayLen();
        for( int i = 0; i < len; i++)
        {
          m_linkedList->AddLast(input->ReadObject());
        }
        return this;
      }

      /*uint32_t ObjectSize::get()
      {
        //TODO::
        uint32_t size = static_cast<uint32_t> (sizeof(CacheableVector^));
        for each (IGFSerializable^ val in this) {
          if (val != nullptr) {
            size += val->ObjectSize;
          }
        }
        return m_linkedList->Count;
      }*/

        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get()
          {
            return m_linkedList->Count;
          }
        }

        virtual property System::Collections::Generic::LinkedList<Object^>^ Value
        {
          virtual System::Collections::Generic::LinkedList<Object^>^ get()
          {
            return m_linkedList;
          }
        }
        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableLinkedList(gcnew System::Collections::Generic::LinkedList<Object^>());
        }
      };
    }
  }
}
 } //namespace
