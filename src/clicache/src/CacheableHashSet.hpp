/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CacheableBuiltins.hpp>
#include "Serializable.hpp"
#include "ExceptionTypes.hpp"
#include "impl/PdxInstanceImpl.hpp"

using namespace System;
using namespace System::Collections::Generic;
#pragma managed

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      namespace Internal
      {
        /// <summary>
        /// A mutable <c>ICacheableKey</c> hash set wrapper that can serve as
        /// a distributable object for caching.
        /// </summary>
        template <uint32_t TYPEID, typename HSTYPE>
        public ref class CacheableHashSetType
          : public Serializable, public ICollection<Object^>
        {
        public:

        virtual void ToData(DataOutput^ output) override
        {
          output->WriteArrayLen(this->Count);

          Internal::ManagedPtrWrap< gemfire::Serializable,
              Internal::SBWrap<gemfire::Serializable> > nptr = NativePtr;
          HSTYPE* set = static_cast<HSTYPE*>(nptr());
          for (typename HSTYPE::Iterator iter = set->begin();
              iter != set->end(); ++iter) {
                //Generic::ICacheableKey^ key = SafeGenericUMKeyConvert<ICacheableKey^>((*iter).ptr());
                Object^ key = Serializable::GetManagedValueGeneric<Object^>((*iter));
            output->WriteObject(key);
          }
        }

        virtual IGFSerializable^ FromData(DataInput^ input) override
        {
          int len = input->ReadArrayLen();
          if (len > 0)
          {
            for ( int i = 0; i < len; i++)
            {
              //Generic::ICacheableKey^ key = dynamic_cast<Generic::ICacheableKey^>(input->ReadObject());
              Object^ key = (input->ReadObject());
              this->Add(key);
            }
          }
          return this;
        }

        virtual property uint32_t ObjectSize 
        {
          virtual uint32_t get() override
          {
            uint32_t size = 0;
            for each (Object^ key in this) {
              if ( key != nullptr)
                //size += key->ObjectSize; 
                //TODO:: how should we do this now
                size += 1;
            }
            return size;
          }
        }  

        virtual int GetHashCode() override
        {
          IEnumerator<Object^>^ ie = GetEnumerator();

          int h = 0;
          while(ie->MoveNext() == true)
          {
            h = h + PdxInstanceImpl::deepArrayHashCode(ie->Current);
          }
          return h;
        }

        virtual bool Equals(Object^ other)override
        {
          if(other == nullptr)
            return false;

          CacheableHashSetType^ otherCHST = dynamic_cast<CacheableHashSetType^>(other);
          
          if(otherCHST == nullptr)
            return false;

          if(Count != otherCHST->Count)
            return false;

          IEnumerator<Object^>^ ie = GetEnumerator();

          while(ie->MoveNext() == true)
          {
            if(otherCHST->Contains(ie->Current))
              return true;
            else
              return false;
          }

          return true;
        }

          /// <summary>
          /// Enumerator for <c>CacheableHashSet</c> class.
          /// </summary>
          ref class Enumerator sealed
            : public Internal::UMWrap<typename HSTYPE::Iterator>,
            public IEnumerator<Object^>
          {
          public:
            // Region: IEnumerator<ICacheableKey^> Members

            /// <summary>
            /// Gets the element in the collection at the current
            /// position of the enumerator.
            /// </summary>
            /// <returns>
            /// The element in the collection at the current position
            /// of the enumerator.
            /// </returns>
            property Object^ Current
            {
              virtual Object^ get() =
                IEnumerator<Object^>::Current::get
              {
                if (!m_started) {
                  throw gcnew System::InvalidOperationException(
                    "Call MoveNext first.");
                }
                //return SafeGenericUMKeyConvert<Generic::ICacheableKey^>((*(*NativePtr())).ptr());
                return Serializable::GetManagedValueGeneric<Object^>((*(*NativePtr())));
              }
            }

            // End Region: IEnumerator<ICacheableKey^> Members

            // Region: IEnumerator Members

            /// <summary>
            /// Advances the enumerator to the next element of the collection.
            /// </summary>
            /// <returns>
            /// true if the enumerator was successfully advanced to the next
            /// element; false if the enumerator has passed the end of
            /// the collection.
            /// </returns>
            virtual bool MoveNext()
            {
              Internal::ManagedPtrWrap< typename HSTYPE::Iterator,
                Internal::UMWrap<typename HSTYPE::Iterator> > nptr = NativePtr;
              bool isEnd = nptr->isEnd();
              if (!m_started) {
                m_started = true;
              }
              else {
                if (!isEnd) {
                  (*nptr())++;
                  isEnd = nptr->isEnd();
                }
              }
              GC::KeepAlive(this);
              return !isEnd;
            }

            /// <summary>
            /// Sets the enumerator to its initial position, which is before
            /// the first element in the collection.
            /// </summary>
            virtual void Reset()
            {
              NativePtr->reset();
              m_started = false;
            }

            // End Region: IEnumerator Members

          internal:
            /// <summary>
            /// Internal constructor to wrap a native object pointer
            /// </summary>
            /// <param name="nativeptr">The native object pointer</param>
            inline Enumerator(typename HSTYPE::Iterator* nativeptr,
                CacheableHashSetType<TYPEID, HSTYPE>^ set)
              : UMWrap(nativeptr, true), m_set(set) { }

          private:
            // Region: IEnumerator Members

            /// <summary>
            /// Gets the current element in the collection.
            /// </summary>
            /// <returns>
            ///     The current element in the collection.
            /// </returns>
            /// <exception cref="System.InvalidOperationException">
            /// The enumerator is positioned before the first element of
            /// the collection or after the last element.
            /// </exception>
            property Object^ ICurrent
            {
              virtual Object^ get() sealed =
                System::Collections::IEnumerator::Current::get
              {
                return Current;
              }
            }

            // End Region: IEnumerator Members

            bool m_started;

            CacheableHashSetType<TYPEID, HSTYPE>^ m_set;
          };

          /// <summary>
          /// Returns the classId of the instance being serialized.
          /// This is used by deserialization to determine what instance
          /// type to create and deserialize into.
          /// </summary>
          /// <returns>the classId</returns>
          virtual property uint32_t ClassId
          {
            virtual uint32_t get() override
            {
              //return static_cast<HSTYPE*>(NativePtr())->classId() + 0x80000000;
              return TYPEID;
            }
          }

          /// <summary>
          /// Get the largest possible size of the <c>CacheableHashSet</c>.
          /// </summary>
          property int32_t MaxSize
          {
            inline int32_t get()
            {
              return static_cast<HSTYPE*>(NativePtr())->max_size();
            }
          }

          /// <summary>
          /// True if the <c>CacheableHashSet</c>'s size is 0.
          /// </summary>
          property bool IsEmpty
          {
            inline bool get()
            {
              return static_cast<HSTYPE*>(NativePtr())->empty();
            }
          }

          /// <summary>
          /// Get the number of buckets used by the HashSet.
          /// </summary>
          property int32_t BucketCount
          {
            inline int32_t get()
            {
              return static_cast<HSTYPE*>(NativePtr())->bucket_count();
            }
          }

          /// <summary>
          /// Increases the bucket count to at least <c>size</c> elements.
          /// </summary>
          /// <param name="size">The new size of the HashSet.</param>
          virtual void Resize(int32_t size) sealed
          {
            static_cast<HSTYPE*>(NativePtr())->resize(size);
          }

          /// <summary>
          /// Swap the contents of this <c>CacheableHashSet</c>
          /// with the given one.
          /// </summary>
          /// <param name="other">
          /// The other CacheableHashSet to use for swapping.
          /// </param>
          virtual void Swap(CacheableHashSetType<TYPEID, HSTYPE>^ other) sealed
          {
            if (other != nullptr) {
              static_cast<HSTYPE*>(NativePtr())->swap(
                *static_cast<HSTYPE*>(other->NativePtr()));
            }
          }

          // Region: ICollection<ICacheableKey^> Members

          /// <summary>
          /// Adds an item to the <c>CacheableHashSet</c>.
          /// </summary>
          /// <param name="item">
          /// The object to add to the collection.
          /// </param>
          virtual void Add(Object^ item)
          {
            _GF_MG_EXCEPTION_TRY2/* due to auto replace */

              gemfire::CacheableKeyPtr nativeptr(Serializable::GetUnmanagedValueGeneric(item));
            static_cast<HSTYPE*>(NativePtr())->insert(nativeptr);

            _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
          }

          /// <summary>
          /// Removes all items from the <c>CacheableHashSet</c>.
          /// </summary>
          virtual void Clear()
          {
            static_cast<HSTYPE*>(NativePtr())->clear();
          }

          /// <summary>
          /// Determines whether the <c>CacheableHashSet</c> contains
          /// a specific value.
          /// </summary>
          /// <param name="item">
          /// The object to locate in the <c>CacheableHashSet</c>.
          /// </param>
          /// <returns>
          /// true if item is found in the <c>CacheableHashSet</c>;
          /// otherwise false.
          /// </returns>
          virtual bool Contains(Object^ item)
          {
            return static_cast<HSTYPE*>(NativePtr())->contains(
              gemfire::CacheableKeyPtr(Serializable::GetUnmanagedValueGeneric(item)));
          }

          /// <summary>
          /// Copies the elements of the <c>CacheableHashSet</c> to an
          /// <c>System.Array</c>, starting at a particular
          /// <c>System.Array</c> index.
          /// </summary>
          /// <param name="array">
          /// The one-dimensional System.Array that is the destination of the
          /// elements copied from <c>CacheableHashSet</c>. The
          /// <c>System.Array</c> must have zero-based indexing.
          /// </param>
          /// <param name="arrayIndex">
          /// The zero-based index in array at which copying begins.
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// arrayIndex is less than 0 or array is null.
          /// </exception>
          /// <exception cref="OutOfRangeException">
          /// arrayIndex is equal to or greater than the length of array.
          /// -or-The number of elements in the source <c>CacheableHashSet</c>
          /// is greater than the available space from arrayIndex to the end
          /// of the destination array.
          /// </exception>
          virtual void CopyTo(array<Object^>^ array, int32_t arrayIndex)
          {
            if (array == nullptr || arrayIndex < 0) {
              throw gcnew IllegalArgumentException("CacheableHashSet.CopyTo():"
                " array is null or array index is less than zero");
            }
            Internal::ManagedPtrWrap< gemfire::Serializable,
              Internal::SBWrap<gemfire::Serializable> > nptr = NativePtr;
            HSTYPE* set = static_cast<HSTYPE*>(nptr());
            int32_t index = arrayIndex;

            if (arrayIndex >= array->Length ||
              array->Length < (arrayIndex + (int32_t)set->size())) {
                throw gcnew OutOfRangeException("CacheableHashSet.CopyTo():"
                  " array index is beyond the HashSet or length of given "
                  "array is less than that required to copy all the "
                  "elements from HashSet");
            }
            for (typename HSTYPE::Iterator iter = set->begin();
              iter != set->end(); ++iter, ++index) {
                array[index] = Serializable::GetManagedValueGeneric<Object^>((*iter));
            }
            GC::KeepAlive(this);
          }

          /// <summary>
          /// Gets the number of elements contained in the
          /// <c>CacheableHashSet</c>.
          /// </summary>
          virtual property int32_t Count
          {
            virtual int32_t get()
            {
              return static_cast<HSTYPE*>(NativePtr())->size();
            }
          }

          /// <summary>
          /// Removes the first occurrence of a specific object from the
          /// <c>CacheableHashSet</c>.
          /// </summary>
          /// <param name="item">
          /// The object to remove from the <c>CacheableHashSet</c>.
          /// </param>
          /// <returns>
          /// true if item was successfully removed from the
          /// <c>CacheableHashSet</c>; otherwise, false. This method also
          /// returns false if item is not found in the original
          /// <c>CacheableHashSet</c>.
          /// </returns>
          virtual bool Remove(Object^ item)
          {
            return (static_cast<HSTYPE*>(NativePtr())->erase(
              gemfire::CacheableKeyPtr(Serializable::GetUnmanagedValueGeneric(item))) > 0);
          }

          /// <summary>
          /// Gets a value indicating whether the collection is read-only.
          /// </summary>
          /// <returns>
          /// always false for <c>CacheableHashSet</c>
          /// </returns>
          virtual property bool IsReadOnly
          {
            virtual bool get()
            {
              return false;
            }
          }

          // End Region: ICollection<ICacheableKey^> Members

          // Region: IEnumerable<ICacheableKey^> Members

          /// <summary>
          /// Returns an enumerator that iterates through the
          /// <c>CacheableHashSet</c>.
          /// </summary>
          /// <returns>
          /// A <c>System.Collections.Generic.IEnumerator</c> that
          /// can be used to iterate through the <c>CacheableHashSet</c>.
          /// </returns>
          virtual IEnumerator<Object^>^ GetEnumerator()
          {
            typename HSTYPE::Iterator* iter = new typename HSTYPE::Iterator(
              static_cast<HSTYPE*>(NativePtr())->begin());

            return gcnew Enumerator(iter, this);
          }

          // End Region: IEnumerable<ICacheableKey^> Members

        internal:
          /// <summary>
          /// Factory function to register wrapper
          /// </summary>
          static IGFSerializable^ Create(gemfire::Serializable* obj)
          {
            return (obj != NULL ?
              gcnew CacheableHashSetType<TYPEID,HSTYPE>(obj) : nullptr);
          }

        private:
          // Region: IEnumerable Members

          /// <summary>
          /// Returns an enumerator that iterates through a collection.
          /// </summary>
          /// <returns>
          /// An <c>System.Collections.IEnumerator</c> object that can be used
          /// to iterate through the collection.
          /// </returns>
          virtual System::Collections::IEnumerator^ GetIEnumerator() sealed =
            System::Collections::IEnumerable::GetEnumerator
          {
            return GetEnumerator();
          }

          // End Region: IEnumerable Members

        protected:
          /// <summary>
          /// Private constructor to wrap a native object pointer
          /// </summary>
          /// <param name="nativeptr">The native object pointer</param>
          inline CacheableHashSetType<TYPEID, HSTYPE>(gemfire::Serializable* nativeptr)
            : Serializable(nativeptr) { }

          /// <summary>
          /// Allocates a new empty instance.
          /// </summary>
          inline CacheableHashSetType<TYPEID, HSTYPE>()
            : Serializable(HSTYPE::createDeserializable())
          { }

          /// <summary>
          /// Allocates a new empty instance with given initial size.
          /// </summary>
          /// <param name="size">The initial size of the HashSet.</param>
          inline CacheableHashSetType<TYPEID,HSTYPE>(int32_t size)
            : Serializable(HSTYPE::create(size).ptr())
          { }
        };
      }

#define _GFCLI_CACHEABLEHASHSET_DEF_GENERIC(m, HSTYPE)                               \
	public ref class m : public Internal::CacheableHashSetType<GemStone::GemFire::Cache::Generic::GemFireClassIds::m, HSTYPE>      \
      {                                                                       \
      public:                                                                 \
        /** <summary>
         *  Allocates a new empty instance.
         *  </summary>
         */                                                                   \
        inline m()                                                            \
        : Internal::CacheableHashSetType<GemStone::GemFire::Cache::Generic::GemFireClassIds::m, HSTYPE>() {}                      \
                                                                              \
        /** <summary>
         *  Allocates a new instance with the given size.
         *  </summary>
         *  <param name="size">the intial size of the new instance</param>
         */                                                                   \
        inline m(int32_t size)                                                 \
        : Internal::CacheableHashSetType<GemStone::GemFire::Cache::Generic::GemFireClassIds::m, HSTYPE>(size) {}                  \
                                                                              \
        /** <summary>
         *  Static function to create a new empty instance.
         *  </summary>
         */                                                                   \
        inline static m^ Create()                                             \
        {                                                                     \
          return gcnew m();                                                   \
        }                                                                     \
                                                                              \
        /** <summary>
         *  Static function to create a new instance with the given size.
         *  </summary>
         */                                                                   \
        inline static m^ Create(int32_t size)                                  \
        {                                                                     \
          return gcnew m(size);                                               \
        }                                                                     \
                                                                              \
        /* <summary>
         * Factory function to register this class.
         * </summary>
         */                                                                   \
        static IGFSerializable^ CreateDeserializable()                        \
        {                                                                     \
          return gcnew m();                                                   \
        }                                                                     \
                                                                              \
      internal:                                                               \
        static IGFSerializable^ Create(gemfire::Serializable* obj)            \
        {                                                                     \
          return gcnew m(obj);                                                \
        }                                                                     \
                                                                              \
      private:                                                                \
        inline m(gemfire::Serializable* nativeptr)                            \
        : Internal::CacheableHashSetType<GemStone::GemFire::Cache::Generic::GemFireClassIds::m, HSTYPE>(nativeptr) { }             \
      };

      /// <summary>
      /// A mutable <c>ICacheableKey</c> hash set wrapper that can serve as
      /// a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLEHASHSET_DEF_GENERIC(CacheableHashSet,
        gemfire::CacheableHashSet);

      /// <summary>
      /// A mutable <c>ICacheableKey</c> hash set wrapper that can serve as
      /// a distributable object for caching. This is provided for compability
      /// with java side though is functionally identical to
      /// <c>CacheableHashSet</c> i.e. does not provide the linked semantics of
      /// java <c>LinkedHashSet</c>.
      /// </summary>
      _GFCLI_CACHEABLEHASHSET_DEF_GENERIC(CacheableLinkedHashSet,
        gemfire::CacheableLinkedHashSet);
    }
  }
}
 } //namespace 
