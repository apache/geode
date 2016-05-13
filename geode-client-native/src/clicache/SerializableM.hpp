/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CacheableKey.hpp"
#include "cppcache/CacheableBuiltins.hpp"
#include "IGFSerializable.hpp"
#include "IGFDelta.hpp"
#include "impl/ManagedString.hpp"
#include "impl/NativeWrapper.hpp"
#include "LogM.hpp"
#include <vcclr.h>

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      /// <summary>
      /// Signature of native function delegates passed to native
      /// <c>gemfire::Serializable::registerType</c>.
      /// Such functions should return an empty instance of the type they
      /// represent. The instance will typically be initialized immediately
      /// after creation by a call to native
      /// <c>gemfire::Serializable::fromData</c>.
      /// </summary>
      delegate gemfire::Serializable* TypeFactoryNativeMethod();

      /// <summary>
      /// Signature of function delegates passed to
      /// <see cref="Serializable.RegisterType" />. Such functions should
      /// return an empty instance of the type they represent.
      /// The delegate shall be stored in the internal <c>DelegateWrapper</c>
      /// class and an instance will be initialized in the
      /// <c>DelegateWrapper.NativeDelegate</c> method by a call to
      /// <see cref="IGFSerializable.FromData" />.
      /// </summary>
      public delegate GemStone::GemFire::Cache::IGFSerializable^ TypeFactoryMethod();
      /// <summary>
      /// Delegate to wrap a native <c>gemfire::Serializable</c> type.
      /// </summary>
      /// <remarks>
      /// This delegate should return an object of type <c>IGFSerializable</c>
      /// given a native object.
      /// </remarks>
      delegate GemStone::GemFire::Cache::IGFSerializable^ WrapperDelegate(gemfire::Serializable* obj);

      /// <summary>
      /// This class wraps the native C++ <c>gemfire::Serializable</c> objects
      /// as managed <see cref="IGFSerializable" /> objects.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class Serializable
        : public Internal::SBWrap<gemfire::Serializable>,
        public /*GemStone::GemFire::Cache::*/IGFSerializable
      {
      public:
        /// <summary>
        /// Serializes this native (C++) object.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        virtual void ToData(GemStone::GemFire::Cache::DataOutput^ output);

        /// <summary>
        /// Deserializes the native (C++) object -- returns an instance of the
        /// <c>Serializable</c> class with the native object wrapped inside.
        /// </summary>
        /// <param name="input">
        /// the DataInput stream to use for reading the object data
        /// </param>
        /// <returns>the deserialized object</returns>
        virtual GemStone::GemFire::Cache::IGFSerializable^ FromData(GemStone::GemFire::Cache::DataInput^ input);
        
        /// <summary>
        /// return the size of this object in bytes
        /// </summary>
        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get(); 
        }

        /// <summary>
        /// Returns the classId of the instance being serialized.
        /// This is used by deserialization to determine what instance
        /// type to create and deserialize into.
        /// </summary>
        /// <returns>the classId</returns>
        virtual property uint32_t ClassId
        {
          virtual uint32_t get();
        }

        /// <summary>
        /// Return a string representation of the object.
        /// It simply returns the string representation of the underlying
        /// native object by calling its <c>toString()</c> function.
        /// </summary>
        virtual String^ ToString() override;

        // Static conversion function from primitive types string, integer
        // and byte array.

        /// <summary>
        /// Implicit conversion operator from a boolean
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (bool value);

        /// <summary>
        /// Implicit conversion operator from a byte
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (Byte value);

        /// <summary>
        /// Implicit conversion operator from an array of bytes
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<Byte>^ value);

        /// <summary>
        /// Implicit conversion operator from an boolean array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<bool>^ value);

        /// <summary>
        /// Implicit conversion operator from a double
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (Double value);

        /// <summary>
        /// Implicit conversion operator from a double array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<Double>^ value);

        /// <summary>
        /// Implicit conversion operator from a float
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (Single value);

        /// <summary>
        /// Implicit conversion operator from a float array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<Single>^ value);

        /// <summary>
        /// Implicit conversion operator from a 16-bit integer
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (int16_t value);

        /// <summary>
        /// Implicit conversion operator from a character
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (Char value);

        /// <summary>
        /// Implicit conversion operator from a character array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<Char>^ value);

        /// <summary>
        /// Implicit conversion operator from a 16-bit integer array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<int16_t>^ value);

        /// <summary>
        /// Implicit conversion operator from a 32-bit integer
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (int32_t value);

        /// <summary>
        /// Implicit conversion operator from a 32-bit integer array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<int32_t>^ value);

        /// <summary>
        /// Implicit conversion operator from a 64-bit integer
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator /*GemStone::GemFire::Cache::*/Serializable^ (int64_t value);

        /// <summary>
        /// Implicit conversion operator from a 64-bit integer array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<int64_t>^ value);

        /// <summary>
        /// Implicit conversion operator from a string
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (String^ value);

        /// <summary>
        /// Implicit conversion operator from a string array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Serializable^ (array<String^>^ value);

        /// <summary>
        /// Register an instance factory method for a given type.
        /// This should be used when registering types that implement
        /// IGFSerializable.
        /// </summary>
        /// <param name="creationMethod">
        /// the creation function to register
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if the method is null
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if the typeId has already been registered, or there is an error
        /// in registering the type; check <c>Utils::LastError</c> for more
        /// information in the latter case.
        /// </exception>
        static void RegisterType(TypeFactoryMethod^ creationMethod);

      internal:

        // These are the new static methods to get/put data from c++

        //byte
        static Byte getByte(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableByte* ci = static_cast<gemfire::CacheableByte*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableByte(Byte val)
        {
          return gemfire::CacheableByte::create(val);
        }

        //boolean
        static bool getBoolean(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableBoolean* ci = static_cast<gemfire::CacheableBoolean*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableBoolean(bool val)
        {
          return gemfire::CacheableBoolean::create(val);
        }

        //widechar
        static Char getChar(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableWideChar* ci = static_cast<gemfire::CacheableWideChar*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableWideChar(Char val)
        {
          return gemfire::CacheableWideChar::create(val);
        }

        //double
        static double getDouble(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableDouble* ci = static_cast<gemfire::CacheableDouble*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableDouble(double val)
        {
          return gemfire::CacheableDouble::create(val);
        }

        //float
        static float getFloat(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableFloat* ci = static_cast<gemfire::CacheableFloat*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableFloat(float val)
        {
          return gemfire::CacheableFloat::create(val);
        }

        //int16
        static int16 getInt16(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableInt16* ci = static_cast<gemfire::CacheableInt16*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableInt16(int val)
        {
          return gemfire::CacheableInt16::create(val);
        }

        //int32
        static int32 getInt32(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableInt32* ci = static_cast<gemfire::CacheableInt32*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableInt32(int32 val)
        {
          return gemfire::CacheableInt32::create(val);
        }

        //int64
        static int64 getInt64(gemfire::SerializablePtr nativeptr)
        {
          gemfire::CacheableInt64* ci = static_cast<gemfire::CacheableInt64*>(nativeptr.ptr());
          return ci->value();
        }

        static gemfire::CacheableKeyPtr getCacheableInt64(int64 val)
        {
          return gemfire::CacheableInt64::create(val);
        }

        //cacheable ascii string
        static String^ getASCIIString(gemfire::SerializablePtr nativeptr)
        {
          //gemfire::CacheableString* ci = static_cast<gemfire::CacheableString*>(nativeptr.ptr());          
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        static gemfire::CacheableKeyPtr getCacheableASCIIString(String^ val)
        {
          return GetCacheableString(val);
        }

        //cacheable ascii string huge
        static String^ getASCIIStringHuge(gemfire::SerializablePtr nativeptr)
        {
          //gemfire::CacheableString* ci = static_cast<gemfire::CacheableString*>(nativeptr.ptr());          
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        static gemfire::CacheableKeyPtr getCacheableASCIIStringHuge(String^ val)
        {
          return GetCacheableString(val);
        }

        //cacheable string
        static String^ getUTFString(gemfire::SerializablePtr nativeptr)
        {
          //gemfire::CacheableString* ci = static_cast<gemfire::CacheableString*>(nativeptr.ptr());          
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        static gemfire::CacheableKeyPtr getCacheableUTFString(String^ val)
        {
          return GetCacheableString(val);
        }

        //cacheable string huge
        static String^ getUTFStringHuge(gemfire::SerializablePtr nativeptr)
        {
          //gemfire::CacheableString* ci = static_cast<gemfire::CacheableString*>(nativeptr.ptr());
          //return GetString(ci);
          return GetString(nativeptr->toString());
        }

        static gemfire::CacheableKeyPtr getCacheableUTFStringHuge(String^ val)
        {
          return GetCacheableString(val);
        }

       static gemfire::CacheableStringPtr GetCacheableString(String^ value)
       {
          gemfire::CacheableStringPtr cStr;
          size_t len = 0;
          if (value != nullptr) {
            len = value->Length;
            pin_ptr<const wchar_t> pin_value = PtrToStringChars(value);
            cStr = gemfire::CacheableString::create(pin_value, static_cast<int32_t> (len));
          }
          else {
            cStr = (gemfire::CacheableString*)
              gemfire::CacheableString::createDeserializable();
          }

          return cStr;
        }

        static String^ GetString(gemfire::CacheableStringPtr cStr)//gemfire::CacheableString*
        {
          if (cStr == NULLPTR) {
            return nullptr;
          }
          else if (cStr->isWideString()) {
            return ManagedString::Get(cStr->asWChar());
          }
          else {
            return ManagedString::Get(cStr->asChar());
          }
        }

        static array<Byte>^ getSByteArray(array<SByte>^ sArray)
        {
          array<Byte>^ dArray = gcnew array<Byte>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        static array<int16_t>^ getInt16Array(array<uint16_t>^ sArray)
        {
          array<int16_t>^ dArray = gcnew array<int16_t>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        static array<int32_t>^ getInt32Array(array<uint32_t>^ sArray)
        {
          array<int32_t>^ dArray = gcnew array<int32_t>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        static array<int64_t>^ getInt64Array(array<uint64_t>^ sArray)
        {
          array<int64_t>^ dArray = gcnew array<int64_t>(sArray->Length);
          for (int index = 0; index < dArray->Length; index++) 
          {
            dArray[index] = sArray[index];
          }          
          return dArray;
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline GemStone::GemFire::Cache::Serializable()
          : SBWrap() { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline GemStone::GemFire::Cache::Serializable(gemfire::Serializable* nativeptr)
          : SBWrap(nativeptr) { }

        /// <summary>
        /// Register an instance factory method for a given type and typeId.
        /// This should be used when registering types that implement
        /// IGFSerializable.
        /// </summary>
        /// <param name="typeId">typeId of the type being registered.</param>
        /// <param name="creationMethod">
        /// the creation function to register
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if the method is null
        /// </exception>
        static void RegisterType(Byte typeId,
          TypeFactoryMethod^ creationMethod);

        /// <summary>
        /// Unregister the type with the given typeId
        /// </summary>
        /// <param name="typeId">typeId of the type to unregister.</param>
        static void UnregisterType(Byte typeId);

        /// <summary>
        /// Used to assign the native Serializable pointer to a new object.
        /// </summary>
        /// <remarks>
        /// Note the order of preserveSB() and releaseSB(). This handles the
        /// corner case when <c>m_nativeptr</c> is same as <c>nativeptr</c>.
        /// </remarks>
        inline void AssignSP(gemfire::Serializable* nativeptr)
        {
          AssignPtr(nativeptr);
        }

        /// <summary>
        /// Used to assign the native Serializable pointer to a new object.
        /// </summary>
        inline void SetSP(gemfire::Serializable* nativeptr)
        {
          if (nativeptr != nullptr) {
            nativeptr->preserveSB();
          }
          _SetNativePtr(nativeptr);
        }

        /// <summary>
        /// Static list of <c>TypeFactoryNativeMethod</c> delegates created
        /// from registered managed <c>TypeFactoryMethod</c> delegates.
        /// This is so that the underlying managed objects do not get GCed.
        /// </summary>
        static List<TypeFactoryNativeMethod^>^ NativeDelegates =
          gcnew List<TypeFactoryNativeMethod^>();

        /// <summary>
        /// Static map of <c>TypeFactoryMethod</c> delegates created
        /// from registered managed <c>TypeFactoryMethod</c> delegates.
        /// This is for cross AppDomain object creations.
        /// </summary>
        static Dictionary<UInt32, TypeFactoryMethod^>^ DelegateMap =
          gcnew Dictionary<UInt32, TypeFactoryMethod^>();

        static Dictionary<UInt32, TypeFactoryMethod^>^ InternalDelegateMap =
          gcnew Dictionary<UInt32, TypeFactoryMethod^>();

        static TypeFactoryMethod^ GetTypeFactoryMethod(UInt32 classid)
        {
          Log::Fine("TypeFactoryMethod type id " + classid + " domainid :" + System::Threading::Thread::GetDomainID() );
          if(DelegateMap->ContainsKey(classid) )
            return DelegateMap[classid];
          else
            return InternalDelegateMap[classid];//builtin types
        }

        /// <summary>
        /// Static map of <c>TypeFactoryNativeMethod</c> delegates created
        /// for builtin managed <c>TypeFactoryMethod</c> delegates.
        /// This is so that the underlying managed objects do not get GCed.
        /// </summary>
        static Dictionary<Byte, TypeFactoryNativeMethod^>^ BuiltInDelegates =
          gcnew Dictionary<Byte, TypeFactoryNativeMethod^>();

        /// <summary>
        /// Static map of <c>TypeFactoryMethod</c> delegates created
        /// for managed <c>TypeFactoryMethod</c> delegates.
        /// </summary>
        static Dictionary<int64_t, TypeFactoryMethod^>^ ManagedDelegates =
          gcnew Dictionary<int64_t, TypeFactoryMethod^>();

        /// <summary>
        /// This is to get manged delegates.
        /// </summary>
        static TypeFactoryMethod^ GetManagedDelegate(int64_t typeId)
        {
          if (ManagedDelegates->ContainsKey(typeId))
            return (TypeFactoryMethod^)ManagedDelegates[typeId];
          else
            return nullptr;
        }

        /// <summary>
        /// Static array of managed <c>WrapperDelegate</c> delegates that
        /// maintains a mapping of built-in native typeIds to their corresponding
        /// wrapper type delegates.
        /// </summary>
        /// <remarks>
        /// This is as an array to make lookup as fast as possible, taking
        /// advantage of the fact that the range of wrapped built-in typeIds is
        /// small. <b>IMPORTANT:</b> If the built-in native typeIds encompass a
        /// greater range then change <c>WrapperEnd</c> in this accordingly
        /// or move to using a Dictionary instead.
        /// </remarks>
        static array<WrapperDelegate^>^ NativeWrappers =
          gcnew array<WrapperDelegate^>(WrapperEnd + 1);
        literal Byte WrapperEnd = 128;

        /// <summary>
        /// Static method to register a managed wrapper for a native
        /// <c>gemfire::Serializable</c> type.
        /// </summary>
        /// <param name="wrapperMethod">
        /// A factory delegate of the managed wrapper class that returns the
        /// managed object given the native object.
        /// </param>
        /// <param name="typeId">The typeId of the native type.</param>
        /// <seealso cref="NativeWrappers" />
        static void RegisterWrapper(WrapperDelegate^ wrapperMethod,
          Byte typeId);

        /// <summary>
        /// Internal static method to remove managed artifacts created by
        /// RegisterType and RegisterWrapper methods when
        /// <see cref="DistributedSystem.Disconnect" /> is called.
        /// </summary>
        static void UnregisterNatives();

        /// <summary>
        /// Static method to lookup the wrapper delegate for a given typeId.
        /// </summary>
        /// <param name="typeId">
        /// The typeId of the native <c>gemfire::Serializable</c> type.
        /// </param>
        /// <returns>
        /// If a managed wrapper is registered for the given typeId then the
        /// wrapper delegate is returned, else this returns null.
        /// </returns>
        inline static WrapperDelegate^ GetWrapper(Byte typeId)
        {
          if (typeId >= 0 && typeId <= WrapperEnd) {
            return NativeWrappers[typeId];
          }
          return nullptr;
        }
      };
    }
  }
}
