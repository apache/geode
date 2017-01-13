/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CacheableKey.hpp>
#include <gfcpp/CacheableBuiltins.hpp>
#include "IGFSerializable.hpp"
#include "IGFDelta.hpp"
#include "impl/ManagedString.hpp"
#include "impl/NativeWrapper.hpp"
#include "impl/EnumInfo.hpp"
#include "Log.hpp"
#include <vcclr.h>
#include "IPdxTypeMapper.hpp"
using namespace System::Reflection;
using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
				interface class IPdxSerializable;
        interface class IPdxSerializer;
      /// <summary>
      /// Signature of native function delegates passed to native
      /// <c>gemfire::Serializable::registerType</c>.
      /// Such functions should return an empty instance of the type they
      /// represent. The instance will typically be initialized immediately
      /// after creation by a call to native
      /// <c>gemfire::Serializable::fromData</c>.
      /// </summary>
      delegate gemfire::Serializable* TypeFactoryNativeMethodGeneric();

      /// <summary>
      /// Signature of function delegates passed to
      /// <see cref="Serializable.RegisterType" />. Such functions should
      /// return an empty instance of the type they represent.
      /// The delegate shall be stored in the internal <c>DelegateWrapper</c>
      /// class and an instance will be initialized in the
      /// <c>DelegateWrapper.NativeDelegate</c> method by a call to
      /// <see cref="IGFSerializable.FromData" />.
      /// </summary>
      public delegate GemStone::GemFire::Cache::Generic::IGFSerializable^ TypeFactoryMethodGeneric();
      /// <summary>
      /// Delegate to wrap a native <c>gemfire::Serializable</c> type.
      /// </summary>
      /// <remarks>
      /// This delegate should return an object of type <c>IGFSerializable</c>
      /// given a native object.
      /// </remarks>
      delegate GemStone::GemFire::Cache::Generic::IGFSerializable^ WrapperDelegateGeneric(gemfire::Serializable* obj);

			/// <summary>
      /// Signature of function delegates passed to
      /// <see cref="Serializable.RegisterPdxType" />. Such functions should
      /// return an empty instance of the type they represent.
      /// New instance will be created during de-serialization of Pdx Types
      /// <see cref="IPdxSerializable" />.
      /// </summary>
      public delegate GemStone::GemFire::Cache::Generic::IPdxSerializable^ PdxTypeFactoryMethod();
      
      /// <summary>
      /// This class wraps the native C++ <c>gemfire::Serializable</c> objects
      /// as managed <see cref="IGFSerializable" /> objects.
      /// </summary>
      public ref class Serializable
        : public GemStone::GemFire::Cache::Generic::Internal::SBWrap<gemfire::Serializable>,
        public GemStone::GemFire::Cache::Generic::IGFSerializable
      {
      public:
        /// <summary>
        /// Serializes this native (C++) object.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        virtual void ToData(GemStone::GemFire::Cache::Generic::DataOutput^ output);

        /// <summary>
        /// Deserializes the native (C++) object -- returns an instance of the
        /// <c>Serializable</c> class with the native object wrapped inside.
        /// </summary>
        /// <param name="input">
        /// the DataInput stream to use for reading the object data
        /// </param>
        /// <returns>the deserialized object</returns>
        virtual GemStone::GemFire::Cache::Generic::IGFSerializable^
          FromData(GemStone::GemFire::Cache::Generic::DataInput^ input);
        
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
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (bool value);

        /// <summary>
        /// Implicit conversion operator from a byte
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (Byte value);

        /// <summary>
        /// Implicit conversion operator from an array of bytes
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Byte>^ value);

        /// <summary>
        /// Implicit conversion operator from an boolean array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<bool>^ value);

        /// <summary>
        /// Implicit conversion operator from a double
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (Double value);

        /// <summary>
        /// Implicit conversion operator from a double array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Double>^ value);

        /// <summary>
        /// Implicit conversion operator from a float
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (Single value);

        /// <summary>
        /// Implicit conversion operator from a float array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Single>^ value);

        /// <summary>
        /// Implicit conversion operator from a 16-bit integer
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (int16_t value);

        /// <summary>
        /// Implicit conversion operator from a character
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (Char value);

        /// <summary>
        /// Implicit conversion operator from a character array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<Char>^ value);

        /// <summary>
        /// Implicit conversion operator from a 16-bit integer array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<int16_t>^ value);

        /// <summary>
        /// Implicit conversion operator from a 32-bit integer
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (int32_t value);

        /// <summary>
        /// Implicit conversion operator from a 32-bit integer array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<int32_t>^ value);

        /// <summary>
        /// Implicit conversion operator from a 64-bit integer
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator /*GemStone::GemFire::Cache::*/Serializable^ (int64_t value);

        /// <summary>
        /// Implicit conversion operator from a 64-bit integer array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<int64_t>^ value);

        /// <summary>
        /// Implicit conversion operator from a string
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (String^ value);

        /// <summary>
        /// Implicit conversion operator from a string array
        /// to a <c>Serializable</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::Generic::Serializable^ (array<String^>^ value);

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
        static void RegisterTypeGeneric(TypeFactoryMethodGeneric^ creationMethod);

        /// <summary>
        /// Set the PDX serializer for the cache. If this serializer is set,
        /// it will be consulted to see if it can serialize any domain classes which are 
        /// added to the cache in portable data exchange format. 
        /// </summary>
        static void RegisterPdxSerializer(IPdxSerializer^ pdxSerializer);
        
				/// <summary>
        /// Register an instance factory method for a given type.
        /// This should be used when registering types that implement
        /// IPdxSerializable.
        /// </summary>
        /// <param name="creationMethod">
        /// the creation function to register
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if the method is null
        /// </exception>
        
        static void RegisterPdxType(PdxTypeFactoryMethod^ creationMethod);

        /// <summary>
        /// Register an PdxTypeMapper to map the local types to pdx types
        /// </summary>
        /// <param name="pdxTypeMapper">
        /// Object which implements IPdxTypeMapper interface
        /// </param>
       

        static void SetPdxTypeMapper(IPdxTypeMapper^ pdxTypeMapper);        

      internal:

				static int32 GetPDXIdForType(const char* poolName, IGFSerializable^ pdxType);
				static IGFSerializable^ GetPDXTypeById(const char* poolName, int32 typeId);
				static IPdxSerializable^ Serializable::GetPdxType(String^ className);
				static void RegisterPDXManagedCacheableKey(bool appDomainEnable);
        static bool IsObjectAndPdxSerializerRegistered(String^ className);

        static IPdxSerializer^ GetPdxSerializer();
        static String^ GetPdxTypeName(String^ localTypeName);
        static String^ GetLocalTypeName(String^ pdxTypeName);
        static void Clear();

        static Type^ GetType(String^ className);

        static int GetEnumValue(Internal::EnumInfo^ ei);
        static Internal::EnumInfo^ GetEnum(int val);

         static Dictionary<String^, PdxTypeFactoryMethod^>^ PdxDelegateMap =
          gcnew Dictionary<String^, PdxTypeFactoryMethod^>();
       
        static String^ GetString(gemfire::CacheableStringPtr cStr);//gemfire::CacheableString*
        
        // These are the new static methods to get/put data from c++

        //byte
        static Byte getByte(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableByte(SByte val);
        
        //boolean
        static bool getBoolean(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableBoolean(bool val);
        
        //widechar
        static Char getChar(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableWideChar(Char val);
        
        //double
        static double getDouble(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableDouble(double val);
        
        //float
        static float getFloat(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableFloat(float val);
        
        //int16
        static int16 getInt16(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableInt16(int val);
        
        //int32
        static int32 getInt32(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableInt32(int32 val);
        
        //int64
        static int64 getInt64(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableInt64(int64 val);
        
        //cacheable ascii string
        static String^ getASCIIString(gemfire::SerializablePtr nativeptr);        

        static gemfire::CacheableKeyPtr getCacheableASCIIString(String^ val);

        static gemfire::CacheableKeyPtr getCacheableASCIIString2(String^ val);
        
        //cacheable ascii string huge
        static String^ getASCIIStringHuge(gemfire::SerializablePtr nativeptr);
        
        static gemfire::CacheableKeyPtr getCacheableASCIIStringHuge(String^ val);        

        //cacheable string
        static String^ getUTFString(gemfire::SerializablePtr nativeptr);        

        static gemfire::CacheableKeyPtr getCacheableUTFString(String^ val);
        

        //cacheable string huge
        static String^ getUTFStringHuge(gemfire::SerializablePtr nativeptr);
        

        static gemfire::CacheableKeyPtr getCacheableUTFStringHuge(String^ val);
        

       static gemfire::CacheableStringPtr GetCacheableString(String^ value);       

       static gemfire::CacheableStringPtr GetCacheableString2(String^ value); 

       /*
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
        */

        static array<Byte>^ getSByteArray(array<SByte>^ sArray);
        
        static array<int16_t>^ getInt16Array(array<uint16_t>^ sArray);
        
        static array<int32_t>^ getInt32Array(array<uint32_t>^ sArray);        

        static array<int64_t>^ getInt64Array(array<uint64_t>^ sArray);
        

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline GemStone::GemFire::Cache::Generic::Serializable()
          : GemStone::GemFire::Cache::Generic::Internal::SBWrap<gemfire::Serializable>() { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline GemStone::GemFire::Cache::Generic::Serializable(gemfire::Serializable* nativeptr)
          : Generic::Internal::SBWrap<gemfire::Serializable>(nativeptr) { }

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
        static void RegisterTypeGeneric(Byte typeId,
          TypeFactoryMethodGeneric^ creationMethod, Type^ type);

        /// <summary>
        /// Unregister the type with the given typeId
        /// </summary>
        /// <param name="typeId">typeId of the type to unregister.</param>
        static void UnregisterTypeGeneric(Byte typeId);

        generic<class TValue>
        static TValue GetManagedValueGeneric(gemfire::SerializablePtr val);

        generic<class TKey>
        static gemfire::CacheableKeyPtr GetUnmanagedValueGeneric(TKey key);

        generic<class TKey>
        static gemfire::CacheableKeyPtr GetUnmanagedValueGeneric(TKey key, bool isAciiChar);

        generic<class TKey>
        static gemfire::CacheableKeyPtr GetUnmanagedValueGeneric(
          Type^ managedType, TKey key);

        generic<class TKey>
        static gemfire::CacheableKeyPtr GetUnmanagedValueGeneric(
          Type^ managedType, TKey key, bool isAsciiChar);

        /// <summary>
        /// Static map of <c>TypeFactoryMethod</c> delegates created
        /// for managed <c>TypeFactoryMethod</c> delegates.
        /// </summary>
        static Dictionary<System::Type^, Byte>^ ManagedTypeMappingGeneric =
          gcnew Dictionary<System::Type^, Byte>();

        static Byte GetManagedTypeMappingGeneric (Type^ type)
        {
          Byte retVal = 0;
          ManagedTypeMappingGeneric->TryGetValue(type, retVal);
          return retVal;
        }

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
        static List<TypeFactoryNativeMethodGeneric^>^ NativeDelegatesGeneric =
          gcnew List<TypeFactoryNativeMethodGeneric^>();

        /// <summary>
        /// Static map of <c>TypeFactoryMethod</c> delegates created
        /// from registered managed <c>TypeFactoryMethod</c> delegates.
        /// This is for cross AppDomain object creations.
        /// </summary>
        static Dictionary<UInt32, TypeFactoryMethodGeneric^>^ DelegateMapGeneric =
          gcnew Dictionary<UInt32, TypeFactoryMethodGeneric^>();

        static Dictionary<UInt32, TypeFactoryMethodGeneric^>^ InternalDelegateMapGeneric =
          gcnew Dictionary<UInt32, TypeFactoryMethodGeneric^>();

        static TypeFactoryMethodGeneric^ GetTypeFactoryMethodGeneric(UInt32 classid)
        {
         // Log::Finer("TypeFactoryMethodGeneric type id " + classid + " domainid :" + System::Threading::Thread::GetDomainID() );
          if(DelegateMapGeneric->ContainsKey(classid) )
            return DelegateMapGeneric[classid];
          else
            return InternalDelegateMapGeneric[classid];//builtin types
        }

        /// <summary>
        /// Static map of <c>TypeFactoryNativeMethod</c> delegates created
        /// for builtin managed <c>TypeFactoryMethod</c> delegates.
        /// This is so that the underlying managed objects do not get GCed.
        /// </summary>
        static Dictionary<Byte, TypeFactoryNativeMethodGeneric^>^ BuiltInDelegatesGeneric =
          gcnew Dictionary<Byte, TypeFactoryNativeMethodGeneric^>();

        /// <summary>
        /// Static map of <c>TypeFactoryMethod</c> delegates created
        /// for managed <c>TypeFactoryMethod</c> delegates.
        /// </summary>
        static Dictionary<int64_t, TypeFactoryMethodGeneric^>^ ManagedDelegatesGeneric =
          gcnew Dictionary<int64_t, TypeFactoryMethodGeneric^>();

        /// <summary>
        /// This is to get manged delegates.
        /// </summary>
        static TypeFactoryMethodGeneric^ GetManagedDelegateGeneric(int64_t typeId)
        {
          TypeFactoryMethodGeneric^ ret = nullptr;
          ManagedDelegatesGeneric->TryGetValue(typeId, ret);
          return ret;
        }

        static IPdxSerializer^ PdxSerializer = nullptr;
        static IPdxTypeMapper^ PdxTypeMapper = nullptr;
        static Object^ LockObj = gcnew Object();
        static Dictionary<String^, String^>^ PdxTypeNameToLocal =
          gcnew Dictionary<String^, String^>();
        static Dictionary<String^, String^>^ LocalTypeNameToPdx =
          gcnew Dictionary<String^, String^>();


        static Object^ ClassNameVsTypeLockObj = gcnew Object();
        static Dictionary<String^, Type^>^ ClassNameVsType =
          gcnew Dictionary<String^, Type^>();

        delegate Object^ CreateNewObjectDelegate();
        static CreateNewObjectDelegate^ CreateNewObjectDelegateF(Type^ type);
       
        delegate Object^ CreateNewObjectArrayDelegate(int len);
        static CreateNewObjectArrayDelegate^ CreateNewObjectArrayDelegateF(Type^ type);
        
        static array<Type^>^ singleIntTypeA = gcnew array<Type^>{ Int32::typeid };

        static Type^ createNewObjectDelegateType = Type::GetType("GemStone.GemFire.Cache.Generic.Serializable+CreateNewObjectDelegate");
        static Type^ createNewObjectArrayDelegateType = Type::GetType("GemStone.GemFire.Cache.Generic.Serializable+CreateNewObjectArrayDelegate");

        static array<Type^>^ singleIntType = gcnew array<Type^>(1){Int32::typeid};

        static Object^ CreateObject(String^ className);
        static Object^ GetArrayObject(String^ className, int len);
        static Type^ getTypeFromRefrencedAssemblies(String^ className, Dictionary<Assembly^, bool>^ referedAssembly, Assembly^ currentAsm);

        static Dictionary<String^, CreateNewObjectDelegate^>^ ClassNameVsCreateNewObjectDelegate =
          gcnew Dictionary<String^, CreateNewObjectDelegate^>();

        static Dictionary<String^, CreateNewObjectArrayDelegate^>^ ClassNameVsCreateNewObjectArrayDelegate =
          gcnew Dictionary<String^, CreateNewObjectArrayDelegate^>();

        static Object^ CreateObjectEx(String^ className);
        static Object^ GetArrayObjectEx(String^ className, int len);
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
        static array<WrapperDelegateGeneric^>^ NativeWrappersGeneric =
          gcnew array<WrapperDelegateGeneric^>(WrapperEndGeneric + 1);
        literal Byte WrapperEndGeneric = 128;

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
        static void RegisterWrapperGeneric(WrapperDelegateGeneric^ wrapperMethod,
          Byte typeId, System::Type^ type);

        /// <summary>
        /// Internal static method to remove managed artifacts created by
        /// RegisterType and RegisterWrapper methods when
        /// <see cref="DistributedSystem.Disconnect" /> is called.
        /// </summary>
        static void UnregisterNativesGeneric();

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
        inline static WrapperDelegateGeneric^ GetWrapperGeneric(Byte typeId)
        {
          if (typeId >= 0 && typeId <= WrapperEndGeneric) {
            return NativeWrappersGeneric[typeId];
          }
          return nullptr;
        }

				static Serializable()
        {
          PdxTypeMapper = nullptr;
          //RegisterPDXManagedCacheableKey();

          {
          Dictionary<Object^, Object^>^ dic = gcnew Dictionary<Object^, Object^>();
          ManagedTypeMappingGeneric[dic->GetType()] = gemfire::GemfireTypeIds::CacheableHashMap;
          ManagedTypeMappingGeneric[dic->GetType()->GetGenericTypeDefinition()] = gemfire::GemfireTypeIds::CacheableHashMap;
          }

          {
          System::Collections::ArrayList^ arr = gcnew System::Collections::ArrayList();
          ManagedTypeMappingGeneric[arr->GetType()] = gemfire::GemfireTypeIds::CacheableVector;
          }
		  
          {
          System::Collections::Generic::LinkedList<Object^>^ linketList = gcnew  System::Collections::Generic::LinkedList<Object^>();
          ManagedTypeMappingGeneric[linketList->GetType()] = gemfire::GemfireTypeIds::CacheableLinkedList;
          ManagedTypeMappingGeneric[linketList->GetType()->GetGenericTypeDefinition()] = gemfire::GemfireTypeIds::CacheableLinkedList;
          }
		  
          {
          System::Collections::Generic::IList<Object^>^ iList = gcnew System::Collections::Generic::List<Object^>();
          ManagedTypeMappingGeneric[iList->GetType()] = gemfire::GemfireTypeIds::CacheableArrayList;
          ManagedTypeMappingGeneric[iList->GetType()->GetGenericTypeDefinition()] = gemfire::GemfireTypeIds::CacheableArrayList;
          }

          //TODO: Linked list, non generic stack, some other map types and see if more

          {
            System::Collections::Generic::Stack<Object^>^ stack = gcnew System::Collections::Generic::Stack<Object^>();
            ManagedTypeMappingGeneric[stack->GetType()] = gemfire::GemfireTypeIds::CacheableStack;
            ManagedTypeMappingGeneric[stack->GetType()->GetGenericTypeDefinition()] = gemfire::GemfireTypeIds::CacheableStack;
          }
          {
            ManagedTypeMappingGeneric[SByte::typeid] = gemfire::GemfireTypeIds::CacheableByte;
            ManagedTypeMappingGeneric[Boolean::typeid] = gemfire::GemfireTypeIds::CacheableBoolean;
            ManagedTypeMappingGeneric[Char::typeid] = gemfire::GemfireTypeIds::CacheableWideChar;
            ManagedTypeMappingGeneric[Double::typeid] = gemfire::GemfireTypeIds::CacheableDouble;
            ManagedTypeMappingGeneric[String::typeid] = gemfire::GemfireTypeIds::CacheableASCIIString;
            ManagedTypeMappingGeneric[float::typeid] = gemfire::GemfireTypeIds::CacheableFloat;
            ManagedTypeMappingGeneric[Int16::typeid] = gemfire::GemfireTypeIds::CacheableInt16;
            ManagedTypeMappingGeneric[Int32::typeid] = gemfire::GemfireTypeIds::CacheableInt32;
            ManagedTypeMappingGeneric[Int64::typeid] = gemfire::GemfireTypeIds::CacheableInt64;
            ManagedTypeMappingGeneric[Type::GetType("System.Byte[]")] = gemfire::GemfireTypeIds::CacheableBytes;
            ManagedTypeMappingGeneric[Type::GetType("System.Double[]")] = gemfire::GemfireTypeIds::CacheableDoubleArray;
            ManagedTypeMappingGeneric[Type::GetType("System.Single[]")] = gemfire::GemfireTypeIds::CacheableFloatArray;
            ManagedTypeMappingGeneric[Type::GetType("System.Int16[]")] = gemfire::GemfireTypeIds::CacheableInt16Array;
            ManagedTypeMappingGeneric[Type::GetType("System.Int32[]")] = gemfire::GemfireTypeIds::CacheableInt32Array;
            ManagedTypeMappingGeneric[Type::GetType("System.Int64[]")] = gemfire::GemfireTypeIds::CacheableInt64Array;
            ManagedTypeMappingGeneric[Type::GetType("System.String[]")] = gemfire::GemfireTypeIds::CacheableStringArray;
            ManagedTypeMappingGeneric[Type::GetType("System.DateTime")] = gemfire::GemfireTypeIds::CacheableDate;
            ManagedTypeMappingGeneric[Type::GetType("System.Collections.Hashtable")] = gemfire::GemfireTypeIds::CacheableHashTable;
          }
        }
      };
      } // end namespace Generic
    }
  }
}
