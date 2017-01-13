/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"

//#include "impl/NativeWrapper.hpp"
#include "IGFSerializable.hpp"
#include "ICacheableKey.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "CacheableString.hpp"

#include "gfcpp/Properties.hpp"
#include "impl/SafeConvert.hpp"
#include "Serializable.hpp"

using namespace System;
using namespace System::Runtime::Serialization;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      delegate void PropertyVisitor(GemStone::GemFire::Cache::Generic::ICacheableKey^ key, GemStone::GemFire::Cache::Generic::IGFSerializable^ value);

      generic <class TPropKey, class TPropValue>
      ref class PropertyVisitorProxy;

      namespace Generic
      {

      /// <summary>
      /// Delegate that represents visitor for the <c>Properties</c> class.
      /// </summary>
      /// <remarks>
      /// This delegate is passed to the <c>Properties.ForEach</c> function
      /// that invokes this delegate for each property having a key
      /// and a value.
      /// </remarks>
      /// <param name="key">The key of the property.</param>
      /// <param name="value">The value of the property.</param>
      generic<class TPropKey, class TPropValue>
	    public delegate void PropertyVisitorGeneric( TPropKey key, TPropValue value );

      generic<class TPropKey, class TPropValue>
      [Serializable]
      /// <summary>
      /// Provides a collection of properties, each of which is a key/value
      /// pair. Each key is a string, and the value may be a string
      /// or an integer.
      /// </summary>
      public ref class Properties sealed
        : public Internal::SBWrap<gemfire::Properties>, public IGFSerializable,
        public ISerializable
      {
      public:

        /// <summary>
        /// Default constructor: returns an empty collection.
        /// </summary>
        inline Properties( )
          : SBWrap( gemfire::Properties::create( ).ptr( ) ) { }

        /// <summary>
        /// Factory method to create an empty collection of properties.
        /// </summary>
        /// <returns>empty collection of properties</returns>
        generic<class TPropKey, class TPropValue>
        inline static Properties<TPropKey, TPropValue>^ Create( )
        {
          return gcnew Properties<TPropKey, TPropValue>( );
        }

        /// <summary>
        /// Return the value for the given key, or NULL if not found. 
        /// </summary>
        /// <param name="key">the key to find</param>
        /// <returns>the value for the key</returns>
        /// <exception cref="NullPointerException">
        /// if the key is null
        /// </exception>
        TPropValue Find( TPropKey key );

        /// <summary>
        /// Add or update the string value for key.
        /// </summary>
        /// <param name="key">the key to insert</param>
        /// <param name="value">the string value to insert</param>
        /// <exception cref="NullPointerException">
        /// if the key is null
        /// </exception>
        void Insert( TPropKey key, TPropValue value );

        /// <summary>
        /// Remove the key from the collection. 
        /// </summary>
        /// <param name="key">the key to remove</param>
        /// <exception cref="NullPointerException">
        /// if the key is null
        /// </exception>
        void Remove( TPropKey key );

        /// <summary>
        /// Execute the Visitor delegate for each entry in the collection.
        /// </summary>
        /// <param name="visitor">visitor delegate</param>
        void ForEach( PropertyVisitorGeneric<TPropKey, TPropValue>^ visitor );

        /// <summary>
        /// Return the number of entries in the collection.
        /// </summary>
        /// <returns>the number of entries</returns>
        property uint32_t Size
        {
          uint32_t get( );
        }

        /*/// <summary>
        /// Adds the contents of <c>other</c> to this instance, replacing
        /// any existing values with those from other.
        /// </summary>
        /// <param name="other">new set of properties</param>*/
        void AddAll( Properties<TPropKey, TPropValue>^ other );

        /// <summary>
        /// Reads property values from a file, overriding what is currently
        /// in the properties object. 
        /// </summary>
        /// <param name="fileName">the name of the file</param>
        void Load( String^ fileName );

        /// <summary>
        /// Returns a string representation of the current
        /// <c>Properties</c> object.
        /// </summary>
        /// <returns>
        /// A comma separated list of property name,value pairs.
        /// </returns>
        virtual String^ ToString( ) override;

        // IGFSerializable members

        /// <summary>
        /// Serializes this Properties object.
        /// </summary>
        /// <param name="output">
        /// the DataOutput stream to use for serialization
        /// </param>
        virtual void ToData( DataOutput^ output );

        /// <summary>
        /// Deserializes this Properties object.
        /// </summary>
        /// <param name="input">
        /// the DataInput stream to use for reading data
        /// </param>
        /// <returns>the deserialized Properties object</returns>
        virtual IGFSerializable^ FromData( DataInput^ input );

        /// <summary>
        /// return the size of this object in bytes
        /// </summary>
        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get( ); 
        }

        /// <summary>
        /// Returns the classId of this class for serialization.
        /// </summary>
        /// <returns>classId of the Properties class</returns>
        /// <seealso cref="IGFSerializable.ClassId" />
        virtual property uint32_t ClassId
        {
          inline virtual uint32_t get( )
          {
            return GemFireClassIds::Properties;
          }
        }

        // End: IGFSerializable members

        // ISerializable members

        virtual void GetObjectData( SerializationInfo^ info,
          StreamingContext context );

        // End: ISerializable members

        /// <summary>
        /// Get the underlying native unmanaged pointer.
        /// </summary>
        property void* NativeIntPtr
        {
          inline void* get()
          {
            return _NativePtr;
          }
        }

        /*
        inline static IGFSerializable^ ConvertCacheableString(gemfire::CacheablePtr& value);
        inline static gemfire::CacheableKey *  ConvertCacheableStringKey(CacheableString^ cStr);
        */
        

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="ptr">The native IntPtr pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        generic<class TPropKey, class TPropValue>
        static Properties<TPropKey, TPropValue>^ CreateFromVoidPtr(void* ptr)
        {
          gemfire::Properties* nativeptr = (gemfire::Properties*)ptr;
          return ( nativeptr != nullptr ?
            gcnew Properties<TPropKey, TPropValue>( nativeptr ) : nullptr );
        }


      protected:

        // For deserialization using the .NET serialization (ISerializable)
        Properties( SerializationInfo^ info, StreamingContext context );


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        generic<class TPropKey, class TPropValue>
        static Properties<TPropKey, TPropValue>^ Create( gemfire::Serializable* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Properties<TPropKey, TPropValue>( nativeptr ) : nullptr );
        }

        inline static IGFSerializable^ CreateDeserializable( )
        {
          return Create<String^, String^>(  );
        }

        /// <summary>
        /// Factory function to register wrapper
        /// </summary>
        inline static IGFSerializable^ CreateDeserializable(
          gemfire::Serializable* nativeptr )
        {
          return Create<String^, String^>( nativeptr );
        }

      internal:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Properties( gemfire::Serializable* nativeptr )
          : SBWrap( (gemfire::Properties*)nativeptr ) { }
      };
      } // end namespace Generic

      generic <class TPropKey, class TPropValue>
      ref class PropertyVisitorProxy
      {
      public:
        void Visit(GemStone::GemFire::Cache::Generic::ICacheableKey^ key,
          GemStone::GemFire::Cache::Generic::IGFSerializable^ value)
        {
          TPropKey tpkey = GemStone::GemFire::Cache::Generic::Serializable::
            GetManagedValueGeneric<TPropKey>(SerializablePtr(SafeMSerializableConvertGeneric(key)));
          TPropValue tpvalue = GemStone::GemFire::Cache::Generic::Serializable::
            GetManagedValueGeneric<TPropValue>(SerializablePtr(SafeMSerializableConvertGeneric(value)));
          m_visitor->Invoke(tpkey, tpvalue);
        }

        void SetPropertyVisitorGeneric(
          GemStone::GemFire::Cache::Generic::PropertyVisitorGeneric<TPropKey, TPropValue>^ visitor)
        {
          m_visitor = visitor;
        }

      private:

        GemStone::GemFire::Cache::Generic::PropertyVisitorGeneric<TPropKey, TPropValue>^ m_visitor;

      };

    }
  }
}

