/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "Properties.hpp"
#include "impl/ManagedVisitor.hpp"
#include "impl/ManagedString.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypes.hpp"


using namespace System;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      // Visitor class to get string representations of a property object
      ref class PropertyToString
      {
      private:

        String^ m_str;

      public:

        inline PropertyToString( ) : m_str( "{" )
        { }

        void Visit( GemStone::GemFire::Cache::Generic::ICacheableKey^ key, IGFSerializable^ value )
        {
          if ( m_str->Length > 1 ) {
            m_str += ",";
          }
          m_str += key->ToString( ) + "=" + value;
        }

        virtual String^ ToString( ) override
        {
          return m_str;
        }
      };

      generic<class TPropKey, class TPropValue>
      TPropValue Properties<TPropKey, TPropValue>::Find( TPropKey key)
      {
        //ManagedString mg_key( key );
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TPropKey>( key ) );

        //_GF_MG_EXCEPTION_TRY2

          gemfire::CacheablePtr nativeptr(NativePtr->find( keyptr ));
          TPropValue returnVal = Serializable::GetManagedValueGeneric<TPropValue>( nativeptr );
          return returnVal;

          //gemfire::CacheablePtr& value = NativePtr->find( keyptr );
          //return SafeUMSerializableConvert( value.ptr( ) );

          //gemfire::CacheableStringPtr value = NativePtr->find( mg_key.CharPtr );
          //return CacheableString::GetString( value.ptr( ) );

       // _GF_MG_EXCEPTION_CATCH_ALL2
      }

      /*IGFSerializable^ Properties::Find( GemStone::GemFire::Cache::Generic::ICacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);

        if ( key != nullptr) {
          _GF_MG_EXCEPTION_TRY2
          
          gemfire::CacheableStringPtr csPtr;
          
          CacheableString::GetCacheableString(cStr->Value, csPtr);

          gemfire::CacheablePtr& value = NativePtr->find( csPtr );

          return ConvertCacheableString(value);

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          _GF_MG_EXCEPTION_TRY2

            gemfire::CacheablePtr& value = NativePtr->find( keyptr );
            return SafeUMSerializableConvert( value.ptr( ) );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
				return nullptr;
      }*/

      /*
       generic<class TPropKey, class TPropValue>
       IGFSerializable^ Properties<TPropKey, TPropValue>::ConvertCacheableString(gemfire::CacheablePtr& value)
       {
         gemfire::CacheableString * cs =  dynamic_cast<gemfire::CacheableString *>( value.ptr() );
          if ( cs == NULL) {
            return SafeUMSerializableConvert( value.ptr( ) );
          } 
          else {
            if(cs->typeId() == (int8_t)gemfire::GemfireTypeIds::CacheableASCIIString
              || cs->typeId() == (int8_t)gemfire::GemfireTypeIds::CacheableASCIIStringHuge) {
              String^ str = gcnew String(cs->asChar());
              return CacheableString::Create(str);
            }
            else {
              String^ str = gcnew String(cs->asWChar());
              return CacheableString::Create(str);
            }
          }
				 return nullptr;
        }
      */

      /*IGFSerializable^ Properties::Find( CacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);

        if ( key != nullptr) {
          _GF_MG_EXCEPTION_TRY2
          
          gemfire::CacheableStringPtr csPtr;
          
          CacheableString::GetCacheableString(cStr->Value, csPtr);

          gemfire::CacheablePtr& value = NativePtr->find( csPtr );

          return ConvertCacheableString(value);

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );

          _GF_MG_EXCEPTION_TRY2

            gemfire::CacheablePtr& value = NativePtr->find( keyptr );
            return SafeUMSerializableConvert( value.ptr( ) );

          _GF_MG_EXCEPTION_CATCH_ALL2        
        }
				return nullptr;
      }*/

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::Insert( TPropKey key, TPropValue value )
      {
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TPropKey>( key, true ) );
        gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TPropValue>( value, true ) );

        //ManagedString mg_key( key );
        //ManagedString mg_value( value );

        _GF_MG_EXCEPTION_TRY2

          //NativePtr->insert( mg_key.CharPtr, mg_value.CharPtr );
          NativePtr->insert( keyptr, valueptr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      /*void Properties::Insert( String^ key, const int32_t value)
      {
        //TODO::
        ManagedString mg_key( key );

        _GF_MG_EXCEPTION_TRY2

          NativePtr->insert( mg_key.CharPtr, value );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }*/

      /*void Properties::Insert( GemStone::GemFire::Cache::Generic::ICacheableKey^ key, IGFSerializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            gemfire::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      /*void Properties::Insert( CacheableKey^ key, IGFSerializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            gemfire::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      /*
      generic<class TPropKey, class TPropValue>
      gemfire::CacheableKey * Properties<TPropKey, TPropValue>::ConvertCacheableStringKey(CacheableString^ cStr)
      {
        gemfire::CacheableStringPtr csPtr;
        CacheableString::GetCacheableString(cStr->Value, csPtr);

        return csPtr.ptr();
      }
      */

	  /*void Properties::Insert( GemStone::GemFire::Cache::Generic::ICacheableKey^ key, Serializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            gemfire::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            gemfire::CacheablePtr valueptr(
              GetNativePtr<gemfire::Cacheable>( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr<gemfire::Cacheable>( value ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      /*void Properties::Insert( CacheableKey^ key, Serializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            gemfire::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            gemfire::CacheablePtr valueptr(
              GetNativePtr<gemfire::Cacheable>( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr<gemfire::Cacheable>( value ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::Remove( TPropKey key)
      {
        //ManagedString mg_key( key );
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TPropKey>( key ) );

        _GF_MG_EXCEPTION_TRY2

          //NativePtr->remove( mg_key.CharPtr );
          NativePtr->remove( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      /*void Properties::Remove( GemStone::GemFire::Cache::Generic::ICacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          
             gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));

             NativePtr->remove( keyptr );
          
           _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->remove( keyptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      /*void Properties::Remove( CacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          
             gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));

             NativePtr->remove( keyptr );
          
           _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->remove( keyptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::ForEach( PropertyVisitorGeneric<TPropKey, TPropValue>^ visitor )
      {
       if (visitor != nullptr)
        {
          gemfire::ManagedVisitorGeneric mg_visitor( visitor );

          PropertyVisitorProxy<TPropKey, TPropValue>^ proxy = gcnew PropertyVisitorProxy<TPropKey, TPropValue>();
          proxy->SetPropertyVisitorGeneric(visitor);

          PropertyVisitor^ otherVisitor = gcnew PropertyVisitor(proxy, &PropertyVisitorProxy<TPropKey, TPropValue>::Visit);
          mg_visitor.setptr(otherVisitor);

          _GF_MG_EXCEPTION_TRY2

            NativePtr->foreach( mg_visitor );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }

      generic<class TPropKey, class TPropValue>
      uint32_t Properties<TPropKey, TPropValue>::Size::get( )
      {
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->getSize( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::AddAll( Properties<TPropKey, TPropValue>^ other )
      {
        /*gemfire::PropertiesPtr p_other(
          GetNativePtr<gemfire::Properties>( other ) );*/

        gemfire::PropertiesPtr p_other(
          GetNativePtrFromSBWrapGeneric<gemfire::Properties>( other ) );        

        _GF_MG_EXCEPTION_TRY2

          NativePtr->addAll( p_other );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::Load( String^ fileName )
      {
        ManagedString mg_fname( fileName );

        _GF_MG_EXCEPTION_TRY2

          NativePtr->load( mg_fname.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TPropKey, class TPropValue>
      String^ Properties<TPropKey, TPropValue>::ToString( )
      {
       /* PropertyToString^ propStr = gcnew PropertyToString( );
        this->ForEach( gcnew PropertyVisitorGeneric( propStr,
          &PropertyToString::Visit ) );
        String^ str = propStr->ToString( );
        return ( str + "}" );*/
				return "";
      }

      // IGFSerializable methods

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::ToData( DataOutput^ output )
      {
        if (output->IsManagedObject()) {
        //TODO::??
          output->WriteBytesToUMDataOutput();          
        }
        
         gemfire::DataOutput* nativeOutput =
            GetNativePtr<gemfire::DataOutput>(output);
        
        if (nativeOutput != nullptr)
        {
          _GF_MG_EXCEPTION_TRY2

            NativePtr->toData( *nativeOutput );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }

        if (output->IsManagedObject()) {
          output->SetBuffer();          
        }
      }

      generic<class TPropKey, class TPropValue>
      IGFSerializable^ Properties<TPropKey, TPropValue>::FromData( DataInput^ input )
      {
        if(input->IsManagedObject()) {
          input->AdvanceUMCursor();
        }
        //TODO::??
        gemfire::DataInput* nativeInput =
          GetNativePtr<gemfire::DataInput>( input );
        if (nativeInput != nullptr)
        {
          _GF_MG_EXCEPTION_TRY2

            AssignPtr( static_cast<gemfire::Properties*>(
              NativePtr->fromData( *nativeInput ) ) );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        
        if(input->IsManagedObject()) {
          input->SetBuffer();
        }

        return this;
      }

      generic<class TPropKey, class TPropValue>
      uint32_t Properties<TPropKey, TPropValue>::ObjectSize::get( )
      {
        //TODO::
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->objectSize( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      // ISerializable methods

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::GetObjectData( SerializationInfo^ info,
        StreamingContext context )
      {
        if (_NativePtr != NULL) {
          gemfire::DataOutput output;

          _GF_MG_EXCEPTION_TRY2

            NativePtr->toData( output );

          _GF_MG_EXCEPTION_CATCH_ALL2

          array<Byte>^ bytes = gcnew array<Byte>( output.getBufferLength( ) );
          {
            pin_ptr<const Byte> pin_bytes = &bytes[0];
            memcpy( (uint8_t*)pin_bytes, output.getBuffer( ),
              output.getBufferLength( ) );
          }
          info->AddValue( "bytes", bytes, array<Byte>::typeid );
        }
      }
      
      generic<class TPropKey, class TPropValue>
      Properties<TPropKey, TPropValue>::Properties( SerializationInfo^ info,
        StreamingContext context )
        : SBWrap( gemfire::Properties::create( ).ptr( ) )
      {
        array<Byte>^ bytes = nullptr;
        try {
          bytes = dynamic_cast<array<Byte>^>( info->GetValue( "bytes",
            array<Byte>::typeid ) );
        }
        catch ( System::Exception^ ) {
          // could not find the header -- null value
        }
        if (bytes != nullptr) {
          pin_ptr<const Byte> pin_bytes = &bytes[0];

          _GF_MG_EXCEPTION_TRY2

            gemfire::DataInput input( (uint8_t*)pin_bytes, bytes->Length );
            AssignPtr( static_cast<gemfire::Properties*>(
              NativePtr->fromData( input ) ) );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }
      } // end namespace Generic
    }
  }
}