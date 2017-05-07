/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "PropertiesM.hpp"
#include "impl/ManagedVisitor.hpp"
#include "impl/ManagedString.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypesM.hpp"


using namespace System;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      // Visitor class to get string representations of a property object
      ref class PropertyToString
      {
      private:

        String^ m_str;

      public:

        inline PropertyToString( ) : m_str( "{" )
        { }

        void Visit( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value )
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


      String^ Properties::Find( String^ key)
      {
        ManagedString mg_key( key );

        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableStringPtr value = NativePtr->find( mg_key.CharPtr );
          return CacheableString::GetString( value.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IGFSerializable^ Properties::Find( GemStone::GemFire::Cache::ICacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);

        if ( key != nullptr) {
          _GF_MG_EXCEPTION_TRY
          
          gemfire::CacheableStringPtr csPtr;
          
          CacheableString::GetCacheableString(cStr->Value, csPtr);

          gemfire::CacheablePtr& value = NativePtr->find( csPtr );

          return ConvertCacheableString(value);

          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          _GF_MG_EXCEPTION_TRY

            gemfire::CacheablePtr& value = NativePtr->find( keyptr );
            return SafeUMSerializableConvert( value.ptr( ) );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

       IGFSerializable^ Properties::ConvertCacheableString(gemfire::CacheablePtr& value)
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
        }

      IGFSerializable^ Properties::Find( CacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);

        if ( key != nullptr) {
          _GF_MG_EXCEPTION_TRY
          
          gemfire::CacheableStringPtr csPtr;
          
          CacheableString::GetCacheableString(cStr->Value, csPtr);

          gemfire::CacheablePtr& value = NativePtr->find( csPtr );

          return ConvertCacheableString(value);

          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );

          _GF_MG_EXCEPTION_TRY

            gemfire::CacheablePtr& value = NativePtr->find( keyptr );
            return SafeUMSerializableConvert( value.ptr( ) );

          _GF_MG_EXCEPTION_CATCH_ALL        
        }
      }

      void Properties::Insert( String^ key, String^ value)
      {
        ManagedString mg_key( key );
        ManagedString mg_value( value );

        _GF_MG_EXCEPTION_TRY

          NativePtr->insert( mg_key.CharPtr, mg_value.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Properties::Insert( String^ key, const int32_t value)
      {
        //TODO::hitesh
        ManagedString mg_key( key );

        _GF_MG_EXCEPTION_TRY

          NativePtr->insert( mg_key.CharPtr, value );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Properties::Insert( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY
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
          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );

          _GF_MG_EXCEPTION_TRY

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Properties::Insert( CacheableKey^ key, IGFSerializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY
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
          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );

          _GF_MG_EXCEPTION_TRY

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      gemfire::CacheableKey * Properties::ConvertCacheableStringKey(CacheableString^ cStr)
      {
        gemfire::CacheableStringPtr csPtr;
        CacheableString::GetCacheableString(cStr->Value, csPtr);

        return csPtr.ptr();
      }

      void Properties::Insert( GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY
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
          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr<gemfire::Cacheable>( value ) );

          _GF_MG_EXCEPTION_TRY

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Properties::Insert( CacheableKey^ key, Serializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY
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
          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr<gemfire::Cacheable>( value ) );

          _GF_MG_EXCEPTION_TRY

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Properties::Remove( String^ key)
      {
        ManagedString mg_key( key );

        _GF_MG_EXCEPTION_TRY

          NativePtr->remove( mg_key.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Properties::Remove( GemStone::GemFire::Cache::ICacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY
          
             gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));

             NativePtr->remove( keyptr );
          
           _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          _GF_MG_EXCEPTION_TRY

            NativePtr->remove( keyptr );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Properties::Remove( CacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY
          
             gemfire::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));

             NativePtr->remove( keyptr );
          
           _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr<gemfire::Cacheable>( key ) );

          _GF_MG_EXCEPTION_TRY

            NativePtr->remove( keyptr );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Properties::ForEach( PropertyVisitor^ visitor )
      {
        if (visitor != nullptr)
        {
          gemfire::ManagedVisitor mg_visitor( visitor );

          _GF_MG_EXCEPTION_TRY

            NativePtr->foreach( mg_visitor );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      uint32_t Properties::Size::get( )
      {
        _GF_MG_EXCEPTION_TRY

          return NativePtr->getSize( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Properties::AddAll( Properties^ other )
      {
        gemfire::PropertiesPtr p_other(
          GetNativePtr<gemfire::Properties>( other ) );

        _GF_MG_EXCEPTION_TRY

          NativePtr->addAll( p_other );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Properties::Load( String^ fileName )
      {
        ManagedString mg_fname( fileName );

        _GF_MG_EXCEPTION_TRY

          NativePtr->load( mg_fname.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      String^ Properties::ToString( )
      {
        PropertyToString^ propStr = gcnew PropertyToString( );
        this->ForEach( gcnew PropertyVisitor( propStr,
          &PropertyToString::Visit ) );
        String^ str = propStr->ToString( );
        return ( str + "}" );
      }

      // IGFSerializable methods

      void Properties::ToData( DataOutput^ output )
      {
        if (output->IsManagedObject()) {
        //TODO::hitesh??
          output->WriteBytesToUMDataOutput();          
        }
        
         gemfire::DataOutput* nativeOutput =
            GetNativePtr<gemfire::DataOutput>(output);
        
        if (nativeOutput != nullptr)
        {
          _GF_MG_EXCEPTION_TRY

            NativePtr->toData( *nativeOutput );

          _GF_MG_EXCEPTION_CATCH_ALL
        }

        if (output->IsManagedObject()) {
          output->SetBuffer();          
        }
      }

      IGFSerializable^ Properties::FromData( DataInput^ input )
      {
        if(input->IsManagedObject()) {
          input->AdvanceUMCursor();
        }
        //TODO::hitesh??
        gemfire::DataInput* nativeInput =
          GetNativePtr<gemfire::DataInput>( input );
        if (nativeInput != nullptr)
        {
          _GF_MG_EXCEPTION_TRY

            AssignPtr( static_cast<gemfire::Properties*>(
              NativePtr->fromData( *nativeInput ) ) );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
        
        if(input->IsManagedObject()) {
          input->SetBuffer();
        }

        return this;
      }

      uint32_t Properties::ObjectSize::get( )
      {
        //TODO::hitesh
        _GF_MG_EXCEPTION_TRY

          return NativePtr->objectSize( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      // ISerializable methods

      void Properties::GetObjectData( SerializationInfo^ info,
        StreamingContext context )
      {
        if (_NativePtr != NULL) {
          gemfire::DataOutput output;

          _GF_MG_EXCEPTION_TRY

            NativePtr->toData( output );

          _GF_MG_EXCEPTION_CATCH_ALL

          array<Byte>^ bytes = gcnew array<Byte>( output.getBufferLength( ) );
          {
            pin_ptr<const Byte> pin_bytes = &bytes[0];
            memcpy( (uint8_t*)pin_bytes, output.getBuffer( ),
              output.getBufferLength( ) );
          }
          info->AddValue( "bytes", bytes, array<Byte>::typeid );
        }
      }

      Properties::Properties( SerializationInfo^ info,
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

          _GF_MG_EXCEPTION_TRY

            gemfire::DataInput input( (uint8_t*)pin_bytes, bytes->Length );
            AssignPtr( static_cast<gemfire::Properties*>(
              NativePtr->fromData( input ) ) );

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }
    }
  }
}
