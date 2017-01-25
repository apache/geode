/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//#include "gf_includes.hpp"
#include "Properties.hpp"
#include "impl/ManagedVisitor.hpp"
#include "impl/ManagedString.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypes.hpp"


using namespace System;


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      // Visitor class to get string representations of a property object
      ref class PropertyToString
      {
      private:

        String^ m_str;

      public:

        inline PropertyToString( ) : m_str( "{" )
        { }

        void Visit( Apache::Geode::Client::ICacheableKey^ key, IGFSerializable^ value )
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
        apache::geode::client::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TPropKey>( key ) );

        //_GF_MG_EXCEPTION_TRY2

          apache::geode::client::CacheablePtr nativeptr(NativePtr->find( keyptr ));
          TPropValue returnVal = Serializable::GetManagedValueGeneric<TPropValue>( nativeptr );
          return returnVal;

          //apache::geode::client::CacheablePtr& value = NativePtr->find( keyptr );
          //return SafeUMSerializableConvert( value.ptr( ) );

          //apache::geode::client::CacheableStringPtr value = NativePtr->find( mg_key.CharPtr );
          //return CacheableString::GetString( value.ptr( ) );

       // _GF_MG_EXCEPTION_CATCH_ALL2
      }

      /*IGFSerializable^ Properties::Find( Apache::Geode::Client::ICacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);

        if ( key != nullptr) {
          _GF_MG_EXCEPTION_TRY2
          
          apache::geode::client::CacheableStringPtr csPtr;
          
          CacheableString::GetCacheableString(cStr->Value, csPtr);

          apache::geode::client::CacheablePtr& value = NativePtr->find( csPtr );

          return ConvertCacheableString(value);

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          _GF_MG_EXCEPTION_TRY2

            apache::geode::client::CacheablePtr& value = NativePtr->find( keyptr );
            return SafeUMSerializableConvert( value.ptr( ) );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
				return nullptr;
      }*/

      /*
       generic<class TPropKey, class TPropValue>
       IGFSerializable^ Properties<TPropKey, TPropValue>::ConvertCacheableString(apache::geode::client::CacheablePtr& value)
       {
         apache::geode::client::CacheableString * cs =  dynamic_cast<apache::geode::client::CacheableString *>( value.ptr() );
          if ( cs == NULL) {
            return SafeUMSerializableConvert( value.ptr( ) );
          } 
          else {
            if(cs->typeId() == (int8_t)apache::geode::client::GeodeTypeIds::CacheableASCIIString
              || cs->typeId() == (int8_t)apache::geode::client::GeodeTypeIds::CacheableASCIIStringHuge) {
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
          
          apache::geode::client::CacheableStringPtr csPtr;
          
          CacheableString::GetCacheableString(cStr->Value, csPtr);

          apache::geode::client::CacheablePtr& value = NativePtr->find( csPtr );

          return ConvertCacheableString(value);

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr(
            (apache::geode::client::CacheableKey*)GetNativePtr<apache::geode::client::Cacheable>( key ) );

          _GF_MG_EXCEPTION_TRY2

            apache::geode::client::CacheablePtr& value = NativePtr->find( keyptr );
            return SafeUMSerializableConvert( value.ptr( ) );

          _GF_MG_EXCEPTION_CATCH_ALL2        
        }
				return nullptr;
      }*/

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::Insert( TPropKey key, TPropValue value )
      {
        apache::geode::client::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TPropKey>( key, true ) );
        apache::geode::client::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TPropValue>( value, true ) );

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

      /*void Properties::Insert( Apache::Geode::Client::ICacheableKey^ key, IGFSerializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          apache::geode::client::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            apache::geode::client::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            apache::geode::client::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          apache::geode::client::CacheablePtr valueptr( SafeMSerializableConvert( value ) );

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
          apache::geode::client::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            apache::geode::client::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            apache::geode::client::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr(
            (apache::geode::client::CacheableKey*)GetNativePtr<apache::geode::client::Cacheable>( key ) );
          apache::geode::client::CacheablePtr valueptr( SafeMSerializableConvert( value ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      /*
      generic<class TPropKey, class TPropValue>
      apache::geode::client::CacheableKey * Properties<TPropKey, TPropValue>::ConvertCacheableStringKey(CacheableString^ cStr)
      {
        apache::geode::client::CacheableStringPtr csPtr;
        CacheableString::GetCacheableString(cStr->Value, csPtr);

        return csPtr.ptr();
      }
      */

	  /*void Properties::Insert( Apache::Geode::Client::ICacheableKey^ key, Serializable^ value)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          apache::geode::client::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            apache::geode::client::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            apache::geode::client::CacheablePtr valueptr(
              GetNativePtr<apache::geode::client::Cacheable>( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          apache::geode::client::CacheablePtr valueptr(
            GetNativePtr<apache::geode::client::Cacheable>( value ) );

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
          apache::geode::client::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));
          CacheableString^ cValueStr = dynamic_cast<CacheableString ^>(value);

          if (cValueStr != nullptr) {
            apache::geode::client::CacheablePtr valueptr(ConvertCacheableStringKey(cValueStr));
            NativePtr->insert( keyptr, valueptr );
          }
          else {
            apache::geode::client::CacheablePtr valueptr(
              GetNativePtr<apache::geode::client::Cacheable>( value ) );
            NativePtr->insert( keyptr, valueptr );
          }
          _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr(
            (apache::geode::client::CacheableKey*)GetNativePtr<apache::geode::client::Cacheable>( key ) );
          apache::geode::client::CacheablePtr valueptr(
            GetNativePtr<apache::geode::client::Cacheable>( value ) );

          _GF_MG_EXCEPTION_TRY2

            NativePtr->insert( keyptr, valueptr );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }*/

      generic<class TPropKey, class TPropValue>
      void Properties<TPropKey, TPropValue>::Remove( TPropKey key)
      {
        //ManagedString mg_key( key );
        apache::geode::client::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TPropKey>( key ) );

        _GF_MG_EXCEPTION_TRY2

          //NativePtr->remove( mg_key.CharPtr );
          NativePtr->remove( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      /*void Properties::Remove( Apache::Geode::Client::ICacheableKey^ key)
      {
        CacheableString^ cStr = dynamic_cast<CacheableString ^>(key);
        if (cStr != nullptr) {
           _GF_MG_EXCEPTION_TRY2
          
             apache::geode::client::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));

             NativePtr->remove( keyptr );
          
           _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

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
          
             apache::geode::client::CacheableKeyPtr keyptr(ConvertCacheableStringKey(cStr));

             NativePtr->remove( keyptr );
          
           _GF_MG_EXCEPTION_CATCH_ALL2
        }
        else {
          apache::geode::client::CacheableKeyPtr keyptr(
            (apache::geode::client::CacheableKey*)GetNativePtr<apache::geode::client::Cacheable>( key ) );

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
          apache::geode::client::ManagedVisitorGeneric mg_visitor( visitor );

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
        /*apache::geode::client::PropertiesPtr p_other(
          GetNativePtr<apache::geode::client::Properties>( other ) );*/

        apache::geode::client::PropertiesPtr p_other(
          GetNativePtrFromSBWrapGeneric<apache::geode::client::Properties>( other ) );        

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
        
         apache::geode::client::DataOutput* nativeOutput =
            GetNativePtr<apache::geode::client::DataOutput>(output);
        
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
        apache::geode::client::DataInput* nativeInput =
          GetNativePtr<apache::geode::client::DataInput>( input );
        if (nativeInput != nullptr)
        {
          _GF_MG_EXCEPTION_TRY2

            AssignPtr( static_cast<apache::geode::client::Properties*>(
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
          apache::geode::client::DataOutput output;

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
        : SBWrap( apache::geode::client::Properties::create( ).ptr( ) )
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

            apache::geode::client::DataInput input( (uint8_t*)pin_bytes, bytes->Length );
            AssignPtr( static_cast<apache::geode::client::Properties*>(
              NativePtr->fromData( input ) ) );

          _GF_MG_EXCEPTION_CATCH_ALL2
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

  }
}