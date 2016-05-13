/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "NativeWrapper.hpp"
#include "ManagedCacheableKey.hpp"
#include "ManagedCacheableDelta.hpp"
#include "ManagedCacheableKeyBytes.hpp"
#include "ManagedCacheableDeltaBytes.hpp"
#include "../SerializableM.hpp"
#include "../LogM.hpp"
#include "../CacheableKeyM.hpp"
#include "../CqEventM.hpp"
#include "../UserFunctionExecutionExceptionM.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      public ref class SafeConvertClass
      {
      public:
        static bool isAppDomainEnabled = false;
  
        static void SetAppDomainEnabled(bool isAppDomainEnable)
        {
          GemFire::Cache::Log::Fine("SetAppDomainEnabled: " + isAppDomainEnable);
          isAppDomainEnabled = isAppDomainEnable;
        }
      };

      /// <summary>
      /// This function is to safely cast objects from native class to managed class.
      /// </summary>
      /// <remarks>
      /// <para>
      /// Consider the scenario that we have both native objects of class
      /// <c>gemfire::Serializable</c> and managed objects of class
      /// <see cref="IGFSerializable" /> in a region.
      /// </para><para>
      /// The latter would be passed wrapped inside the <c>gemfire::ManagedSerializable</c>
      /// class to the internal implementation. However, when the user wants
      /// to get a particular key then it can be either of class <c>gemfire::Serializable</c>
      /// or of the class <c>gemfire::ManagedSerializable</c>. To the user we would like
      /// to show the managed class object i.e. of <see cref="IGFSerializable" />.
      /// For the 'ManagedSerializable' class object, we will directly get the
      /// IGFSerializable object since this just wraps a IGFSerializable object.
      /// However, for the former case (i.e. <c>gemfire::Serializable</c> class object)
      /// we shall have to wrap it inside a managed class implementing
      /// IGFSerializable -- which is <see cref="Serializable" />
      /// in this case.
      /// </para><para>
      /// This template function does a dynamic_cast to check if the object is of
      /// the given <c>ManagedWrapper</c> type and if so, then simply return the
      /// managed object else create a new object that wraps the native object.
      /// </para>
      /// </remarks>
      template<typename NativeType, typename NativeWrapper,
        typename ManagedType, typename ManagedWrapper>
      inline static ManagedType^ SafeUM2MConvert( NativeType* obj )
      {
        if (obj == nullptr) return nullptr;
        ManagedWrapper* mg_obj = dynamic_cast<ManagedWrapper*>( obj );
        if (mg_obj != nullptr)
        {
          return mg_obj->ptr( );
        }
        else
        {
          return gcnew NativeWrapper( obj );
        }
      }

      /// <summary>
      /// This function is to safely cast objects from managed class to native class.
      /// </summary>
      /// <remarks>
      /// <para>
      /// Consider the scenario that we have both native objects of class
      /// <c>gemfire::Serializable</c> and managed objects of class
      /// <see cref="IGFSerializable" /> in a Region.
      /// </para><para>
      /// The former would be passed wrapped inside the
      /// <see cref="Serializable" /> class.
      /// When this object is passed to native methods, it would be wrapped
      /// inside <c>ManagedSerializable</c> class. However, for the
      /// former case it will result in double wrapping and loss of information
      /// (since the <c>ManagedSerializable</c> would not be as rich as the
      /// original native class). So for the former case we will directly
      /// get the native object, while we need to wrap only for the latter case.
      /// </para><para>
      /// This template function does a dynamic_cast to check if the object is of
      /// the given <c>NativeWrapper</c> type and if so, then simply return the
      /// native object else create a new object that wraps the managed object.
      /// </para>
      /// </remarks>
      template<typename ManagedType, typename ManagedWrapper,
        typename NativeType, typename NativeWrapper>
      inline static NativeType* SafeM2UMConvert( ManagedType^ mg_obj )
      {
        /*
        *return SafeM2UMConvert<IGFSerializable, gemfire::ManagedCacheableKey,
          gemfire::Serializable, Serializable>( mg_obj );
        */
        //TODO:hitesh need to look this further for all types
        if (mg_obj == nullptr) return NULL;
        
        NativeWrapper^ obj = dynamic_cast<NativeWrapper^>( mg_obj );
        
        //if (obj != nullptr) {
        //  //hitesh this should not be 
        //  throw gcnew Exception("Something is worng");
        //  return obj->_NativePtr;
        //}
        //else 
        {
          GemStone::GemFire::Cache::IGFDelta^ sDelta = dynamic_cast<GemStone::GemFire::Cache::IGFDelta^> (mg_obj);
          if(sDelta != nullptr){
            if(!SafeConvertClass::isAppDomainEnabled)
              return new gemfire::ManagedCacheableDelta( sDelta);
            else
              return new gemfire::ManagedCacheableDeltaBytes( sDelta, true);
          }
          else{
            if(!SafeConvertClass::isAppDomainEnabled)
              return new ManagedWrapper(mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId);
            else
              return new gemfire::ManagedCacheableKeyBytes( mg_obj, true);
          }
        }
         //if (mg_obj == nullptr) return NULL;
         //return new ManagedWrapper(mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId);
      }

      /*
      template<typename ManagedType, typename ManagedWrapper,
        typename NativeType, typename NativeWrapper>
      inline static NativeType* SafeM2UMConvert( ManagedType^ mg_obj )
      {
        if (mg_obj == nullptr) return NULL;
        NativeWrapper^ obj = dynamic_cast<NativeWrapper^>( mg_obj );
        if (obj != nullptr) {
          return obj->_NativePtr;
        }
        else {
          GemStone::GemFire::Cache::IGFDelta^ sDelta = dynamic_cast<GemStone::GemFire::Cache::IGFDelta^> (mg_obj);
          if(sDelta != nullptr){
            if(!SafeConvertClass::isAppDomainEnabled)
              return new gemfire::ManagedCacheableDelta( sDelta);
            else
              return new gemfire::ManagedCacheableDeltaBytes( sDelta, true);
          }
          else{
            if(!SafeConvertClass::isAppDomainEnabled)
              return new ManagedWrapper(mg_obj);
            else
              return new gemfire::ManagedCacheableKeyBytes( mg_obj, true);
          }
        }
      }
      */

      template<typename NativeType, typename ManagedType>
      inline static NativeType* GetNativePtr( ManagedType^ mg_obj )
      {
        return (mg_obj != nullptr ? mg_obj->_NativePtr : NULL);
      }

      /// <summary>
      /// Helper function to convert native <c>gemfire::Serializable</c> object
      /// to managed <see cref="IGFSerializable" /> object.
      /// </summary>
      inline static IGFSerializable^ SafeUMSerializableConvert( gemfire::Serializable* obj )
      {

        if (obj == nullptr) return nullptr;
        
        gemfire::ManagedCacheableKey* mg_obj = nullptr;          
        gemfire::ManagedCacheableKeyBytes* mg_bytesObj = nullptr;          

        if(!SafeConvertClass::isAppDomainEnabled)
          mg_obj = dynamic_cast<gemfire::ManagedCacheableKey*>( obj );
        else
          mg_bytesObj = dynamic_cast<gemfire::ManagedCacheableKeyBytes*>( obj );

        gemfire::ManagedCacheableDelta* mg_obj_delta = nullptr;
        gemfire::ManagedCacheableDeltaBytes* mg_bytesObj_delta = nullptr;
        
        if (mg_obj != nullptr)
        {
          return mg_obj->ptr( );
        }
        else if(mg_bytesObj != nullptr)
        {
          return mg_bytesObj->ptr();
        }
        else
        {
          if(!SafeConvertClass::isAppDomainEnabled)
            mg_obj_delta = dynamic_cast<gemfire::ManagedCacheableDelta*>( obj );
          else
            mg_bytesObj_delta = dynamic_cast<gemfire::ManagedCacheableDeltaBytes*>( obj );
          
          if( mg_obj_delta != nullptr )
          {
            return dynamic_cast<IGFSerializable^>(mg_obj_delta->ptr( ));
          }
          else if(mg_bytesObj_delta != nullptr)
          {
            return dynamic_cast<IGFSerializable^>(mg_bytesObj_delta->ptr( ));
          }
          else
          {
            if ( obj->typeId( ) == 0 ) {
              //Special case for UserFunctionExecutionException which is not registered.
              gemfire::UserFunctionExecutionException* mg_UFEEobj = nullptr;
              mg_UFEEobj = dynamic_cast<gemfire::UserFunctionExecutionException*>( obj );
              if (mg_UFEEobj != nullptr) 
              {              
                return gcnew UserFunctionExecutionException(mg_UFEEobj);              
              }
            }

            WrapperDelegate^ wrapperMethod = GemStone::GemFire::Cache::Serializable::GetWrapper( obj->typeId( ) );
            if (wrapperMethod != nullptr)
            {
              return wrapperMethod( obj );
            }            

            return gcnew GemStone::GemFire::Cache::Serializable( obj );
          }
        }
      }

      /// <summary>
      /// Helper function to convert managed <see cref="IGFSerializable" />
      /// object to native <c>gemfire::Serializable</c> object using
      /// <c>SafeM2UMConvert</c>.
      /// </summary>
      inline static gemfire::Serializable* SafeMSerializableConvert( IGFSerializable^ mg_obj )
      {
        //it is called for cacheables types  only
        return SafeM2UMConvert<IGFSerializable, gemfire::ManagedCacheableKey,
          gemfire::Serializable, Serializable>( mg_obj );
      }

      /// <summary>
      /// Helper function to convert native <c>gemfire::CacheableKey</c> object
      /// to managed <see cref="ICacheableKey" /> object.
      /// </summary>
      inline static GemStone::GemFire::Cache::ICacheableKey^ SafeUMKeyConvert( gemfire::CacheableKey* obj )
      {
        //All cacheables will be ManagedCacheableKey only
        if (obj == nullptr) return nullptr;
        gemfire::ManagedCacheableKey* mg_obj = nullptr;
        gemfire::ManagedCacheableKeyBytes* mg_bytesObj = nullptr;

        if (!SafeConvertClass::isAppDomainEnabled)
          mg_obj = dynamic_cast<gemfire::ManagedCacheableKey*>( obj );
        else
          mg_bytesObj = dynamic_cast<gemfire::ManagedCacheableKeyBytes*>( obj );

        if (mg_obj != nullptr)
        {
          return (GemStone::GemFire::Cache::ICacheableKey^)mg_obj->ptr( );
        }
        else if(mg_bytesObj != nullptr)
        {
          return (GemStone::GemFire::Cache::ICacheableKey^)mg_bytesObj->ptr( );
        }
        else
        {
          WrapperDelegate^ wrapperMethod = GemStone::GemFire::Cache::Serializable::GetWrapper( obj->typeId( ) );
          if (wrapperMethod != nullptr)
          {
            return (GemStone::GemFire::Cache::ICacheableKey^)wrapperMethod( obj );
          }
          return gcnew GemStone::GemFire::Cache::CacheableKey( obj );
        }
      }

      /// <summary>
      /// Helper function to convert managed <see cref="ICacheableKey" />
      /// object to native <c>gemfire::CacheableKey</c> object.
      /// </summary>
      inline static gemfire::CacheableKey* SafeMKeyConvert( GemStone::GemFire::Cache::ICacheableKey^ mg_obj )
      {
        if (mg_obj == nullptr) return NULL;
        /*CacheableKey^ obj = dynamic_cast<CacheableKey^>( mg_obj );
        if (obj != nullptr)
        {
          return (gemfire::CacheableKey*)obj->_NativePtr;
        }
        else*/
        {
          if(!SafeConvertClass::isAppDomainEnabled)
            return new gemfire::ManagedCacheableKey( mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId );
          else
            return new gemfire::ManagedCacheableKeyBytes( mg_obj, true );
        }
      }

      template<typename NativeType, typename ManagedType>
      inline static NativeType* GetNativePtr2( ManagedType^ mg_obj )
      {
        if (mg_obj == nullptr) return NULL;
        //for cacheables types
        //return new gemfire::ManagedCacheableKey(mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId);
        {
          if(!SafeConvertClass::isAppDomainEnabled)
            return new gemfire::ManagedCacheableKey( mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId );
          else
            return new gemfire::ManagedCacheableKeyBytes( mg_obj, true );
        }
      }
    }
  }
}
