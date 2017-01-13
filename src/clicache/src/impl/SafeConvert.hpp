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
#include "../Serializable.hpp"
#include "../Log.hpp"
#include "../CacheableKey.hpp"
#include "../CqEvent.hpp"
#include "PdxManagedCacheableKey.hpp"
#include "PdxManagedCacheableKeyBytes.hpp"
#include "PdxWrapper.hpp"
//TODO::split
#include "../CqEvent.hpp"
#include "../UserFunctionExecutionException.hpp"
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
				interface class IPdxSerializable;
      public ref class SafeConvertClassGeneric
      {
      public:
        static bool isAppDomainEnabled = false;
  
        static void SetAppDomainEnabled(bool isAppDomainEnable)
        {
          GemStone::GemFire::Cache::Generic::Log::Fine("AppDomain support enabled: " + isAppDomainEnable);
          isAppDomainEnabled = isAppDomainEnable;
        }
      };

      /// <summary>
      /// Helper function to convert native <c>gemfire::Serializable</c> object
      /// to managed <see cref="IGFSerializable" /> object.
      /// </summary>
      inline static GemStone::GemFire::Cache::Generic::IGFSerializable^
        SafeUMSerializableConvertGeneric( gemfire::Serializable* obj )
      {

        if (obj == nullptr) return nullptr;
        
        gemfire::ManagedCacheableKeyGeneric* mg_obj = nullptr;          
        gemfire::ManagedCacheableKeyBytesGeneric* mg_bytesObj = nullptr;          

        if(!SafeConvertClassGeneric::isAppDomainEnabled)
          mg_obj = dynamic_cast<gemfire::ManagedCacheableKeyGeneric*>( obj );
        else
          mg_bytesObj = dynamic_cast<gemfire::ManagedCacheableKeyBytesGeneric*>( obj );

        gemfire::ManagedCacheableDeltaGeneric* mg_obj_delta = nullptr;
        gemfire::ManagedCacheableDeltaBytesGeneric* mg_bytesObj_delta = nullptr;
        
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
          if(!SafeConvertClassGeneric::isAppDomainEnabled)
            mg_obj_delta = dynamic_cast<gemfire::ManagedCacheableDeltaGeneric*>( obj );
          else
            mg_bytesObj_delta = dynamic_cast<gemfire::ManagedCacheableDeltaBytesGeneric*>( obj );
          
          if( mg_obj_delta != nullptr )
          {
            return dynamic_cast<GemStone::GemFire::Cache::Generic::IGFSerializable^>(mg_obj_delta->ptr( ));
          }
          else if(mg_bytesObj_delta != nullptr)
          {
            return dynamic_cast<GemStone::GemFire::Cache::Generic::IGFSerializable^>(mg_bytesObj_delta->ptr( ));
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

            WrapperDelegateGeneric^ wrapperMethod =
              GemStone::GemFire::Cache::Generic::Serializable::GetWrapperGeneric( obj->typeId( ) );
            if (wrapperMethod != nullptr)
            {
              return wrapperMethod( obj );
            }            

            return gcnew GemStone::GemFire::Cache::Generic::Serializable( obj );
          }
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
      inline static NativeType* SafeM2UMConvertGeneric( ManagedType^ mg_obj )
      {
        /*
        *return SafeM2UMConvertGeneric<IGFSerializable, gemfire::ManagedCacheableKey,
          gemfire::Serializable, Serializable>( mg_obj );
        */
        //TODO: need to look this further for all types
        if (mg_obj == nullptr) return NULL;
        
        NativeWrapper^ obj = dynamic_cast<NativeWrapper^>( mg_obj );
        
        //if (obj != nullptr) {
        //  // this should not be 
        //  throw gcnew Exception("Something is worng");
        //  return obj->_NativePtr;
        //}
        //else 
        {
          GemStone::GemFire::Cache::Generic::IGFDelta^ sDelta =
            dynamic_cast<GemStone::GemFire::Cache::Generic::IGFDelta^> (mg_obj);
          if(sDelta != nullptr){
            if(!SafeConvertClassGeneric::isAppDomainEnabled)
              return new gemfire::ManagedCacheableDeltaGeneric( sDelta);
            else
              return new gemfire::ManagedCacheableDeltaBytesGeneric( sDelta, true);
          }
          else{
            if(!SafeConvertClassGeneric::isAppDomainEnabled)
              return new ManagedWrapper(mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId);
            else
              return new gemfire::ManagedCacheableKeyBytesGeneric( mg_obj, true);
          }
        }
         //if (mg_obj == nullptr) return NULL;
         //return new ManagedWrapperGeneric(mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId);
      }

      template<typename NativeType, typename ManagedType>
      inline static NativeType* GetNativePtr( ManagedType^ mg_obj )
      {
        return (mg_obj != nullptr ? mg_obj->_NativePtr : NULL);
      }

      generic<class TValue>
      inline static TValue SafeGenericUMSerializableConvert( gemfire::Serializable* obj )
      {

        if (obj == nullptr) return TValue();
        
        gemfire::ManagedCacheableKeyGeneric* mg_obj = nullptr;          
        gemfire::ManagedCacheableKeyBytesGeneric* mg_bytesObj = nullptr;          

        if(!SafeConvertClassGeneric::isAppDomainEnabled)
          mg_obj = dynamic_cast<gemfire::ManagedCacheableKeyGeneric*>( obj );
        else
          mg_bytesObj = dynamic_cast<gemfire::ManagedCacheableKeyBytesGeneric*>( obj );

        gemfire::ManagedCacheableDeltaGeneric* mg_obj_delta = nullptr;
        gemfire::ManagedCacheableDeltaBytesGeneric* mg_bytesObj_delta = nullptr;
        
        if (mg_obj != nullptr)
        {
          return (TValue)mg_obj->ptr( );
        }
        else if(mg_bytesObj != nullptr)
        {
          return (TValue)mg_bytesObj->ptr();
        }
        else
        {
          if(!SafeConvertClassGeneric::isAppDomainEnabled)
            mg_obj_delta = dynamic_cast<gemfire::ManagedCacheableDeltaGeneric*>( obj );
          else
            mg_bytesObj_delta = dynamic_cast<gemfire::ManagedCacheableDeltaBytesGeneric*>( obj );
          
          if( mg_obj_delta != nullptr )
          {
            return safe_cast<TValue>(mg_obj_delta->ptr( ));
          }
          else if(mg_bytesObj_delta != nullptr)
          {
            return safe_cast<TValue>(mg_bytesObj_delta->ptr( ));
          }
          else
          {            
            if ( obj->typeId( ) == 0 ) {
              gemfire::UserFunctionExecutionException* mg_UFEEobj = nullptr;
              mg_UFEEobj = dynamic_cast<gemfire::UserFunctionExecutionException*>( obj );              
              if (mg_UFEEobj != nullptr) 
              {                
                return safe_cast<TValue> (gcnew UserFunctionExecutionException(mg_UFEEobj));              
              }
            }

            WrapperDelegateGeneric^ wrapperMethod =
              GemStone::GemFire::Cache::Generic::Serializable::GetWrapperGeneric( obj->typeId( ) );             
            if (wrapperMethod != nullptr)
            {
              return safe_cast<TValue>(wrapperMethod( obj ));
            }
            return safe_cast<TValue>(gcnew GemStone::GemFire::Cache::Generic::Serializable( obj ));
          }
        }
      }

      /// <summary>
      /// Helper function to convert managed <see cref="IGFSerializable" />
      /// object to native <c>gemfire::Serializable</c> object using
      /// <c>SafeM2UMConvert</c>.
      /// </summary>
      inline static gemfire::Serializable* SafeMSerializableConvertGeneric(
        GemStone::GemFire::Cache::Generic::IGFSerializable^ mg_obj )
      {
        //it is called for cacheables types  only
        return SafeM2UMConvertGeneric<GemStone::GemFire::Cache::Generic::IGFSerializable,
          gemfire::ManagedCacheableKeyGeneric, gemfire::Serializable,
          GemStone::GemFire::Cache::Generic::Serializable>( mg_obj );
      }

      generic<class TValue>
      inline static gemfire::Cacheable* SafeGenericM2UMConvert( TValue mg_val )
      {
        if (mg_val == nullptr) return NULL;

				Object^ mg_obj = (Object^)mg_val;

				/*ICacheableKey^ iKey = dynamic_cast<ICacheableKey^>(obj);

        if(iKey != nullptr)
        {
          if(!SafeConvertClass::isAppDomainEnabled)
          return new vmware::ManagedCacheableKey(iKey);
        else
          return new vmware::ManagedCacheableKeyBytes( iKey, true);
        }*/

        IPdxSerializable^ pdxType = dynamic_cast<IPdxSerializable^>(mg_obj);

        if(pdxType != nullptr)
        {
          //TODO:: probably need to do for appdomain
					if(!SafeConvertClassGeneric::isAppDomainEnabled)
						return new gemfire::PdxManagedCacheableKey(pdxType);
					else
						return new gemfire::PdxManagedCacheableKeyBytes(pdxType, true);
        }
      
				GemStone::GemFire::Cache::Generic::IGFDelta^ sDelta =
            dynamic_cast<GemStone::GemFire::Cache::Generic::IGFDelta^> (mg_obj);
          if(sDelta != nullptr)
					{
            if(!SafeConvertClassGeneric::isAppDomainEnabled)
              return new gemfire::ManagedCacheableDeltaGeneric( sDelta);
            else
              return new gemfire::ManagedCacheableDeltaBytesGeneric( sDelta, true);
          }
          else
					{
						GemStone::GemFire::Cache::Generic::IGFSerializable^ tmpIGFS = 
							dynamic_cast<GemStone::GemFire::Cache::Generic::IGFSerializable^>(mg_obj);
						if(tmpIGFS != nullptr)
						{
							if(!SafeConvertClassGeneric::isAppDomainEnabled)
							{
									return new gemfire::ManagedCacheableKeyGeneric( tmpIGFS );
							}
							else
							{
								return new gemfire::ManagedCacheableKeyBytesGeneric( tmpIGFS, true);
							}
						}
            
            if(Serializable::IsObjectAndPdxSerializerRegistered(mg_obj->GetType()->FullName))
            {
              //TODO:: probably need to do for appdomain
					    if(!SafeConvertClassGeneric::isAppDomainEnabled)
					    	return new gemfire::PdxManagedCacheableKey(gcnew PdxWrapper(mg_obj));
					    else
						    return new gemfire::PdxManagedCacheableKeyBytes(gcnew PdxWrapper(mg_obj), true);
            }
            throw gcnew GemStone::GemFire::Cache::Generic::IllegalStateException(String::Format("Unable to map object type {0}. Possible Object type may not be registered or PdxSerializer is not registered. ", mg_obj->GetType()));
          }	
      }

      generic<class TValue>
      inline static gemfire::Cacheable* SafeGenericMSerializableConvert( TValue mg_obj )
      {
        return SafeGenericM2UMConvert<TValue>( mg_obj );
      }

			inline static IPdxSerializable^ SafeUMSerializablePDXConvert( gemfire::Serializable* obj )
      {
        gemfire::PdxManagedCacheableKey* mg_obj = nullptr; 

         mg_obj = dynamic_cast<gemfire::PdxManagedCacheableKey*>( obj );

         if(mg_obj != nullptr)
           return mg_obj->ptr();

				 gemfire::PdxManagedCacheableKeyBytes* mg_bytes = dynamic_cast<gemfire::PdxManagedCacheableKeyBytes*>( obj );

				 if(mg_bytes != nullptr)
           return mg_bytes->ptr();

         throw gcnew IllegalStateException("Not be able to deserialize managed type");
      }

      /// <summary>
      /// Helper function to convert native <c>gemfire::CacheableKey</c> object
      /// to managed <see cref="ICacheableKey" /> object.
      /// </summary>
      generic<class TKey>
      inline static Generic::ICacheableKey^ SafeGenericUMKeyConvert( gemfire::CacheableKey* obj )
      {
        //All cacheables will be ManagedCacheableKey only
        if (obj == nullptr) return nullptr;
        gemfire::ManagedCacheableKeyGeneric* mg_obj = nullptr;
        gemfire::ManagedCacheableKeyBytesGeneric* mg_bytesObj = nullptr;

        if (!SafeConvertClassGeneric::isAppDomainEnabled)
          mg_obj = dynamic_cast<gemfire::ManagedCacheableKeyGeneric*>( obj );
        else
          mg_bytesObj = dynamic_cast<gemfire::ManagedCacheableKeyBytesGeneric*>( obj );

        if (mg_obj != nullptr)
        {
          return (Generic::ICacheableKey^)mg_obj->ptr( );
        }
        else if(mg_bytesObj != nullptr)
        {
          return (Generic::ICacheableKey^)mg_bytesObj->ptr( );
        }
        else
        {
          WrapperDelegateGeneric^ wrapperMethod =
            GemStone::GemFire::Cache::Generic::Serializable::GetWrapperGeneric( obj->typeId( ) );
          if (wrapperMethod != nullptr)
          {
            return (Generic::ICacheableKey^)wrapperMethod( obj );
          }
          return gcnew Generic::CacheableKey( obj );
        }
      }

      generic <class TKey>
      inline static gemfire::CacheableKey* SafeGenericMKeyConvert( TKey mg_obj )
      {
        if (mg_obj == nullptr) return NULL;
        gemfire::CacheableKey* obj = GemStone::GemFire::Cache::Generic::Serializable::GetUnmanagedValueGeneric<TKey>( mg_obj ).ptr();
        if (obj != nullptr)
        {
          return obj;
        }
        else
        {
          if(!SafeConvertClassGeneric::isAppDomainEnabled)
            return new gemfire::ManagedCacheableKeyGeneric( SafeUMSerializableConvertGeneric(obj) );
          else
            return new gemfire::ManagedCacheableKeyBytesGeneric( SafeUMSerializableConvertGeneric(obj), true );
        }
      }

      template<typename NativeType, typename ManagedType>
      inline static NativeType* GetNativePtr2( ManagedType^ mg_obj )
      {
        if (mg_obj == nullptr) return NULL;
        //for cacheables types
        //return new gemfire::ManagedCacheableKey(mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId);
        {
          if(!SafeConvertClassGeneric::isAppDomainEnabled)
            return new gemfire::ManagedCacheableKeyGeneric( mg_obj, mg_obj->GetHashCode(), mg_obj->ClassId );
          else
            return new gemfire::ManagedCacheableKeyBytesGeneric( mg_obj, true );
        }
      }

      template<typename NativeType> //where NativeType : gemfire::SharedPtr<NativeType>
      //generic<typename ManagedType> where ManagedType : Internal::SBWrap<gemfire::RegionAttributes>
      inline static NativeType* GetNativePtrFromSBWrap( GemStone::GemFire::Cache::Generic::Internal::SBWrap<NativeType>^ mg_obj )
      {
        return (mg_obj != nullptr ? mg_obj->_NativePtr : NULL);
      }

			 template<typename NativeType> //where NativeType : gemfire::SharedPtr<NativeType>
      //generic<typename ManagedType> where ManagedType : Internal::SBWrap<gemfire::RegionAttributes>
			 inline static NativeType* GetNativePtrFromSBWrapGeneric( GemStone::GemFire::Cache::Generic::Internal::SBWrap<NativeType>^ mg_obj )
      {
        return (mg_obj != nullptr ? mg_obj->_NativePtr : NULL);
      }

      template<typename NativeType>
      inline static NativeType* GetNativePtrFromUMWrap( GemStone::GemFire::Cache::Generic::Internal::UMWrap<NativeType>^ mg_obj )
      {
        return (mg_obj != nullptr ? mg_obj->_NativePtr : NULL);
      }

			template<typename NativeType>
			inline static NativeType* GetNativePtrFromUMWrapGeneric( GemStone::GemFire::Cache::Generic::Internal::UMWrap<NativeType>^ mg_obj )
      {
        return (mg_obj != nullptr ? mg_obj->_NativePtr : NULL);
      }
      } // end namespace Generic
    }
  }
}
