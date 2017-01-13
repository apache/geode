/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */


//#include "gf_includes.hpp"
#include "gfcli/Utils.hpp"
#include <Utils.hpp>

namespace GemStone
{
  namespace GemFire
  {
	 namespace Cache
		{
			namespace Generic
			{

    MethodInfo^ Utils::LoadMethod( String^ assemblyName,
      String^ typeName, String^ methodName)
    {
      Type^ loadType;

      loadType = Type::GetType( typeName + ',' + assemblyName, false, true );
      if (loadType != nullptr)
      {
        return loadType->GetMethod( methodName, BindingFlags::Public |
          BindingFlags::Static | BindingFlags::IgnoreCase );
      }
      return nullptr;
    }

    MethodInfo^ Utils::LoadMethodFrom( String^ assemblyPath,
      String^ typeName, String^ methodName)
    {
      String^ assemblyName;
      Type^ loadType;

      assemblyName = System::IO::Path::GetFileNameWithoutExtension(
        assemblyPath );
      loadType = Type::GetType( typeName + ',' + assemblyName, false, true );
      if (loadType == nullptr)
      {
        Assembly^ assmb = Assembly::LoadFrom( assemblyPath );
        if (assmb != nullptr)
        {
          loadType = assmb->GetType(typeName, false, true);
        }
      }
      if (loadType != nullptr)
      {
        return loadType->GetMethod( methodName, BindingFlags::Public |
          BindingFlags::Static | BindingFlags::IgnoreCase );
      }
      return nullptr;
    }

    int32_t Utils::LastError::get( )
    {
       return gemfire::Utils::getLastError( );
    }
			}
	 }
  }
}
