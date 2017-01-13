/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include <gfcpp/gf_base.hpp>
#include "gf_defs.hpp"
//#include "SystemProperties.hpp"
//#include "../../impl/NativeWrapper.hpp"
//#include "../../Log.hpp"

using namespace System;
using namespace System::Reflection;

namespace GemStone
{
  namespace GemFire
  {
		namespace Cache
		{
			namespace Generic
			{

    /// <summary>
    /// Some static utility methods.
    /// </summary>
				public ref class Utils STATICCLASS
				{
				public:

					/// <summary>
					/// Load a method from the given assembly path using the default
					/// constructor (if not a static method) of the given type.
					/// </summary>
					/// <param name="assemblyPath">The path of the assembly.</param>
					/// <param name="typeName">
					/// The name of the class containing the method.
					/// </param>
					/// <param name="methodName">The name of the method.</param>
					/// <returns>
					/// The <c>System.Reflection.MethodInfo</c> for the given method,
					/// or null if the method is not found.
					/// </returns>
					static MethodInfo^ LoadMethod( String^ assemblyPath,
						String^ typeName, String^ methodName);

					/// <summary>
					/// Load a method from the given assembly name using the default
					/// constructor (if not a static method) of the given type.
					/// </summary>
					/// <param name="assemblyName">The name of the assembly.</param>
					/// <param name="typeName">
					/// The name of the class containing the method.
					/// </param>
					/// <param name="methodName">The name of the method.</param>
					/// <returns>
					/// The <c>System.Reflection.MethodInfo</c> for the given method,
					/// or null if the method is not found.
					/// </returns>
					static MethodInfo^ LoadMethodFrom( String^ assemblyName,
						String^ typeName, String^ methodName);

					/// <summary>
					/// Utility method to get the calling thread's last system error code.
					/// </summary>
					static property int32_t LastError
					{
						int32_t get( );
					}
				};
			}
		}
  }
}
