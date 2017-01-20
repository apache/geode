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



#pragma once

#include <gfcpp/gf_base.hpp>
#include "gf_defs.hpp"
//#include "SystemProperties.hpp"
//#include "../../impl/NativeWrapper.hpp"
//#include "../../Log.hpp"

using namespace System;
using namespace System::Reflection;

namespace Apache
{
  namespace Geode
  {
    namespace Client
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
