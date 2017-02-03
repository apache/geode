@echo off

rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem 
rem      http://www.apache.org/licenses/LICENSE-2.0
rem 
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

rem GFCPP must be set

cl /MD /Zc:wchar_t /EHsc /GR /wd4996 /D_EXAMPLE /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DWINVER=0x0500 /DBUILD_TESTOBJECT /I%GFCPP%/include /Fecacherunner.exe *.cpp %GFCPP%/lib/apache-geode.lib

del *.obj
