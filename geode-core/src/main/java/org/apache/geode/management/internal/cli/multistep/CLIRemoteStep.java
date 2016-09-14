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
package org.apache.geode.management.internal.cli.multistep;

import java.io.Serializable;

/**
 * Marker interface to identify remote steps from local steps
 * 
 * Command has to populate the right context information in
 * Remote step to get execution.
 * 
 * For state-ful interactive commands like select where steps
 * are iterating through the result(the state) to and fro, first
 * step has to create the state on the manager.
 * 
 */
public interface CLIRemoteStep extends CLIStep, Serializable{

}
