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
package org.apache.geode.jvsd.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.jvsd.model.StatArchiveFile;

/**
 * Basic container for all statistics files we are managing.
 * 
 * @author Jens Deppe
 */
public enum FileManager {
	INSTANCE;

	private List<StatArchiveFile> statFiles = new ArrayList<>();

	/**
	 * @param fileNames
	 * @throws IOException
	 */
	public void add(String[] fileNames) throws IOException {
		for (String name : fileNames) {
			statFiles.add(new StatArchiveFile(name));
		}
	}

	/**
	 * @param fileName
	 * @throws IOException
	 */
	public void add(String fileName) throws IOException {
		statFiles.add(new StatArchiveFile(fileName));
	}

	/**
	 * @return
	 */
	public List<StatArchiveFile> getArchives() {
		return statFiles;
	}
}
