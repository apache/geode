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
package org.apache.geode.jvsd.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Jens Deppe
 */
public class ResourceWrapper {
	private int row;
	private StatArchiveFile.ResourceInst inst;

	/**
	 * @param inst
	 * @param idx
	 */
	public ResourceWrapper(StatArchiveFile.ResourceInst inst, int idx) {
		this.inst = inst;
		this.row = idx;
	}

	/**
	 * @return
	 */
	public int getRow() {
		return row;
	}

	/**
	 * @return
	 */
	public Date getStartTime() {
		return new Date(inst.getFirstTimeMillis());
	}

	/**
	 * @return
	 */
	public int getSamples() {
		return inst.getSampleCount();
	}

	/**
	 * @return
	 */
	public String getType() {
		return inst.getType().getName();
	}

	/**
	 * @return
	 */
	public String getName() {
		return inst.getName();
	}

	/**
	 * @return
	 */
	public List<String> getStatNames() {
		List<String> statNames = new ArrayList<>();

		for (StatArchiveFile.StatValue sv : inst.getStatValues()) {
			if (!(sv.getSnapshotsAverage() == 0 && sv.getSnapshotsMaximum() == 0 && sv.getSnapshotsMinimum() == 0)) {
				statNames.add(sv.getDescriptor().getName());
			}
		}

		return statNames;
	}

	/**
	 * @param name
	 * @return
	 */
	public StatArchiveFile.StatValue getStatValue(String name) {
		return inst.getStatValue(name);
	}
}
