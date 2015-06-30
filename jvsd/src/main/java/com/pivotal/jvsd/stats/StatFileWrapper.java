package com.pivotal.jvsd.stats;

import com.pivotal.jvsd.stats.StatFileParser.ArchiveInfo;
import com.pivotal.jvsd.stats.StatFileParser.ResourceInst;
import com.pivotal.jvsd.stats.StatFileParser.StatArchiveFile;
import com.pivotal.jvsd.stats.StatFileParser.StatValue;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Vince Ford
 */
public class StatFileWrapper {

	File file = null;
	boolean processed = false;
	ArchiveInfo aInfo = null;
	HashMap<String, ResourceInst> resources = null;

	public ResourceInst getResource(String type, String name) {
		String key = type + name;
		return resources.get(key);

	}

	public StatFileWrapper(File file) {
		try {
			this.file = file;
			File[] files = new File[1];
			files[0] = file;
			resources = new HashMap<String, ResourceInst>();
			StatFileParser sar = new StatFileParser(files, null, false);
			List<ResourceInst> resourceList = sar.getResourceInstList();
			for (ResourceInst ri : resourceList) {
				String key = ri.getType().getName() + ri.getName();
				resources.put(key, ri);
			}
			StatArchiveFile[] sa = sar.getArchives();
			aInfo = sa[0].getArchiveInfo();

		} catch (IOException ex) {
			Logger.getLogger(StatFileWrapper.class.getName()).
							log(Level.SEVERE, null, ex);
		}
	}

	public StatFileWrapper(String filepath) {
		file = new File(filepath);
	}

	public ArchiveInfo getaInfo() {
		return aInfo;
	}

	public List<ResourceInst> getResourceList() {
		ArrayList<ResourceInst> list = new ArrayList<ResourceInst>(resources.
						values());
		return list;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	public File getFile() {
		return file;
	}

	public List<String> getStats(String typeName, String name, boolean hideZeros) {
		String key = typeName + name;
		ResourceInst ri = resources.get(key);
		ArrayList<String> statNames = new ArrayList<String>();
		for (StatValue sv : ri.getStatValues()) {
			if (!(sv.getSnapshotsAverage() == 0 && sv.getSnapshotsMaximum() == 0 && sv.
							getSnapshotsMinimum() == 0)) {
				statNames.add(sv.getDescriptor().getName());
			}
		}
		Collections.sort(statNames);
		return statNames;
	}
}
