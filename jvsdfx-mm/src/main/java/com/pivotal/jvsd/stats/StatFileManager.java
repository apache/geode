package com.pivotal.jvsd.stats;

import com.pivotal.jvsd.stats.StatFileParser.ResourceInst;
import com.pivotal.jvsd.stats.StatFileParser.StatArchiveFile;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author Vince Ford
 */
public class StatFileManager {

	Logger logger = null;
	ArrayList<StatFileWrapper> fileList = null;

	/**
	 *
	 */
	public StatFileManager() {
		logger = Logger.getLogger("com.gemstone.gemfire.support.StatFileManager");
		logger.setUseParentHandlers(false);
		ConsoleHandler ch = new ConsoleHandler();
		ch.setLevel(Level.FINEST);
		logger.addHandler(ch);
		logger.setLevel(Level.FINEST);
		fileList = new ArrayList<StatFileWrapper>();
	}

	/**
	 *
	 * @param filename
	 */
	public void dump() {
		try {
			File[] files = getFileList();
			//uses the GemFire Methods to dump the file
			StatFileParser sar = new StatFileParser(files, null, false);
			List<ResourceInst> ril = sar.getResourceInstList();
			Utility.dumpCharts(ril);
			sar.update();
			StatArchiveFile[] sa = sar.getArchives();
			for (StatArchiveFile o : sa) {
				System.out.println(o.getArchiveInfo().toString());
			}
			sar = null;
		} catch (IOException ex) {
			logger.log(Level.SEVERE, null, ex);
		}
	}

	public void addFile(File file) {
		StatFileWrapper fw = new StatFileWrapper(file);
		fileList.add(fw);
	}

	public void addFile(String filename) {
		File file = new File(filename);
		StatFileWrapper fw = new StatFileWrapper(file);
		fileList.add(fw);
	}

	File[] getFileList() {
		File[] files = new File[fileList.size()];
		int i = 0;
		for (StatFileWrapper f : fileList) {
			files[i] = f.getFile();
			i++;
		}
		return files;
	}

	public void updateTableView(JTable table) {
		DefaultTableModel tablemodel = (DefaultTableModel) table.getModel();
		int index = 1;
		int namelength = 0;

		for (StatFileWrapper sfw : fileList) {
			if (!sfw.isProcessed()) {
				List<ResourceInst> resourceList = sfw.getResourceList();
				int y = 0;
				for (ResourceInst ri : resourceList) {
					Object[] rowData = new Object[7];
					rowData[0] = y;
					rowData[1] = new Date(ri.getFirstTimeMillis());
					rowData[2] = index;
					rowData[3] = ri.getSampleCount();
					rowData[4] = ri.getId();
					rowData[5] = ri.getType().getName();
					rowData[6] = ri.getName();
					if (namelength < ri.getName().length()) {
						namelength = ri.getName().length();
					}
					tablemodel.addRow(rowData);
					y++;
				}
				sfw.setProcessed(true);
			}
			index++;
		}

	}

	public List<String> getStats(int filenum, String type, String name, boolean hideZeros) {
		StatFileWrapper stw = fileList.get(filenum - 1);
		List<String> list = stw.getStats(type, name, hideZeros);
		return list;
	}

	public StatFileWrapper getFile(int Index) {
		return fileList.get(Index);
	}

	public int length() {
		return fileList.size();
	}
}
