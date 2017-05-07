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

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Vince Ford
 */
public class Utility {
	private static Logger logger = null;
	private static ConsoleHandler ch = null;
	private static int horizontalScreenSize = 0;
	private static int verticalScreenSize = 0;

	static {
		logger = Logger.getLogger(Utility.class.getName());
		logger.setUseParentHandlers(false);
		ch = new ConsoleHandler();
		ch.setLevel(Level.FINE);
		logger.addHandler(ch);
		logger.setLevel(Level.FINE);
	}

	public static int getHorizontalScreenSize() {
		return horizontalScreenSize;
	}

	public static int getVerticalScreenSize() {
		return verticalScreenSize;
	}

	public static void initScreenSize() {
		horizontalScreenSize = java.awt.Toolkit.getDefaultToolkit().getScreenSize().width;
		verticalScreenSize = java.awt.Toolkit.getDefaultToolkit().getScreenSize().height;
	}

	public static long computeTimeSampleAverage(long[] timestamps) {
		long tempSum = 0;
		if (timestamps.length > 0 && timestamps.length / 2 > 0) {
			for (int i = 0; i < timestamps.length - 1; i++) {
				tempSum = tempSum + (timestamps[i + 1] - timestamps[i]) / 2;
			}
			return tempSum / (timestamps.length / 2);
		}
		return 0;
	}

	/**
	 *
	 * @param timestamp
	 * @param timestamp2
	 * @param value
	 * @param value2
	 * @param sampleperiod
	 * @return
	 */
	public static double computePerSecondValue(long timestamp, long timestamp2,
      double value, double value2, long sampleperiod) {
		return ((value - value2) / ((timestamp - timestamp2) / 1000));
	}

//	public static void dumpCharts(List<StatFileParser.ResourceInst> ril) {
//		logger.info("Dumping Charts");
//		for (StatFileParser.ResourceInst o : ril) {
//			logger.fine("RESOURCE:" + o.getType().getName() + o.getName() + "," + o.
//							toString());
//			StatFileParser.StatValue[] sv = o.getStatValues();
//			long[] timestamps = sv[0].getRawAbsoluteTimeStamps();
//			double[] datavalue = sv[0].getRawSnapshots();
//			long timesampleaverage = Utility.computeTimeSampleAverage(timestamps);
//			long sampleperiod = timesampleaverage / 1000;
//			logger.
//							finest("timestampaverage:" + timesampleaverage + " sampleperiod:" + sampleperiod);
//			for (StatFileParser.StatValue svtemp : sv) {
//				logger.
//								finest("Name:" + svtemp.getDescriptor().getName() + " typecode:" + svtemp.
//												getDescriptor().getTypeCode());
//				logger.finest("DEBUG:" + svtemp.getDescriptor().getUnits());
//				logger.finest("DEBUG:" + svtemp.getDescriptor().isCounter());
//				logger.finest("DEBUG:" + svtemp.getDescriptor().getUnits());
//				logger.finest("DEBUG:filter:" + svtemp.getFilter());
//				XYSeries sample = new XYSeries(svtemp.getDescriptor().getName());
//				logger.
//								finest("Average:" + svtemp.getSnapshotsAverage() + " Max:" + svtemp.
//												getSnapshotsMaximum() + " Size:" + svtemp.
//												getSnapshotsSize());
//				if (!(svtemp.getSnapshotsAverage() == 0 && svtemp.getSnapshotsMaximum() == 0 && svtemp.
//								getSnapshotsMinimum() == 0)) {
//					timestamps = svtemp.getRawAbsoluteTimeStamps();
//					datavalue = svtemp.getRawSnapshots();
//					if (svtemp.getFilter() == svtemp.FILTER_PERSEC) {
//
//						for (int d = 0; d < timestamps.length; d++) {
//							long timestamp = timestamps[d];
//							double value = datavalue[d];
//							if (d == 0) {
//								value = Utility.
//                    computePerSecondValue(timestamp,
//                        timestamp - timesampleaverage, value, svtemp.
//                        getSnapshotsAverage(), sampleperiod);
//								sample.add(timestamp, value);
//							} else {
//								value = Utility.
//                    computePerSecondValue(timestamp, timestamps[d - 1], value,
//                        datavalue[d - 1], sampleperiod);
//								sample.add(timestamp, value);
//							}
//						}
//					} else {
//						for (int d = 0; d < timestamps.length; d++) {
//							sample.add(timestamps[d], datavalue[d]);
//						}
//					}
//					XYSeriesCollection xysc = new XYSeriesCollection(sample);
//					JFreeChart chart = ChartFactory.
//									createTimeSeriesChart(null, null, null, xysc, true, true, true);
//					try {
//						ChartUtilities.saveChartAsJPEG(new File(o.getName() + "_" + svtemp.
//										getDescriptor().getName() + ".jpg"), chart, 1000, 300);
//					} catch (IOException ex) {
//						logger.severe("Couldn't create chart:" + o.getName() + "_" + svtemp.
//										getDescriptor().getName() + ".jpg");
//					}
//				}
//			}
//		}
//		logger.info("Finished Charts");
//	}

//	static public void dumpCSV(StatFileManager sfm, File file) {
//		int numFiles = sfm.length();
//		try {
//			System.out.println("Writing CSV file: " + file.getAbsolutePath());
//			file.createNewFile();
//			PrintWriter pw = new PrintWriter(file);
//			for (int x = 0; x < numFiles; x++) {
//				StatFileWrapper sfWrapper = sfm.getFile(x);
//				StatFileParser.ArchiveInfo aInfo = sfWrapper.getaInfo();
//				pw.print("FILE INFO, ");
//				pw.print(aInfo.getArchiveFileName() + ", ");
//				pw.print(aInfo.getMachine() + ", ");
//				pw.print(aInfo.getOs() + ", ");
//				pw.print(aInfo.getProductVersion() + ", ");
//				pw.print(aInfo.getStartTimeMillis() + ", ");
//				pw.print(aInfo.getSystem() + ", ");
//				pw.print(aInfo.getSystemId() + ", ");
//				pw.print(aInfo.getSystemStartTimeMillis() + ", ");
//				pw.print(aInfo.getTimeZone());
//				pw.println();
//				pw.flush();
//				List<StatFileParser.ResourceInst> rl = sfWrapper.getResourceList();
//				for (StatFileParser.ResourceInst ri : rl) {
//					pw.print("STATTYPE, ");
//					pw.print(ri.getType().getName() + ", ");
//					pw.print(ri.getName());
//					pw.println();
//					StatFileParser.StatValue[] svArray = ri.getStatValues();
//					for (StatFileParser.StatValue sv : svArray) {
//						pw.print("STAT, ");
//						pw.print(sv.getDescriptor().getName() + ", ");
//						if (sv.getFilter() == sv.FILTER_NONE) {
//							pw.print("NO FILTER, ");
//						} else {
//							if (sv.getFilter() == sv.FILTER_PERSAMPLE) {
//								pw.print("PERSAMPLE FILTER, ");
//							} else {
//								if (sv.getFilter() == sv.FILTER_PERSEC) {
//									pw.print("PERSECOND FILTER, ");
//								}
//							}
//						}
//						pw.println();
//						pw.print("TIME, ");
//						long[] timesnapshot = sv.getRawAbsoluteTimeStamps();
//						for (long time : timesnapshot) {
//							pw.print(time);
//							pw.print(", ");
//						}
//						pw.println();
//						pw.print("DATA, ");
//						double[] datasnapshot = sv.getRawSnapshots();
//						for (double data : datasnapshot) {
//							pw.print(data);
//							pw.print(", ");
//						}
//						pw.println();
//					}
//					pw.println();
//					pw.flush();
//				}
//				pw.close();
//			}
//			System.out.println("Finished writing CSV file: " + file.getAbsolutePath());
//		} catch (IOException ex) {
//			Logger.getLogger(Utility.class.getName()).log(Level.SEVERE, null, ex);
//		}
//
//	}
//
//	static public JMenu createMenu(String menuName, JMenuBar menubar) {
//		JMenu menu = new JMenu(menuName);
//		menubar.add(menu);
//		return menu;
//	}
//
//	static public JMenuItem createMenuItem(String menuItem, JMenu menu, ActionListener al) {
//		JMenuItem mi = new JMenuItem(menuItem);
//		mi.addActionListener(al);
//		menu.add(mi);
//		return mi;
//	}
}
