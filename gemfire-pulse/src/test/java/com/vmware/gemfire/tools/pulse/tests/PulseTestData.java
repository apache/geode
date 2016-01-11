/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

public class PulseTestData {
	
	public static class TopNavigation{

	}
	
	public static class ClusterStatus {

		public static final String membersProperty = "members";
	}
	public static class Topology{
		public static final String hotSpotHeapLbl = "Heap Usage";
		public static final String hotSpotCPULbl = "CPU Usage";		
		
		public static final String cpuUsagePaintStyleM1 = "left: 497px; top: 0px;";
		public static final String cpuUsagePaintStyleM2 = "left: 0px; top: 0px;";
		public static final String cpuUsagePaintStyleM3 = "left: 265px; top: 0px;";		
		
		public static final String heapUsagePaintStyleM1 = "left: 0px; top: 0px;";
		public static final String heapUsagePaintStyleM2 = "left: 559px; top: 0px;";
		public static final String heapUsagePaintStyleM3 = "left: 280px; top: 0px;";
	}
	
	public static class ServerGroups{
		public static final String hotSpotHeapLbl = "Heap Usage";
		public static final String hotSpotCPULbl = "CPU Usage";
		
		//Cpu Usage sorting
//		public static final String cpuUsagePaintStyleM1 = "left: 497px; top: 0px;";
//		public static final String cpuUsagePaintStyleM2 = "left: 0px; top: 0px;";
//		public static final String cpuUsagePaintStyleM3 = "left: 265px; top: 0px;";		
		
		public static final String cpuUsagePaintStyleSG1M1 = "left: 0px; top: 295px;";
		public static final String cpuUsagePaintStyleSG1M2 = "left: 0px; top: 30px;";
		public static final String cpuUsagePaintStyleSG1M3 = "left: 0px; top: 171px;";
		
		public static final String cpuUsagePaintStyleSG2M1 = "left: 240px; top: 239px;";
		public static final String cpuUsagePaintStyleSG2M2 = "left: 240px; top: 30px;";	
		
		public static final String cpuUsagePaintStyleSG3M3 = "left: 479px; top: 30px;"; 
		
		//heap usage sorting
		public static final String heapUsagePaintStyleSG1M1 = "left: 0px; top: 30px;";
		public static final String heapUsagePaintStyleSG1M2 = "left: 152px; top: 179px;";
		public static final String heapUsagePaintStyleSG1M3 = "left: 0px; top: 179px;";
		
		public static final String heapUsagePaintStyleSG2M1 = "left: 240px; top: 30px;";
		public static final String heapUsagePaintStyleSG2M2 = "left: 240px; top: 274px;";	
		
		public static final String heapUsagePaintStyleSG3M3 = "left: 479px; top: 30px;"; 
	}
	
	public static class RedundancyZone{
		
		public static final String hotSpotHeapLbl = "Heap Usage";
		public static final String hotSpotCPULbl = "CPU Usage";
		
		public static final String heapUsagePaintStyleRZ1RZ2M1 = "left: 0px; top: 30px;";
		public static final String heapUsagePaintStyleRZ1RZ2M2 = "left: 0px; top: 274px;";
		
		public static final String heapUsagePaintStyleRZ3M3 = "left: 360px; top: 30px;";	
		
		public static final String cpuUsagePaintStyleRZ1RZ2M1 ="left: 0px; top: 239px;";
		public static final String cpuUsagePaintStyleRZ1RZ2M2 ="left: 0px; top: 30px;";

		
	}
	
	public static class DataPerspectiveView {
		
	}

	public static class DataBrowser {
		public static final String partialRgnName = "R";
		public static final String chkRgnClassName = "bttn chk checkbox_true_full";
		public static final String notChkRgnClassName = "bttn chk checkbox_false_full";
		
		public static final String regName = "R1";
		public static final String query1Text = "select * from /R1";
		
		public static final String datePattern = "EEE, MMM dd yyyy, HH:mm:ss z";		

	}

	
}
