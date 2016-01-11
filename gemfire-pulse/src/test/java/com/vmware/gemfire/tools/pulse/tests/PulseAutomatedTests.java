/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
* This test class contains automated tests for Pulse application related to
* 1. Different grid data validations for example - Topology, Server Group, Redundancy Zone
* 2. Data Browser
* 3. 
* 
*
* @author  Smita Phad
* @version 1.0
* @since   2014-04-02
*/

package com.vmware.gemfire.tools.pulse.tests;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

class PulseBaseTests extends PulseTests {
	WebElement element = null;
	public static int maxWaitTime = 20;

	public WebElement findElementUsingId(String id) {
		return driver.findElement(By.id(id));
	}
	public WebElement findElementUsingXpath(String xpath) {
		return driver.findElement(By.xpath(xpath));
	}

	public void clickElementUsingId(String id) {
		findElementUsingId(id).click();
	}

	public void clickElementUsingXpath(String xpath) {
		findElementUsingXpath(xpath).click();
	}

	public void enterTextUsingId(String id, String textToEnter) {
		findElementUsingId(id).sendKeys(textToEnter);

	}

	public void enterTextUsingXpath(String xpath, String textToEnter) {
		findElementUsingXpath(xpath).sendKeys(textToEnter);
	}

	public String getValueFromPropertiesFile(String key) {
		return JMXProperties.getInstance().getProperty(key);
	}
	
	public void sendKeysUsingId(String Id, String textToEnter){
		findElementById(Id).sendKeys(textToEnter);
	}

	public void waitForElement(WebElement element) {
		driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS);
		WebDriverWait wait = new WebDriverWait(driver, 20);
		wait.until(ExpectedConditions.visibilityOf(element));
	}

	public WebElement findElementById(String id) {
		return driver.findElement(By.id(id));
	}

	public WebElement findElementByXpath(String xpath) {
		return driver.findElement(By.xpath(xpath));
	}

	public String getTextUsingXpath(String xpath) {
		return findElementByXpath(xpath).getText();
	}

	public String getTextUsingId(String id) {
		return findElementById(id).getText();
	}

	public String getPersistanceEnabled(Region r) {
		String persitance = null;

		if (r.getPersistentEnabled()) {
			persitance = "ON";
		} else if (!r.getPersistentEnabled()) {
			persitance = "OFF";
		}
		return persitance;
	}

	public String getPersistanceEnabled(String trueOrFalse) {
		String persitance = null;

		if (trueOrFalse.contains("true")) {
			persitance = "ON";
		} else if (trueOrFalse.contains("false")) {
			persitance = "OFF";
		}
		return persitance;
	}

	public String HeapUsage(String valueInKB) {

		return null;
	}

	// WIP - need to work on this --
	public HashMap<String, HashMap<String, Region>> getRegionDetailsFromUI(String regionName) {

		String[] regionNames = JMXProperties.getInstance().getProperty("regions").split(" ");
		HashMap<String, HashMap<String, Region>> regionUiMap = new HashMap<String, HashMap<String, Region>>();

		for (String region : regionNames) {
			HashMap<String, Region> regionMap = regionUiMap.get(region);
		}

		return regionUiMap;
	}

	public void validateServerGroupGridData() {
		List<WebElement> serverGridRows = driver.findElements(By.xpath("//table[@id='memberListSG']/tbody/tr"));
		int rowsCount = serverGridRows.size();
		String[][] gridDataFromUI = new String[rowsCount][7];

		for (int j = 2, x = 0; j <= serverGridRows.size(); j++, x++) {
			for (int i = 0; i <= 6; i++) {
				gridDataFromUI[x][i] = driver.findElement(
						By.xpath("//table[@id='memberListSG']/tbody/tr[" + j + "]/td[" + (i + 1) + "]")).getText();
			}
		}

		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
		HashMap<String, HashMap<String, Member>> sgMap = new HashMap<String, HashMap<String, Member>>();

		for (String member : memberNames) {
			Member thisMember = new Member(member);
			String[] sgs = thisMember.getGroups();

			for (String sgName : sgs) {
				HashMap<String, Member> sgMembers = sgMap.get(sgName);
				if (sgMembers == null) {
					sgMembers = new HashMap<String, Member>();
					sgMap.put(sgName, sgMembers);
				}
				sgMembers.put(thisMember.getMember(), thisMember);
			}
		}

		for (int i = 0; i < gridDataFromUI.length - 1; i++) {
			String sgName = gridDataFromUI[i][0];
			String memName = gridDataFromUI[i][1];
			Member m = sgMap.get(sgName).get(memName);

			Assert.assertEquals(sgName, gridDataFromUI[i][0]);
			Assert.assertEquals(memName, gridDataFromUI[i][1]);
			Assert.assertEquals(m.getMember(), gridDataFromUI[i][2]);
			Assert.assertEquals(m.getHost(), gridDataFromUI[i][3]);
			String cupUsage = String.valueOf(m.getCpuUsage());
			Assert.assertEquals(cupUsage, gridDataFromUI[i][5]);
		}

	}

	public void validateRedundancyZonesGridData() {
		List<WebElement> rzGridRows = driver.findElements(By.xpath("//table[@id='memberListRZ']/tbody/tr"));
		int rowsCount = rzGridRows.size();
		String[][] gridDataFromUI = new String[rowsCount][7];

		for (int j = 2, x = 0; j <= rzGridRows.size(); j++, x++) {
			for (int i = 0; i <= 6; i++) {
				gridDataFromUI[x][i] = driver.findElement(
						By.xpath("//table[@id='memberListRZ']/tbody/tr[" + j + "]/td[" + (i + 1) + "]")).getText();
			}
		}

		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
		HashMap<String, HashMap<String, Member>> rzMap = new HashMap<String, HashMap<String, Member>>();

		for (String member : memberNames) {
			Member thisMember = new Member(member);
			//String[] rz = thisMember.getRedundancyZone();
			String sgName = thisMember.getRedundancyZone();

			//for (String sgName : rz) {
				HashMap<String, Member> rzMembers = rzMap.get(sgName);

				if (rzMembers == null) {
					rzMembers = new HashMap<String, Member>();
					rzMap.put(sgName, rzMembers);
				}

				rzMembers.put(thisMember.getMember(), thisMember);
			//}
		}

		for (int i = 0; i < gridDataFromUI.length - 1; i++) {
			String sgName = gridDataFromUI[i][0];
			String memName = gridDataFromUI[i][1];
			Member m = rzMap.get(sgName).get(memName);

			Assert.assertEquals(sgName, gridDataFromUI[i][0]);
			Assert.assertEquals(memName, gridDataFromUI[i][1]);
			Assert.assertEquals(m.getMember(), gridDataFromUI[i][2]);
			Assert.assertEquals(m.getHost(), gridDataFromUI[i][3]);
			String cupUsage = String.valueOf(m.getCpuUsage());
			Assert.assertEquals(cupUsage, gridDataFromUI[i][5]);
		}

	}

	public void validateTopologyGridData() {
		List<WebElement> rzGridRows = driver.findElements(By.xpath("//table[@id='memberList']/tbody/tr"));
		int rowsCount = rzGridRows.size();
		String[][] gridDataFromUI = new String[rowsCount][7];

		for (int j = 2, x = 0; j <= rzGridRows.size(); j++, x++) {
			for (int i = 0; i <= 6; i++) {
				gridDataFromUI[x][i] = driver.findElement(
						By.xpath("//table[@id='memberList']/tbody/tr[" + j + "]/td[" + (i + 1) + "]")).getText();
			}
		}

		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
		HashMap<String, Member> tpMap = new HashMap<String, Member>();

		for (String member : memberNames) {
			Member thisMember = new Member(member);
			tpMap.put(thisMember.getMember(), thisMember);

		}

		for (int i = 0; i < gridDataFromUI.length - 1; i++) {

			String memName = gridDataFromUI[i][0];
			Member m = tpMap.get(memName);

			Assert.assertEquals(m.getMember(), gridDataFromUI[i][0]);
			Assert.assertEquals(m.getMember(), gridDataFromUI[i][1]);
			Assert.assertEquals(m.getHost(), gridDataFromUI[i][2]);
			String cupUsage = String.valueOf(m.getCpuUsage());
			Assert.assertEquals(cupUsage, gridDataFromUI[i][4]);
		}
	}

	public void validateDataPrespectiveGridData() {
		List<WebElement> serverGridRows = driver.findElements(By.xpath("//table[@id='regionsList']/tbody/tr"));
		int rowsCount = serverGridRows.size();
		String[][] gridDataFromUI = new String[rowsCount][7];

		for (int j = 2, x = 0; j <= serverGridRows.size(); j++, x++) {
			for (int i = 0; i <= 6; i++) {
				if (i < 5) {
					gridDataFromUI[x][i] = driver.findElement(
							By.xpath("//table[@id='regionsList']/tbody/tr[" + j + "]/td[" + (i + 1) + "]")).getText();
				} else if (i == 5) {
					gridDataFromUI[x][i] = driver.findElement(
							By.xpath("//table[@id='regionsList']/tbody/tr[" + j + "]/td[" + (i + 4) + "]")).getText();
				}
			}
		}

		String[] regionNames = JMXProperties.getInstance().getProperty("regions").split(" ");
		HashMap<String, Region> dataMap = new HashMap<String, Region>();

		for (String region : regionNames) {
			Region thisRegion = new Region(region);
			dataMap.put(thisRegion.getName(), thisRegion);

		}

		for (int i = 0; i < gridDataFromUI.length - 1; i++) {
			String memName = gridDataFromUI[i][0];
			Region r = dataMap.get(memName);

			Assert.assertEquals(r.getName(), gridDataFromUI[i][0]);
			Assert.assertEquals(r.getRegionType(), gridDataFromUI[i][1]);

			Assert.assertEquals(String.valueOf(r.getSystemRegionEntryCount()), gridDataFromUI[i][2]);
			Assert.assertEquals(r.getFullPath(), gridDataFromUI[i][4]);
			Assert.assertEquals(getPersistanceEnabled(r), gridDataFromUI[i][5]);
		}
	}

	public void validateRegionDetailsGridData() {
		List<WebElement> serverGridRows = driver.findElements(By.xpath("//table[@id='memberList']/tbody/tr"));
		int rowsCount = serverGridRows.size();
		String[][] gridDataFromUI = new String[rowsCount][7];

		for (int j = 2, x = 0; j <= serverGridRows.size(); j++, x++) {
			for (int i = 0; i < 2; i++) {
				gridDataFromUI[x][i] = driver.findElement(
						By.xpath("//table[@id='memberList']/tbody/tr[" + j + "]/td[" + (i + 1) + "]")).getText();
			}
		}

		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
		HashMap<String, Member> tpMap = new HashMap<String, Member>();

		for (String member : memberNames) {
			Member thisMember = new Member(member);
			tpMap.put(thisMember.getMember(), thisMember);
		}

		for (int i = 0; i < gridDataFromUI.length - 1; i++) {

			String memName = gridDataFromUI[i][0];
			Member m = tpMap.get(memName);
			Assert.assertEquals(m.getMember(), gridDataFromUI[i][0]);
		}

	}

	public void navigateToToplogyView(){
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.TopologyView.radioButtonXpath);
	}
	
	public void navigateToServerGroupGView(){
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.radioButtonXpath);
	}
	
	public void navigateToRedundancyZoneView(){
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.radioButtonXpath);
	}
	
	//  ------ 	Topology / Server Group / Redundancy Group - Tree View
	
	public void navigateToTopologyTreeView(){
		navigateToToplogyView();
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
	}
	
	public void navigateToServerGroupTreeView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.radioButtonXpath);
	}
	
	public void navigateToRedundancyZonesTreeView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.radioButtonXpath);
	}
	
	//  ------ 	Topology / Server Group / Redundancy Group - Grid View
	
	public void navigateToTopologyGridView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.TopologyView.radioButtonXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
	}

	public void navigateToServerGroupGridView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.radioButtonXpath);
		clickElementUsingId(PulseTestLocators.ServerGroups.gridButtonId);
	}

	public void navigateToRedundancyZonesGridView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.radioButtonXpath);
		clickElementUsingId(PulseTestLocators.RedundancyZone.gridButtonId);
	}
	
	// ----- Data perspective / region details 
	
	public void navigateToDataPrespectiveGridView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.downarrowButtonXpath);
		clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.dataViewButtonXpath);
		clickElementUsingId(PulseTestLocators.DataPerspectiveView.gridButtonId);
	}

	public void navigateToRegionDetailsView() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.downarrowButtonXpath);
		clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.dataViewButtonXpath);
		// clickElementUsingXpath(PulseTestLocators.RegionDetailsView.regionNameXpath);
		// // WIP - region navigation defect needs to fixed
		clickElementUsingXpath(PulseTestLocators.RegionDetailsView.treeMapCanvasXpath);
	}

	public void navigateToRegionDetailsGridView() {
		navigateToRegionDetailsView();
		clickElementUsingXpath(PulseTestLocators.RegionDetailsView.gridButtonXpath);
	}

	public String getPropertyValue(String propertyKey) {
		String propertyValue = JMXProperties.getInstance().getProperty(propertyKey);
		return propertyValue;
	}

	public void verifyElementPresentById(String id) {
		WebDriverWait wait = new WebDriverWait(driver, maxWaitTime, 500);
		wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.id(id)));
	}
	
	public void verifyElementPresentByLinkText(String lnkText) {
		WebDriverWait wait = new WebDriverWait(driver, maxWaitTime, 500);
		wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.linkText(lnkText)));
	}

	public void verifyElementPresentByXpath(String xpath) {
		WebDriverWait wait = new WebDriverWait(driver, maxWaitTime, 500);
		wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.xpath(xpath)));
	}

	public void verifyTextPresrntById(String id, String text) {
		WebDriverWait wait = new WebDriverWait(driver, maxWaitTime, 500);
		wait.until(ExpectedConditions.textToBePresentInElementLocated(By.id(id), text));
	}

	public void verifyTextPresrntByXpath(String xpath, String text) {
		WebDriverWait wait = new WebDriverWait(driver, maxWaitTime, 500);
		wait.until(ExpectedConditions.textToBePresentInElementLocated(By.xpath(xpath), text));
	}

	public void verifyElementAttributeById(String id, String attribute, String value) {
		String actualValue = findElementById(id).getAttribute(attribute);
		Assert.assertTrue(actualValue.equals(value) || actualValue.contains(value));
	}

	
	public void mouseReleaseById(String id){
		verifyElementPresentById(id);
		Actions action = new Actions(driver);
		WebElement we = driver.findElement(By.id(id));
		action.moveToElement(we).release().perform();
	}
	public void mouseClickAndHoldOverElementById(String id) {
		verifyElementPresentById(id);
		Actions action = new Actions(driver);
		WebElement we = driver.findElement(By.id(id));
		action.moveToElement(we).clickAndHold().perform();
	}

	public void mouseOverElementByXpath(String xpath) {
		Actions action = new Actions(driver);
		WebElement we = driver.findElement(By.xpath(xpath));
		action.moveToElement(we).build().perform();
	}

	
	public float stringToFloat(String stringValue){		
		float floatNum = Float.parseFloat(stringValue);	
		return floatNum;		
	}
	
	public String floatToString(float floatValue){		
		String stringValue = Float.toString(floatValue);
		return stringValue;		
	}
	
	
	public String[] splitString(String stringToSplit, String splitDelimiter){		
		String [] stringArray = stringToSplit.split(splitDelimiter);
		return stringArray;		
	}
	
	public void assertMemberSortingByCpuUsage(){		
		Map<Float, String> memberMap = new TreeMap<Float,String>(Collections.reverseOrder());		
		String [] membersNames = splitString(JMXProperties.getInstance().getProperty("members"), " ");
		for (String member : membersNames) {		
			Member thisMember = new Member(member);
			memberMap.put(thisMember.getCpuUsage(), thisMember.getMember());
		}	
		for(Map.Entry<Float,String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberCPUUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM2;
			}else{
				refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM3;
			}			
			Assert.assertTrue(findElementById(entry.getValue()).getAttribute("style").contains(refMemberCPUUsage));			
	    } 			
	}	

	public void assertMemberSortingByHeapUsage(){		
		Map<Long, String> memberMap = new TreeMap<Long,String>(Collections.reverseOrder());		
		String [] membersNames = splitString(JMXProperties.getInstance().getProperty("members"), " ");
		for (String member : membersNames) {		
			Member thisMember = new Member(member);
			memberMap.put(thisMember.getCurrentHeapSize(), thisMember.getMember());
		}	
		for(Entry<Long, String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberHeapUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberHeapUsage = PulseTestData.Topology.heapUsagePaintStyleM1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberHeapUsage = PulseTestData.Topology.heapUsagePaintStyleM2;
			}else{
				refMemberHeapUsage = PulseTestData.Topology.heapUsagePaintStyleM3;
			}			
			Assert.assertTrue(findElementById(entry.getValue()).getAttribute("style").contains(refMemberHeapUsage));
	    } 			
	}		
	
	public void assertMemberSortingBySGCpuUsage(){		
		Map<Float, String> memberMap = new TreeMap<Float,String>(Collections.reverseOrder());		
		String [] membersNames = splitString(JMXProperties.getInstance().getProperty("members"), " ");
		for (String member : membersNames) {		
			Member thisMember = new Member(member);
			memberMap.put(thisMember.getCpuUsage(), thisMember.getMember());
		}	
		for(Map.Entry<Float,String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberCPUUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM2;
			}else{
				refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM3;
			}			
			Assert.assertTrue(findElementById(entry.getValue()).getAttribute("style").contains(refMemberCPUUsage));			
	    } 			
	}	
	
	
	public void assertMemberSortingBySgHeapUsage(){
		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");		
		HashMap<String, HashMap<String, Member>> sgMap = new HashMap<String, HashMap<String, Member>>();
		for (String member : memberNames) {
			Member thisMember = new Member(member);
			String[] sgs = thisMember.getGroups();

			for (String sgName : sgs) {
				HashMap<String, Member> sgMembers = sgMap.get(sgName);
				if (sgMembers == null) {
					sgMembers = new HashMap<String, Member>();
					sgMap.put(sgName, sgMembers);
				}
				sgMembers.put(thisMember.getMember(), thisMember);
			}
		}
		Map<Float, String> memberMap = new TreeMap<Float,String>(Collections.reverseOrder());		
		
		for(int sgId=1; sgId<=3; sgId++){
			String sgName = "SG1";
			String memName = "M" + sgId;				
			Member m = sgMap.get(sgName).get(memName);
			memberMap.put((float) m.getCurrentHeapSize(), m.getMember());			
		}			
		
		for(Map.Entry<Float,String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberCPUUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberCPUUsage = PulseTestData.ServerGroups.heapUsagePaintStyleSG1M1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberCPUUsage = PulseTestData.ServerGroups.heapUsagePaintStyleSG1M2;
			}else{
				refMemberCPUUsage = PulseTestData.ServerGroups.heapUsagePaintStyleSG1M3;
			}			
			Assert.assertTrue(findElementById("SG1(!)"+entry.getValue()).getAttribute("style").contains(refMemberCPUUsage));			
	    } 	
	}
	
	
	
	public void assertMemberSortingBySgCpuUsage(){
		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");		
		HashMap<String, HashMap<String, Member>> sgMap = new HashMap<String, HashMap<String, Member>>();
		for (String member : memberNames) {
			Member thisMember = new Member(member);
			String[] sgs = thisMember.getGroups();

			for (String sgName : sgs) {
				HashMap<String, Member> sgMembers = sgMap.get(sgName);
				if (sgMembers == null) {
					sgMembers = new HashMap<String, Member>();
					sgMap.put(sgName, sgMembers);
				}
				sgMembers.put(thisMember.getMember(), thisMember);
			}
		}
		Map<Float, String> memberMap = new TreeMap<Float,String>(Collections.reverseOrder());		
		//SG3(!)M3		
		for(int sgId=1; sgId<=3; sgId++){
			String sgName = "SG1";
			String memName = "M" + sgId;				
			Member m = sgMap.get(sgName).get(memName);
			memberMap.put(m.getCpuUsage(), m.getMember());			
		}			
		
		for(Map.Entry<Float,String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberCPUUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberCPUUsage = PulseTestData.ServerGroups.cpuUsagePaintStyleSG1M1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberCPUUsage = PulseTestData.ServerGroups.cpuUsagePaintStyleSG1M2;
			}else{
				refMemberCPUUsage = PulseTestData.ServerGroups.cpuUsagePaintStyleSG1M3;
			}			
			Assert.assertTrue(findElementById("SG1(!)"+entry.getValue()).getAttribute("style").contains(refMemberCPUUsage));			
	    } 	
	}
	
	public void assertMemberSortingByRzHeapUsage(){
		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");			
		HashMap<String, HashMap<String, Member>> rzMap = new HashMap<String, HashMap<String, Member>>();
		for (String member : memberNames) {
			Member thisMember = new Member(member);
			String sgName = thisMember.getRedundancyZone();
				HashMap<String, Member> rzMembers = rzMap.get(sgName);
				
				if (rzMembers == null) {
					rzMembers = new HashMap<String, Member>();
					rzMap.put(sgName, rzMembers);
				}

				rzMembers.put(thisMember.getMember(), thisMember);
		}
		Map<Float, String> memberMap = new TreeMap<Float,String>(Collections.reverseOrder());
		String rzName = "RZ1 RZ2";
		String memName = "M1" ;
		Member m = rzMap.get(rzName).get(memName);
		memberMap.put((float) m.getCurrentHeapSize(), m.getMember());
			
		for(Map.Entry<Float,String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberHeapUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberHeapUsage = PulseTestData.RedundancyZone.heapUsagePaintStyleRZ1RZ2M1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberHeapUsage = PulseTestData.RedundancyZone.heapUsagePaintStyleRZ1RZ2M2;
			}else{
				refMemberHeapUsage = PulseTestData.RedundancyZone.heapUsagePaintStyleRZ3M3;
			}			
			Assert.assertTrue(findElementById("RZ1 RZ2(!)"+entry.getValue()).getAttribute("style").contains(refMemberHeapUsage));
	    } 	
	}
	
	public void assertMemeberSortingByRzCpuUsage(){		
		String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");			
		HashMap<String, HashMap<String, Member>> rzMap = new HashMap<String, HashMap<String, Member>>();
		for (String member : memberNames) {
			Member thisMember = new Member(member);
			String sgName = thisMember.getRedundancyZone();
				HashMap<String, Member> rzMembers = rzMap.get(sgName);
				
				if (rzMembers == null) {
					rzMembers = new HashMap<String, Member>();
					rzMap.put(sgName, rzMembers);
				}

				rzMembers.put(thisMember.getMember(), thisMember);
		}
		Map<Float, String> memberMap = new TreeMap<Float,String>(Collections.reverseOrder());
		String rzName = "RZ1 RZ2";
		String memName = "M1" ;
		Member m = rzMap.get(rzName).get(memName);
		memberMap.put(m.getCpuUsage(), m.getMember());
			
		for(Map.Entry<Float,String> entry : memberMap.entrySet()) {		
			//here matching painting style to validation that the members are painted according to their cpu usage			
			String refMemberCPUUsage = null;
			if(entry.getValue().equalsIgnoreCase("M1")){
				refMemberCPUUsage = PulseTestData.RedundancyZone.cpuUsagePaintStyleRZ1RZ2M1;
			}else if(entry.getValue().equalsIgnoreCase("M2")){
				refMemberCPUUsage = PulseTestData.RedundancyZone.cpuUsagePaintStyleRZ1RZ2M2;
			}		
			Assert.assertTrue(findElementById("RZ1 RZ2(!)"+entry.getValue()).getAttribute("style").contains(refMemberCPUUsage));			
	    } 	
	}
	
	public List<WebElement> getRegionsFromDataBrowser(){		
		List<WebElement> regionList = driver.findElements(By.xpath("//span[starts-with(@ID,'treeDemo_')][contains(@id,'_span')]"));		
		return regionList;
	}
}

public class PulseAutomatedTests extends PulseBaseTests {

		
	@Test
	public void serverGroupGridDataValidation() {
		navigateToServerGroupGridView();
		validateServerGroupGridData();
	}

	@Test
	public void redundancyZonesGridDataValidation() {
		navigateToRedundancyZonesGridView();
		validateRedundancyZonesGridData();
	}

	@Test
	public void topologyGridDataValidation() {
		navigateToTopologyGridView();
		validateTopologyGridData();
	}

	@Test
	public void dataViewGridDataValidation() {
		navigateToDataPrespectiveGridView();
		validateDataPrespectiveGridData();
	}

	@Test
	public void regionDetailsGridDataValidation() {
		navigateToRegionDetailsGridView();
		validateRegionDetailsGridData();

	}

	@Test
	public void regionDetailsNavigationTest() {
		navigateToRegionDetailsView();
		Assert.assertEquals("/R2", getTextUsingId(PulseTestLocators.RegionDetailsView.regionNameDivId));
	}

	@Test
	public void regionName() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.name"), getTextUsingId(PulseTestLocators.RegionDetailsView.regionNameDivId));
	}

	@Test
	public void regionPath() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.fullPath"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionPathId));
	}

	@Test
	public void regionType() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.regionType"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionTypeId));
	}

	@Test
	public void regionMembers() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.memberCount"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionMembersTextId));
	}

	@Test
	public void regionEmptyNodes() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.emptyNodes"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionEmptyNodesId));
	}

	@Test
	public void regionEntryCount() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.systemRegionEntryCount"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionEntryCountTextId));
	}

	@Test
	public void regionDiskUsage() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.diskUsage"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionDiskUsageId));
	}

	@Test
	public void regionPersistence() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPersistanceEnabled(getPropertyValue("region.R2.persistentEnabled")),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionPersistenceId));
	}

	@Ignore("WIP")
	@Test
	public void regionMemoryUsage() {
		navigateToRegionDetailsView();
		// need to check the respective property values
	}

	@Test
	public void regionInMemoryRead() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.getsRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.inMemoryReadsId));

	}

	@Test
	public void regionInMemoryWrites() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.putsRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.inMemoryWritesId));
	}

	@Test
	public void regionDiskRead() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.diskReadsRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.diskReadsId));
	}

	@Test
	public void regionDiskWrites() {
		navigateToRegionDetailsView();
		Assert.assertEquals(getPropertyValue("region.R2.diskWritesRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.diskWritesId));
	}

	@Test
	public void clickHostShowsMemberTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH3Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
	}

	@Test
	public void verifyHostTooltipsOfTopologyGraphTest() {		
		for (int i = 1; i <=3; i++) {
			clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
			mouseClickAndHoldOverElementById("h" + i);
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostNameTTXpath, getPropertyValue("member.M" + i + ".host"));
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.cpuUsageTTXpath, "0%");
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memoryUsageTTXpath, getPropertyValue("member.M" + i
					+ ".UsedMemory"));
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.loadAvgTTXpath, getPropertyValue("member.M" + i
					+ ".loadAverage"));
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.soketsTTXpath, getPropertyValue("member.M" + i
					+ ".totalFileDescriptorOpen"));
			mouseReleaseById("h" + i);
			driver.navigate().refresh();
		}
	}

	@Ignore("Issues with member tooltip xpath")
	@Test
	public void verifyMemberTooltipsOfTopologyGraphTest() {

		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH1Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH1Id);
		mouseClickAndHoldOverElementById(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memNameTTXpath, getPropertyValue("member.M1.member"));
		//verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memCpuUsageTTXpath, getPropertyValue("member.M1.cpuUsage") + "%");
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.jvmPausesTTXpath, getPropertyValue("member.M1.JVMPauses"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.regionsTTXpath, getPropertyValue("member.M1.totalRegionCount"));
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH2Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH2Id);
		mouseClickAndHoldOverElementById(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memNameTTXpath, getPropertyValue("member.M2.member"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memCpuUsageTTXpath, getPropertyValue("member.M2.cpuUsage") + "%");
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.jvmPausesTTXpath, getPropertyValue("member.M2.JVMPauses"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.regionsTTXpath, getPropertyValue("member.M2.totalRegionCount"));

		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH3Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH3Id);
		mouseClickAndHoldOverElementById(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memNameTTXpath, getPropertyValue("member.M3.member"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memCpuUsageTTXpath, getPropertyValue("member.M3.cpuUsage") + "%");
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.jvmPausesTTXpath, getPropertyValue("member.M3.JVMPauses"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.regionsTTXpath, getPropertyValue("member.M3.totalRegionCount"));

	}

	@Test
	public void VerifyRGraphTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH3Id);
	}

	@Test
	public void clickMembersOfTopologyGraphTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH1Id);
		clickElementUsingId(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH2Id);
		clickElementUsingId(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH3Id);
		clickElementUsingId(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickTreeMapViewShowingTreeMapTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
	}

	@Test
	public void verifyMembersPresentInTreeMapTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM1Id, "M1");
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM2Id, "M2");
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM3Id, "M3");
	}

	@Test
	public void clickMemberNavigatingToCorrespondingRegionTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM1Id, "M1");
		clickElementUsingId(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM2Id, "M2");
		clickElementUsingId(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM3Id, "M3");
		clickElementUsingId(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickGridButtonShowsGridTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.idM1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.idM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.idM3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH3Xpath);
	}

	@Test
	public void verifyMembersPresentInGridTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM1Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.nameM1Xpath, "M1");

		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM2Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.nameM2Xpath, "M2");

		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM3Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.nameM3Xpath, "M3");
	}

	@Test
	public void verifyHostNamesInGridTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH1Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostH1Xpath, "h1");
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH2Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostH2Xpath, "h2");
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH3Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostH3Xpath, "h3");
	}

	@Test
	public void clickOnGridMemNameNavigatingToCorrespondingRegionTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.nameM1Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");

		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.nameM2Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");

		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.nameM3Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void verifyMembersPresentInSvrGrpTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);
		verifyElementPresentById(PulseTestLocators.ServerGroups.serverGrp1Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.serverGrp2Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.serverGrp3Id);

		verifyElementPresentById(PulseTestLocators.ServerGroups.sg1M1Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.sg1M2Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.sg1M3Id);

		verifyElementPresentById(PulseTestLocators.ServerGroups.sg2M1Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.sg2M2Id);

		verifyElementPresentById(PulseTestLocators.ServerGroups.sg3M3Id);
	}

	@Test
	public void expandAndCloseServerGroupsTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		//waitForElement(findElementByXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath));
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp1Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp1Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp1Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp1Id, "style", "width: 239.667px; height: 399px;");

		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp2Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp2Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp2Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp2Id, "style", "width: 239.667px; height: 399px;");

		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp3Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp3Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp3Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp3Id, "style", "width: 239.667px; height: 399px;");
	}

	@Test
	public void verifyMembersInServGrpTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);

		verifyTextPresrntById(PulseTestLocators.ServerGroups.serverGrp1Id, "SG1");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.serverGrp2Id, "SG2");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.serverGrp3Id, "SG3");

		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg1M1Id, "M1");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg1M2Id, "M2");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg1M3Id, "M3");

		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg2M1Id, "M1");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg2M2Id, "M2");

		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg3M3Id, "M3");
	}

	@Test
	public void memberNavigationFromServGrpTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);
		clickElementUsingId(PulseTestLocators.ServerGroups.sg1M1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg1M2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg1M3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg2M1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg2M2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg3M3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickServGrpGridButtonShowsGridTest() {
		navigateToServerGroupGridView();
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.idSG1M3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.idSG1M2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.idSG1M1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.nameM3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.nameM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.nameM1Xpath);

	}

	@Test
	public void memberNavigationFromServGrpGridTest() {
		navigateToServerGroupGridView();
		clickElementUsingXpath(PulseTestLocators.ServerGroups.idSG1M3Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
		navigateToServerGroupGridView();
		clickElementUsingXpath(PulseTestLocators.ServerGroups.idSG1M1Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToServerGroupGridView();
		clickElementUsingXpath(PulseTestLocators.ServerGroups.idSG1M2Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
	}

	@Test
	public void verifyZonePresentTest() {
		navigateToRedundancyZonesTreeView();
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.zoneRZ1RZ2Xpath);
		verifyElementPresentById(PulseTestLocators.RedundancyZone.zoneRZ2Id);
	}

	@Test
	public void expandAndCloseRdncyZoneTest() {
		navigateToRedundancyZonesTreeView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.zoneRZ1RZ2Xpath);
		verifyElementAttributeById(PulseTestLocators.RedundancyZone.zoneRZ1Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.zoneRZ1RZ2Xpath);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.zoneRZ2Xpath);
		verifyElementAttributeById(PulseTestLocators.RedundancyZone.zoneRZ2Id, "style", "width: 720px; height: 415px;");

	}

	@Test
	public void clickRZMembersNavigationTest() {
		navigateToRedundancyZonesTreeView();		
		clickElementUsingId(PulseTestLocators.RedundancyZone.m1RZ1RZ2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.m2RZ1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.m3RZ2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickRZGridShowingGridTest() {
		navigateToRedundancyZonesGridView();
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.idM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.idM1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.idM3Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.RedundancyZone.idM2Xpath, "M2");
		verifyTextPresrntByXpath(PulseTestLocators.RedundancyZone.idM1Xpath, "M1");
		verifyTextPresrntByXpath(PulseTestLocators.RedundancyZone.idM3Xpath, "M3");
	}

	@Test
	public void clickRZGridMembersNavigationTest() {
		navigateToRedundancyZonesGridView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.idM2Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToRedundancyZonesGridView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.idM1Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToRedundancyZonesGridView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.idM3Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}


	@Test
	public void verifySortingOptionsTest(){
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);			
		verifyElementPresentByLinkText("Heap Usage");
		verifyElementPresentByLinkText("CPU Usage");		
	}
	
	/* 
	 * HotSpot test scripts - 
	 */
	//--- Topology view
	
	@Test
	public void testHotSpotOptPrsntOnTopologyView(){
		navigateToTopologyTreeView();
	    Assert.assertEquals(PulseTestData.Topology.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.TopologyView.hotSpotId));	
	}
	
	@Test
	public void testHotSpotOptionsTopologyView(){	
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		Assert.assertEquals(PulseTestData.Topology.hotSpotHeapLbl, getTextUsingXpath(PulseTestLocators.TopologyView.heapUsageXpath));
		Assert.assertEquals(PulseTestData.Topology.hotSpotCPULbl, getTextUsingXpath(PulseTestLocators.TopologyView.cpuUsageXpath));
	}
	
	@Test
	public void testCpuUsageNavigationOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.cpuUsageXpath);
		Assert.assertEquals(PulseTestData.Topology.hotSpotCPULbl, getTextUsingId(PulseTestLocators.TopologyView.hotSpotId));
	}
	
	@Test
	public void testHeapUsageNavigationOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.heapUsageXpath);
		Assert.assertEquals(PulseTestData.Topology.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.TopologyView.hotSpotId));
	}

	@Test
	public void testSortingUsingCpuUsageOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.cpuUsageXpath);
		assertMemberSortingByCpuUsage();
	}
	
	@Test
	public void testSortingUsingHeapUsageOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.heapUsageXpath);
		assertMemberSortingByHeapUsage();
	}
	
	//--- Server Group view
	
	@Test
	public void testHotSpotOptPrsntOnServerGroupView(){
		navigateToServerGroupTreeView();
	    Assert.assertEquals(PulseTestData.ServerGroups.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.ServerGroups.hotSpotId));	
	}
	
	@Test
	public void testHotSpotOptionsServerGroupView(){	
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		Assert.assertEquals(PulseTestData.ServerGroups.hotSpotHeapLbl, getTextUsingXpath(PulseTestLocators.ServerGroups.heapUsageXpath));
		Assert.assertEquals(PulseTestData.ServerGroups.hotSpotCPULbl, getTextUsingXpath(PulseTestLocators.ServerGroups.cpuUsageXpath));		
	}
	
	@Test
	public void testCpuUsageNavigationOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.cpuUsageXpath);
		Assert.assertEquals(PulseTestData.ServerGroups.hotSpotCPULbl, getTextUsingId(PulseTestLocators.ServerGroups.hotSpotId));
	}
	
	@Test
	public void testHeapUsageNavigationOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.heapUsageXpath);
		Assert.assertEquals(PulseTestData.ServerGroups.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.ServerGroups.hotSpotId));
	}	

	@Test
	public void testSortingUsingHeapUsageOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.heapUsageXpath);
		assertMemberSortingBySgHeapUsage();
	}
	
	@Test
	public void testSortingUsingCpuUsageOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.cpuUsageXpath);
		assertMemberSortingBySgCpuUsage();
	}
	
	//--- Redundancy Zone view
	
	@Test
	public void testHotSpotOptPrsntOnRedundancyZoneView(){
		navigateToRedundancyZonesTreeView();
	    Assert.assertEquals(PulseTestData.RedundancyZone.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.RedundancyZone.hotSpotId));	
	}
	
	
	@Test
	public void testHotSpotOptionsRedundancyZoneView(){	
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		Assert.assertEquals(PulseTestData.RedundancyZone.hotSpotHeapLbl, getTextUsingXpath(PulseTestLocators.RedundancyZone.heapUsageXpath));
		Assert.assertEquals(PulseTestData.RedundancyZone.hotSpotCPULbl, getTextUsingXpath(PulseTestLocators.RedundancyZone.cpuUsageXpath));		
	}
	
	@Test
	public void testCpuUsageNavigationOnRedundancyZoneView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.cpuUsageXpath);
		Assert.assertEquals(PulseTestData.RedundancyZone.hotSpotCPULbl, getTextUsingId(PulseTestLocators.RedundancyZone.hotSpotId));
	}
	
	@Test
	public void testHeapUsageNavigationOnRedundancyZoneView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.heapUsageXpath);
		Assert.assertEquals(PulseTestData.RedundancyZone.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.RedundancyZone.hotSpotId));
	}
	
	@Test
	public void testSortingUsingHeapUsageOnRedundancyView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.heapUsageXpath);
		assertMemberSortingByRzHeapUsage();
	}
	
	@Test
	public void testSortingUsingCpuUsageOnRedundancyView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.cpuUsageXpath);
		assertMemeberSortingByRzCpuUsage();
	}	
	
	@Test
	public void testDataBrowserFilterFeature(){
		// navigate to Data browser page
		loadDataBrowserpage();		
		List<WebElement> regionLst = getRegionsFromDataBrowser();		
		String []regionNames = new String[regionLst.size()];		
		for(int regionIndex = 0; regionIndex < regionLst.size(); regionIndex++){
			regionNames[regionIndex] = findElementByXpath(PulseTestLocators.DataBrowser.rgnSpanFirstPart + (regionIndex + 1 ) + PulseTestLocators.DataBrowser.rgnSpanSecondPart).getText();
		}	
		// type each region name in region filter and verify respective region(s) are displayed in region list
		for (String region : regionNames) {
				findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).clear();
				findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).sendKeys(region);				
				
				List<WebElement> regionLst1 = getRegionsFromDataBrowser();									
				
				for(int regionIndex = 1; regionIndex <= regionLst1.size(); regionIndex++){
					Assert.assertEquals(region,  findElementByXpath(PulseTestLocators.DataBrowser.rgnSpanFirstPart + regionIndex + PulseTestLocators.DataBrowser.rgnSpanSecondPart).getText());
				}
		}
	}
	
	@Test
	public void testDataBrowserFilterPartialRegionName(){
		// navigate to Data browser page
		loadDataBrowserpage();		
		findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).clear();
		
		// type partial region name in region filter and verify that all the regions that contains that  text displays
		findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).sendKeys(PulseTestData.DataBrowser.partialRgnName);
		List<WebElement> regionLst = getRegionsFromDataBrowser();		
		
		for(int regionIndex = 0; regionIndex < regionLst.size(); regionIndex++){			
			Assert.assertTrue(findElementByXpath(PulseTestLocators.DataBrowser.rgnSpanFirstPart + 
					(regionIndex + 1 ) + 
					PulseTestLocators.DataBrowser.rgnSpanSecondPart).
					getText().
					contains(PulseTestData.DataBrowser.partialRgnName));
		}	
	}
	
	@Test
	public void testDataBrowserClearButton(){
		// navigate to Data browser page
		loadDataBrowserpage();		
		
		sendKeysUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId, PulseTestData.DataBrowser.query1Text);		
		String editorTextBeforeClear = getTextUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId);
		clickElementUsingXpath(PulseTestLocators.DataBrowser.btnClearXpath);
		String editorTextAfterClear = getTextUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId);
		
		Assert.assertFalse(PulseTestData.DataBrowser.query1Text.equals(editorTextAfterClear));
	}
	
	@Ignore("WIP") // Data Browser's Query History not showing any data on button click, therefore this test is failing
	@Test
	public void testDataBrowserHistoryQueue(){
		// navigate to Data browser page
		loadDataBrowserpage();	
						
		List<WebElement> numOfReg = driver.findElements(By.xpath(PulseTestLocators.DataBrowser.divDataRegions));
		   
	    for(int i = 1;  i <= numOfReg.size(); i ++){
	    	if(getTextUsingId("treeDemo_" + i + "_span").equals( PulseTestData.DataBrowser.regName)){	  	    		
	    		searchByIdAndClick("treeDemo_" + i + "_check"); 	//driver.findElement(By.id("treeDemo_" + i + "_check")).click();
	    	}
	    }	
		
		sendKeysUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId, PulseTests.QUERY_TYPE_ONE);
		clickElementUsingId(PulseTestLocators.DataBrowser.btnExecuteQueryId);
			
		//Get required datetime format and extract date and hours from date time.
	    DateFormat dateFormat = new SimpleDateFormat(PulseTestData.DataBrowser.datePattern);
	    String queryDateTime = dateFormat.format(System.currentTimeMillis());
	    String queryTime[] = queryDateTime.split(":");
		System.out.println("Query Time from System: " + queryTime[0]);

	    
	    clickElementUsingId(PulseTestLocators.DataBrowser.historyIcon);	    
	    List<WebElement> historyLst = driver.findElements(By.xpath(PulseTestLocators.DataBrowser.historyLst));
		String queryText       = findElementByXpath(PulseTestLocators.DataBrowser.historyLst)
						.findElement(By.cssSelector(PulseTestLocators.DataBrowser.queryText)).getText();
  	String historyDateTime = findElementByXpath(PulseTestLocators.DataBrowser.historyLst)
						.findElement(By.cssSelector(PulseTestLocators.DataBrowser.historyDateTime)).getText();
	  System.out.println("Query Text from History Table: " + queryText);
		System.out.println("Query Time from History Table: " + historyDateTime);
  	    //verify the query text, query datetime in history panel
	    Assert.assertTrue(PulseTests.QUERY_TYPE_ONE.equals(queryText));
	    Assert.assertTrue(historyDateTime.contains(queryTime[0]));
	   
	}	
}
