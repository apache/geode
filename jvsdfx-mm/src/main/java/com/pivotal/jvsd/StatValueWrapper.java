package com.pivotal.jvsd;

import com.pivotal.jvsd.stats.StatFileParser.StatValue;

/**
 *
 * @author Vince Ford
 */
public class StatValueWrapper {

	StatValue sv;
	int axisLocation;
	int plotIndex;

	public StatValueWrapper(StatValue sv, int axisLocation, int plotIndex) {
		this.sv = sv;
		this.axisLocation = axisLocation;
		this.plotIndex = plotIndex;
	}

	public int getAxisLocation() {
		return axisLocation;
	}

	public void setAxisLocation(int axisLocation) {
		this.axisLocation = axisLocation;
	}

	public int getPlotIndex() {
		return plotIndex;
	}

	public void setPlotIndex(int plotIndex) {
		this.plotIndex = plotIndex;
	}

	public StatValue getSv() {
		return sv;
	}

	public void setSv(StatValue sv) {
		this.sv = sv;
	}

}
