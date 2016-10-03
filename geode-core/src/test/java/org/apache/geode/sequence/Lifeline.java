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
package org.apache.geode.sequence;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: dsmith
 * Date: Oct 29, 2010
 * Time: 3:59:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class Lifeline {
    private List<LifelineState> states;
    private final String name;
    private final Comparable diagramName;
    private int x;
    private int width;

    public String getName() {
        return name;
    }

    public int getX() {
        return x;
    }

    public int getWidth() {
        return width;
    }

    public void addState(LifelineState state) {
        this.states.add(state);
    }
    
    public Lifeline(Comparable diagramName, String name) {
      this.name = name;
      this.states = new ArrayList<LifelineState>();
      this.diagramName = diagramName;
    }

    public void resize(int x, int lineWidth, long Ybase, double Yscale) {
        for(LifelineState state : states) {
            state.resize(Yscale, Ybase);
        }

        this.x = x;
        width = lineWidth;
    }

    public void paint(Graphics2D g, StateColorMap colorMap) {
        Rectangle boundary = g.getClipBounds();
        if(x > boundary.getMaxX() || x + width < boundary.getMinX()) {
            //no need to paint if this line isn't displayed
            return;
        }
        //TODO - we need to clip these to the visible states
        for(LifelineState state : states) {
            state.paint(g, colorMap);
        }
    }

    public List<LifelineState> getStates() {
        return states;
    }

    public LifelineState getStateAt(int y) {
        for(LifelineState state : states) {
            if(state.getStartY() < y && state.getStartY() + state.getHeight() > y) {
                return state;
            }
        }
        return null;
    }

    public Comparable getDiagramName() {
      return diagramName;
    }
}
