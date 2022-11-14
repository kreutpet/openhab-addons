/**
 * Copyright (c) 2010-2022 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.stiebelheatpump.internal;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * Record definition class for Stiebel heat pump requests.
 *
 * @author Peter Kreutzer
 */

@XStreamAlias("record")
public class Record {

    public static enum Type {
        Sensor,
        Status,
        Settings;
    }

    @XStreamAsAttribute
    private String channelid;

    @XStreamAsAttribute
    private String requestByte;

    @XStreamAsAttribute
    private Type dataType;

    @XStreamAsAttribute
    private int position;

    @XStreamAsAttribute
    private int length;

    @XStreamAsAttribute
    private double scale;

    @XStreamAsAttribute
    private int bitPosition;

    @XStreamAsAttribute
    private double min;

    @XStreamAsAttribute
    private double max;

    @XStreamAsAttribute
    private double step;

    @XStreamAsAttribute
    private String unit;

    public Record() {
    }

    public String getChannelid() {
        return channelid;
    }

    public void setChannelid(String channelid) {
        this.channelid = channelid;
    }

    public String getRequestByte() {
        return requestByte;
    }

    public void setRequestByte(String b) {
        this.requestByte = b;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public double getScale() {
        return scale;
    }

    public void setScale(double scale) {
        this.scale = scale;
    }

    public Type getDataType() {
        return dataType;
    }

    public void setDataType(Type dataType) {
        this.dataType = dataType;
    }

    public double getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public double getStep() {
        return step;
    }

    public void setStep(double step) {
        this.step = step;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public int getBitPosition() {
        return bitPosition;
    }

    public void setBitPosition(int bitPosition) {
        this.bitPosition = bitPosition;
    }
}
