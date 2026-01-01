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
package org.openhab.binding.stiebelheatpump.protocol;

/**
 * Record definition class for Stiebel heat pump requests.
 *
 * @author Peter Kreutzer
 */

public class RecordDefinition {

    public static enum Type {
        Sensor,
        Status,
        Settings;
    }

    private String channelid;

    private byte[] requestByte;

    private byte[] requestByte1000;

    private Type dataType;

    private int position;

    private int length;

    private double scale;

    private int bitPosition;

    private double min;

    private double max;

    private double step;

    private String unit;

    public RecordDefinition() {
    }

    /**
     * Constructor of record definition used for status and sensor values
     *
     * @param channelid
     *            of record
     * @param requestByte
     *            bytes to request the value
     * @param requestByte1000
     *            bytes to request the thousands of the value (usually null, i.e. unused)
     * @param position
     *            of the value in the byte array
     * @param length
     *            of byte representing the value
     * @param scale
     *            to apply to the byte value
     * @param dataType
     *            of the record, see enums
     * @param unit
     *            of the value
     */
    public RecordDefinition(String channelid, byte[] requestByte, byte[] requestByte1000, int position, int length,
            double scale, Type dataType, String unit) {
        this.channelid = channelid;
        this.requestByte = requestByte;
        this.requestByte1000 = requestByte1000;
        this.position = position;
        this.length = length;
        this.scale = scale;
        this.dataType = dataType;
        this.unit = unit;
    }

    /**
     * Constructor of record definition used for setting programs with week days
     * encoding
     *
     * @param channelid
     *            of record
     * @param requestByte
     *            bytes to request the value
     * @param requestByte1000
     *            bytes to request the thousands of the value (usually null, i.e. unused)
     * @param position
     *            of the value in the byte array
     * @param length
     *            of byte representing the value
     * @param scale
     *            to apply to the byte value
     * @param dataType
     *            of the record, see enums
     * @param min
     *            values for a setting
     * @param max
     *            values for a setting
     * @param step
     *            in which setting can be changed
     * @param bitPosition
     *            of the bit in the byte representing the value
     * @param unit
     *            of the value
     */
    public RecordDefinition(String channelid, byte[] requestByte, byte[] requestByte1000, int position, int length,
            double scale, Type dataType, int min, int max, double step, int bitPosition, String unit) {
        this.channelid = channelid;
        this.requestByte = requestByte;
        this.requestByte1000 = requestByte1000;
        this.position = position;
        this.length = length;
        this.scale = scale;
        this.dataType = dataType;
        this.min = min;
        this.max = max;
        this.step = step;
        this.bitPosition = bitPosition;
        this.unit = unit;
    }

    /**
     * Constructor of record definition used for settings that can be changed
     *
     * @param channelid
     *            of record
     * @param requestByte
     *            bytes to request the value
     * @param requestByte1000
     *            bytes to request the thousands of the value (usually null, i.e. unused)
     * @param position
     *            of the value in the byte array
     * @param length
     *            of byte representing the value
     * @param scale
     *            to apply to the byte value
     * @param dataType
     *            of the record, see enums
     * @param min
     *            values for a setting
     * @param max
     *            values for a setting
     * @param step
     *            in which setting can be changed
     * @param unit
     *            of the value
     */
    public RecordDefinition(String channelid, byte[] requestByte, byte[] requestByte1000, int position, int length,
            double scale, Type dataType, int min, int max, double step, String unit) {
        this.channelid = channelid;
        this.requestByte = requestByte;
        this.requestByte1000 = requestByte1000;
        this.position = position;
        this.length = length;
        this.scale = scale;
        this.dataType = dataType;
        this.min = min;
        this.max = max;
        this.step = step;
        this.unit = unit;
    }

    public String getChannelid() {
        return channelid;
    }

    public void setChannelid(String channelid) {
        this.channelid = channelid;
    }

    public byte[] getRequestByte() {
        return requestByte;
    }

    public void setRequestByte(byte[] requestByte) {
        this.requestByte = requestByte;
    }

    public byte[] getRequestByte1000() {
        return requestByte1000;
    }

    public void setRequestByte1000(byte[] requestByte1000) {
        this.requestByte1000 = requestByte1000;
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

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
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
