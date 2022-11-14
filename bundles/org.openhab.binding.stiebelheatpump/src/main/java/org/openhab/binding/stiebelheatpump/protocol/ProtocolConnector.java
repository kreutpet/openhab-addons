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

import org.openhab.binding.stiebelheatpump.internal.StiebelHeatPumpException;
import org.openhab.core.io.transport.serial.SerialPortManager;

/**
 * @author Stefan Triller
 */
public interface ProtocolConnector {

    public abstract void connect(SerialPortManager portManager, String s, int i) throws StiebelHeatPumpException;

    public abstract void disconnect();

    public abstract byte get() throws StiebelHeatPumpException;

    public abstract short getShort() throws StiebelHeatPumpException;

    public abstract void get(byte abyte0[]) throws StiebelHeatPumpException;

    public abstract void mark();

    public abstract void reset();

    public abstract void write(byte abyte0[]) throws StiebelHeatPumpException;

    public abstract void write(byte byte0) throws StiebelHeatPumpException;
}
