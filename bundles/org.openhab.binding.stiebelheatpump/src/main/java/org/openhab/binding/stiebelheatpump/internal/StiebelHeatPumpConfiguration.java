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

/**
 * The {@link StiebelHeatPumpConfiguration} class contains fields mapping thing configuration parameters.
 *
 * @author Peter Kreutzer - Initial contribution
 */
public class StiebelHeatPumpConfiguration {

    public String port;
    public int refresh;
    public int waitingTime;
    public int baudRate;
}
