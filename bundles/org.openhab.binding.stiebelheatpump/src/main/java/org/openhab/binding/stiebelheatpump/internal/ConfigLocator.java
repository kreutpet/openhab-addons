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

import java.net.URL;
import java.util.List;

import org.openhab.binding.stiebelheatpump.protocol.Request;
import org.openhab.binding.stiebelheatpump.protocol.Requests;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config locator class. This class located the configuration file in the
 * resources and converts it into a list of requests
 *
 * @author Peter Kreutzer
 */
public class ConfigLocator {

    private static final Logger logger = LoggerFactory.getLogger(ConfigLocator.class);

    private String file;
    private ConfigParser configParser = new ConfigParser();
    private Records records = new Records();
    private Requests requests = new Requests();

    public ConfigLocator() {
    }

    /**
     * @param file
     *            that shall be located in the resources\thing the file shall contain
     *            the configuration of the specific request to the firmware
     *            version naming convention shall be "thingtypenid.xml" , e.g.
     *            LWZ_THZ303_2_06.xml
     */
    public ConfigLocator(String file) {
        this.file = file;
        getconfig();
    }

    /**
     * Searches for the given files in the class path.
     *
     * @return All found Configurations
     */
    public void getconfig() {
        logger.debug("Parsing  heat pump configuration file {}.", file);

        URL entry = FrameworkUtil.getBundle(ConfigParser.class).getEntry("HeatpumpConfig/" + file);
        if (entry == null) {
            logger.error("Unable to load  {} config file of heatpump!", file);
            return;
        }

        records = configParser.parseConfig(entry);
        requests = records.getRequests();
    }

    /**
     * Searches for the given files in the class path.
     *
     * @return All request of the configuration
     */
    public List<Request> getRequests() {
        return requests.getRequests();
    }

    /**
     * Searches for the given files in the class path.
     *
     * @return All records of the configuration
     */
    public List<Record> getRecords() {
        return records.getRecords();
    }
}
