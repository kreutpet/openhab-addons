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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import com.thoughtworks.xstream.security.AnyTypePermission;

/**
 * Config parser class. This class parses the xml configuration file converts it
 * into a list of requests
 *
 * @author Peter Kreutzer
 */
public class ConfigParser {

    private static final Logger logger = LoggerFactory.getLogger(ConfigParser.class);

    public ConfigParser() {
    }

    /**
     * This method loads a List of Request objects from xml file
     *
     * @param fileName
     *            file object to load the object from
     * @return List of Requests
     */
    public Records parseConfig(URL configFile) {

        ClassLoader classloader = this.getClass().getClassLoader();

        XStream xstream = new XStream(new StaxDriver());
        xstream.addPermission(AnyTypePermission.ANY);
        xstream.setClassLoader(classloader);
        xstream.ignoreUnknownElements();
        xstream.processAnnotations(Record.class);
        xstream.processAnnotations(Records.class);

        Records records = null;
        try {
            InputStream x = configFile.openStream();
            // creating an InputStreamReader object
            // InputStreamReader isReader = new InputStreamReader(x);
            records = (Records) xstream.fromXML(x);

            if (records == null) {
                logger.debug("Records could not be deserialized from: {}", configFile.toString());
                return null;
            }
        } catch (IOException e) {
            logger.error("IO error while parsing configuration", e);
        }

        return records;
    }
}
