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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.openhab.core.util.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Requests class for Stiebel heat pump.
 *
 * @author Peter Kreutzer
 */
public class Requests {
    private Logger logger = LoggerFactory.getLogger(Requests.class);
    private List<Request> requestList = new CopyOnWriteArrayList<Request>();

    public Requests() {
    }

    public Requests(List<Request> requests) {
        this.requestList.addAll(requests);
    }

    public List<Request> getRequests() {
        return requestList;
    }

    public void setRequests(List<Request> requests) {
        this.requestList.clear();
        this.requestList.addAll(requests);
    }

    public static <T> List<T> searchIn(List<T> list, Matcher<T> m) {
        List<T> r = new ArrayList<T>();
        for (T t : list) {
            if (m.matches(t)) {
                r.add(t);
            }
        }
        return r;
    }

    public interface Matcher<T> {
        public boolean matches(T t);
    }

    public Request getRequestByChannelId(String channelId) {
        for (Request request : requestList) {
            for (RecordDefinition record : request.getRecordDefinitions()) {
                if (record.getChannelid().equalsIgnoreCase(channelId)) {
                    return request;
                }
            }
        }
        logger.warn("Could not find valid request definition for {},  please verify thing definition.", channelId);
        return null;
    }

    public RecordDefinition getRecordDefinitionByChannelId(String channelId) {
        for (Request request : requestList) {
            RecordDefinition record = request.getRecordDefinitionByChannelId(channelId);
            if (record != null) {
                return request.getRecordDefinitionByChannelId(channelId);
            }
        }
        logger.warn("Could not find valid request definition for {},  please verify thing definition.", channelId);
        return null;
    }

    public Request getRequestByByte(byte[] requestByte) {
        for (Request request : requestList) {
            if (Arrays.equals(request.getRequestByte(), requestByte)) {
                return request;
            }
        }
        String requestStr = HexUtils.bytesToHex(requestByte);
        // String requestStr = String.format("%02X", requestByte);
        logger.warn("Could not find valid request definition for {},  please verify thing definition.", requestStr);
        return null;
    }
}
