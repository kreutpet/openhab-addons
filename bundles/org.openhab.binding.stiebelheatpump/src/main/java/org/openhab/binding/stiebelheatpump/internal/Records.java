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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.openhab.binding.stiebelheatpump.protocol.RecordDefinition;
import org.openhab.binding.stiebelheatpump.protocol.Request;
import org.openhab.binding.stiebelheatpump.protocol.Requests;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * Records class for Stiebel heat pump.
 *
 * @author Peter Kreutzer
 */

@XStreamAlias("records")
public class Records {

    // @XStreamImplicit(itemFieldName = "record")
    @XStreamImplicit
    // @XStreamAlias("records")
    private List<Record> records = new ArrayList<Record>();

    public Records() {
    }

    public Records(List<Record> records) {
        this.records = records;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
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

    public Requests getRequests() {

        Requests requests = new Requests();

        for (Record record : records) {
            byte[] requestByte = DatatypeConverter.parseHexBinary(record.getRequestByte());
            byte[] requestByte1000 = null;
            if (record.getRequestByte1000() != null) {
                requestByte1000 = DatatypeConverter.parseHexBinary(record.getRequestByte1000());
            }

            RecordDefinition recordDefinition = new RecordDefinition();
            recordDefinition.setChannelid(record.getChannelid());
            recordDefinition.setRequestByte(requestByte);
            recordDefinition.setRequestByte1000(requestByte1000);

            switch (record.getDataType()) {
                case Settings:
                    recordDefinition.setDataType(RecordDefinition.Type.Settings);
                    break;
                case Sensor:
                    recordDefinition.setDataType(RecordDefinition.Type.Sensor);
                    break;
                case Status:
                    recordDefinition.setDataType(RecordDefinition.Type.Status);
                    break;
            }

            recordDefinition.setLength(record.getLength());
            recordDefinition.setPosition(record.getPosition());
            recordDefinition.setBitPosition(record.getBitPosition());
            recordDefinition.setMax(record.getMax());
            recordDefinition.setMin(record.getMin());
            recordDefinition.setScale(record.getScale());
            recordDefinition.setStep(record.getStep());
            recordDefinition.setUnit(record.getUnit());

            boolean found = false;
            for (Request request : requests.getRequests()) {
                if (Arrays.equals(request.getRequestByte(), requestByte)) {
                    request.getRecordDefinitions().add(recordDefinition);
                    found = true;
                }
            }
            if (!found) {
                Request newRequest = new Request("", "", requestByte, requestByte1000);
                newRequest.getRecordDefinitions().add(recordDefinition);
                requests.getRequests().add(newRequest);
            }
        }

        return requests;
    }
}
