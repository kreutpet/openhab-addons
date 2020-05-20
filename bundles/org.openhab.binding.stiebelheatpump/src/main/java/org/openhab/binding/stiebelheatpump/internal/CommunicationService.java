/**
 * Copyright (c) 2010-2020 Contributors to the openHAB project
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

import static org.openhab.binding.stiebelheatpump.internal.StiebelHeatPumpBindingConstants.DATE_PATTERN;

import java.time.LocalDateTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.smarthome.io.transport.serial.SerialPortManager;
import org.openhab.binding.stiebelheatpump.protocol.DataParser;
import org.openhab.binding.stiebelheatpump.protocol.ProtocolConnector;
import org.openhab.binding.stiebelheatpump.protocol.RecordDefinition;
import org.openhab.binding.stiebelheatpump.protocol.Request;
import org.openhab.binding.stiebelheatpump.protocol.SerialConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunicationService {

    private Logger logger = LoggerFactory.getLogger(CommunicationService.class);

    private ProtocolConnector connector;
    private int maxRetry = 5;
    private static int inputBufferLength = 1024;
    private byte[] buffer = new byte[inputBufferLength];

    DataParser parser = new DataParser();

    private SerialPortManager serialPortManager;
    private String serialPortName;
    private int baudRate;
    private int waitingTime = 1200;

    public CommunicationService(SerialPortManager serialPortManager, String serialPortName, int baudRate,
            int waitingTime) {
        this.waitingTime = waitingTime;
        this.baudRate = baudRate;
        this.serialPortName = serialPortName;
        this.serialPortManager = serialPortManager;
        this.connector = new SerialConnector();
    }

    public void finalizer() {
        connector.disconnect();
    }

    public void connect() {
        try {
            connector.connect(serialPortManager, serialPortName, baudRate);
        } catch (StiebelHeatPumpException e) {
        }
    }

    public void disconnect() {
        connector.disconnect();
    }

    /**
     * This method parses the version information from the heat pump response
     *
     * @return version string, e.g: 2.06
     */
    public String getVersion(Request versionRequest) throws StiebelHeatPumpException {
        logger.debug("Loading version info ...");
        Map<String, Object> data = readData(versionRequest);
        String versionKey = StiebelHeatPumpBindingConstants.CHANNEL_VERSION;
        return data.get(versionKey).toString();
    }

    /**
     * This method reads all settings defined in the heat pump configuration
     * from the heat pump
     *
     * @return map of heat pump setting values
     */
    public Map<String, Object> getRequestData(List<Request> requests) throws StiebelHeatPumpException {
        Map<String, Object> data = new HashMap<>();
        for (Request request : requests) {
            logger.debug("Loading data for request {} ...", request.getName());
            try {
                data.putAll(readData(request));
                if (requests.size() > 1) {
                    Thread.sleep(waitingTime);
                }
            } catch (InterruptedException e) {
                throw new StiebelHeatPumpException(e.toString());
            }
        }
        return data;
    }

    /**
     * This method set the time of the heat pump to the current time
     *
     * @return true if time has been updated
     */
    public Map<String, Object> setTime(Request timeRequest) throws StiebelHeatPumpException {

        startCommunication();
        Map<String, Object> data = new HashMap<>();

        if (timeRequest == null) {
            logger.warn("Could not find request definition for time settings! Skip setting time.");
            return data;
        }
        try {
            // get time from heat pump
            logger.debug("Loading current time data ...");
            byte[] requestMessage = createRequestMessage(timeRequest.getRequestByte());
            byte[] response = getData(requestMessage);
            data = parser.parseRecords(response, timeRequest);

            // get current time from local machine
            LocalDateTime dt = LocalDateTime.now();
            String formattedString = dt.format(DateTimeFormatter.ofPattern(DATE_PATTERN));
            logger.debug("Current time is : {}", dt);
            short weekday = (short) dt.getDayOfWeek().getValue();
            short day = (short) dt.getDayOfMonth();
            short month = (short) dt.getMonthValue();
            short year = Short.parseShort(Year.now().format(DateTimeFormatter.ofPattern("uu")));
            short seconds = (short) dt.getSecond();
            short hours = (short) dt.getHour();
            short minutes = (short) dt.getMinute();

            for (RecordDefinition record : timeRequest.getRecordDefinitions()) {
                String channelid = record.getChannelid();
                Object value = data.get(channelid);

                switch (channelid) {
                    case "weekday":
                        response = parser.composeRecord(value, weekday, response, record);
                        break;
                    case "hours":
                        response = parser.composeRecord(value, hours, response, record);
                        break;
                    case "minutes":
                        response = parser.composeRecord(value, minutes, response, record);
                        break;
                    case "seconds":
                        response = parser.composeRecord(value, seconds, response, record);
                        break;
                    case "year":
                        response = parser.composeRecord(value, year, response, record);
                        break;
                    case "month":
                        response = parser.composeRecord(value, month, response, record);
                        break;
                    case "day":
                        response = parser.composeRecord(value, day, response, record);
                        break;
                    default:
                        break;
                }
            }

            Thread.sleep(waitingTime);
            logger.info("Time need update. Set time to {}", dt);
            setData(response);
            Thread.sleep(waitingTime);
            response = getData(requestMessage);
            data = parser.parseRecords(response, timeRequest);

            data.put(StiebelHeatPumpBindingConstants.CHANNEL_LASTUPDATE, formattedString);
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                logger.info("Key = {} , Value =  {}", entry.getKey(), entry.getValue());
            }
            return data;

        } catch (InterruptedException e) {
            throw new StiebelHeatPumpException(e.toString());
        }
    }

    /**
     * This method reads all values defined in the request from the heat pump
     *
     * @param request
     *            definition to load the values from
     * @return map of heat pump values according request definition
     */
    public Map<String, Object> readData(Request request) throws StiebelHeatPumpException {
        Map<String, Object> data = new HashMap<>();
        String requestStr = String.format("%02X", request.getRequestByte());
        logger.debug("Request : Name -> {}, Description -> {} , RequestByte -> {}", request.getName(),
                request.getDescription(), requestStr);
        byte responseAvailable[] = new byte[0];
        byte requestMessage[] = createRequestMessage(request.getRequestByte());

        boolean success = false;
        int count = 0, MAX_TRIES = 3;
        while (!success && count++ < MAX_TRIES) {
            try {
                startCommunication();
                responseAvailable = getData(requestMessage);
                responseAvailable = parser.fixDuplicatedBytes(responseAvailable);
                if (parser.headerCheck(responseAvailable)) {
                    return parser.parseRecords(responseAvailable, request);
                }
                success = true;
            } catch (StiebelHeatPumpException e) {
                logger.error("Error reading data : {} -> Retry: {}", e.toString(), count);
                logger.warn("Retry readData {}", count);
            }
        }
        if (!success) {
            new StiebelHeatPumpException("readData failed 3 time!");
        }
        return data;
    }

    /**
     * This method updates the parameter item of a heat pump request
     *
     * @param value
     *            the new value of the item
     * @param parameter
     *            to be update in the heat pump
     */
    public Map<String, Object> writeData(Object newValue, String channelId, RecordDefinition updateRecord)
            throws StiebelHeatPumpException {
        Map<String, Object> data = new HashMap<>();

        try {
            // get actual value for the corresponding request, in case settings have changed locally
            // as we do no have individual requests for each settings we need to
            // decode the new value
            // into a current response , the response is available in the
            // connector object
            byte[] requestMessage = createRequestMessage(updateRecord.getRequestByte());
            byte[] response = getData(requestMessage);
            response = parser.fixDuplicatedBytes(response);
            Object currentValue = parser.parseRecord(response, updateRecord);

            // create new set request created from the existing read response
            byte[] requestUpdateMessage = parser.composeRecord(currentValue, newValue, response, updateRecord);
            if (Arrays.equals(requestMessage, response)) {
                logger.debug("Current value for {} is already {}.", channelId, newValue);
                return data;
            }

            logger.debug("Setting new value [{}] for parameter [{}]", newValue, channelId);

            Thread.sleep(waitingTime);

            response = setData(requestUpdateMessage);
            response = parser.fixDuplicatedBytes(response);
            if (parser.setDataCheck(response)) {
                logger.debug("Updated parameter {} successfully.", channelId);
                data.put(channelId, newValue);
            } else {
                logger.debug("Update for parameter {} failed!", channelId);
                data.put(channelId, currentValue);
            }

        } catch (StiebelHeatPumpException e) {
            logger.error("Stiebel heat pump communication error during update of value !");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * dumps response of connected heat pump by request byte
     *
     * @param requestByte
     *            request byte to send to heat pump
     */
    public void dumpResponse(byte requestByte) {
        int tmp = maxRetry;
        maxRetry = 1;
        String requestStr = String.format("%02X", requestByte);
        logger.info("Prepare response for request byte {}", requestStr);
        Request request = new Request();
        request.setRequestByte(requestByte);

        byte[] requestMessage = createRequestMessage(request.getRequestByte());

        if (!establishRequest(requestMessage)) {
            logger.info("Could not get response for request byte {}", requestStr);
            return;
        }
        maxRetry = tmp;
        try {
            connector.write(DataParser.ESCAPE);
            byte[] response = receiveData();
            response = parser.fixDuplicatedBytes(response);
            String respondStr = DataParser.bytesToHex(response);
            logger.info("Request {} received response : {}", requestStr, respondStr);

            boolean validData = parser.headerCheck(response);
            if (validData) {
                parser.parseRecords(response, request);
            }
        } catch (Exception e) {
            logger.error(String.format("Could not get data from heat pump! for request %02X", requestByte));
        }
    }

    /**
     * Gets data from connected heat pump
     *
     * @param request
     *            request bytes to send to heat pump
     * @return response bytes from heat pump
     *
     *         General overview of handshake between application and serial
     *         interface of heat pump
     *
     *         1. Sending request bytes ,
     *         e.g.: 01 00 FD FC 10 03 for version request
     *         01 -> header start
     *         00 -> get request
     *         FD -> checksum of request
     *         FC -> request byte
     *         10 03 -> Footer ending the communication
     *
     *         2. Receive a data available
     *         10 -> ok
     *         02 -> it does have data,which wants to send now
     *
     *         3. acknowledge sending data
     *         10 -> ok
     *
     *         4. receive data until footer
     *         01 -> header start
     *         00 -> get request
     *         CC -> checksum of send data
     *         FD -> request byte
     *         00 CE -> data, e.g. short value as 2 bytes -> 206 -> 2.06 version
     *         10 03 -> Footer ending the communication
     */
    private byte[] getData(byte[] request) {
        if (!establishRequest(request)) {
            return new byte[0];
        }
        try {
            connector.write(DataParser.ESCAPE);
            return receiveData();
        } catch (Exception e) {
            logger.error("Could not get data from heat pump! {}", e.toString());
            return buffer;
        }
    }

    /**
     * Sets setting value in heat pump
     *
     * @param request
     *            request bytes to send to heat pump
     * @return response bytes from heat pump
     *
     *         General overview of handshake between application and serial
     *         interface of heat pump
     *
     *         1. Sending request bytes, e.g update time in heat pump
     *         01 -> header start
     *         80 -> set request
     *         F1 -> checksum of request
     *         FC -> request byte
     *         00 02 0a 22 1b 0e 00 03 1a -> new values according record definition for time
     *         10 03 -> Footer ending the communication
     *
     *         2. Receive response message the confirmation message is ready for sending
     *         10 -> ok
     *         02 -> it does have data, which wants to send now
     *
     *         3. acknowledge sending data
     *         10 -> ok
     *
     *         4. receive confirmation message until footer
     *         01 -> header start
     *         80 -> set request
     *         7D -> checksum of send data
     *         FC -> request byte
     *         10 03 -> Footer ending the communication
     */
    private byte[] setData(byte[] request) throws StiebelHeatPumpException {
        try {
            startCommunication();
            establishRequest(request);
            // Acknowledge sending data
            connector.write(DataParser.ESCAPE);

        } catch (Exception e) {
            logger.error("Could not set data to heat pump! {}", e.toString());
            return new byte[0];
        }

        // finally receive data
        return receiveData();
    }

    /**
     * This method start the communication for the request It send the initial
     * handshake and expects a response
     */
    private void startCommunication() throws StiebelHeatPumpException {
        logger.debug("Sending start communication");
        byte response;
        try {
            connector.write(DataParser.STARTCOMMUNICATION);
            response = connector.get();
        } catch (Exception e) {
            throw new StiebelHeatPumpException("heat pump communication could not be established !" + e.getMessage());
        }
        if (response != DataParser.ESCAPE) {
            throw new StiebelHeatPumpException(
                    "heat pump is communicating, but did not receive Escape message in initial handshake!");
        }
    }

    /**
     * This method establish the connection for the request It send the request
     * and expects a data available response
     *
     * @param request
     *            to be send to heat pump
     * @return true if data are available from heatpump
     */
    private boolean establishRequest(byte[] request) {
        int numBytesReadTotal = 0;
        boolean dataAvailable = false;
        int requestRetry = 0;
        int retry = 0;
        try {
            while (requestRetry < maxRetry) {
                connector.write(request);
                retry = 0;
                byte singleByte;
                while ((!dataAvailable) && (retry < maxRetry)) {
                    try {
                        singleByte = connector.get();
                    } catch (Exception e) {
                        retry++;
                        continue;
                    }
                    buffer[numBytesReadTotal] = singleByte;
                    numBytesReadTotal++;
                    if (buffer[0] != DataParser.DATAAVAILABLE[0] || buffer[1] != DataParser.DATAAVAILABLE[1]) {
                        continue;
                    }
                    return true;
                }
                logger.debug("retry request!");
                retry++;
                startCommunication();
            }
            if (!dataAvailable) {
                logger.warn("heat pump has no data available for request!");
                return false;
            }
        } catch (Exception e1) {
            logger.error("Could not get data from heat pump! {}", e1.toString());
            return false;
        }
        return true;
    }

    /**
     * This method receive the response from the heat pump It receive single
     * bytes until the end of message s detected
     *
     * @return bytes representing the data send from heat pump
     */
    private byte[] receiveData() {
        byte singleByte;
        int numBytesReadTotal;
        int retry;
        buffer = new byte[inputBufferLength];
        retry = 0;
        numBytesReadTotal = 0;
        boolean endOfMessage = false;

        while (!endOfMessage & retry < maxRetry) {
            try {
                singleByte = connector.get();
            } catch (Exception e) {
                // reconnect and try again to send request
                retry++;
                continue;
            }

            buffer[numBytesReadTotal] = singleByte;
            numBytesReadTotal++;

            if (numBytesReadTotal > 4 && buffer[numBytesReadTotal - 2] == DataParser.ESCAPE
                    && buffer[numBytesReadTotal - 1] == DataParser.END) {
                // we have reached the end of the response
                endOfMessage = true;
                logger.debug("reached end of response message.");
            }
        }

        byte[] responseBuffer = new byte[numBytesReadTotal];
        System.arraycopy(buffer, 0, responseBuffer, 0, numBytesReadTotal);
        return responseBuffer;
    }

    /**
     * This creates the request message ready to be send to heat pump
     *
     * @param request
     *            object containing necessary information to build request
     *            message
     * @return request message byte[]
     */
    private byte[] createRequestMessage(byte requestByte) {
        short checkSum;
        byte[] requestMessage = new byte[] { DataParser.HEADERSTART, DataParser.GET, (byte) 0x00, requestByte,
                DataParser.ESCAPE, DataParser.END };
        try {
            // prepare request message
            checkSum = parser.calculateChecksum(requestMessage);
            requestMessage[2] = parser.shortToByte(checkSum)[0];
            requestMessage = parser.addDuplicatedBytes(requestMessage);
        } catch (StiebelHeatPumpException e) {
        }
        return requestMessage;
    }

}
