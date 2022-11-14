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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import org.eclipse.jdt.annotation.NonNull;
import org.openhab.binding.stiebelheatpump.internal.StiebelHeatPumpException;
import org.openhab.core.io.transport.serial.SerialPort;
import org.openhab.core.io.transport.serial.SerialPortIdentifier;
import org.openhab.core.io.transport.serial.SerialPortManager;
import org.openhab.core.io.transport.serial.UnsupportedCommOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * connector for serial port communication.
 *
 * @author Evert van Es (originaly copied from)
 * @author Peter Kreutzer
 */

public class SerialConnector implements ProtocolConnector {

    private static final Logger logger = LoggerFactory.getLogger(SerialConnector.class);

    InputStream in = null;
    DataOutputStream out = null;
    SerialPort serialPort = null;
    ByteStreamPipe byteStreamPipe = null;

    private CircularByteBuffer buffer;
    private ScheduledExecutorService scheduler;

    public SerialConnector(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void connect(SerialPortManager portManager, String device, int baudrate) throws StiebelHeatPumpException {
        try {
            SerialPortIdentifier portIdentifier = portManager.getIdentifier(device);
            SerialPort commPort = portIdentifier.open(this.getClass().getName(), 2000);
            serialPort = commPort;
            setSerialPortParameters(baudrate);

            in = serialPort.getInputStream();
            out = new DataOutputStream(serialPort.getOutputStream());

            out.flush();

            buffer = new CircularByteBuffer(Byte.MAX_VALUE * Byte.MAX_VALUE + 2 * Byte.MAX_VALUE);
            byteStreamPipe = new ByteStreamPipe(in, buffer, scheduler);
            byteStreamPipe.startTask();

        } catch (IOException e) {
            @NonNull
            Stream<@NonNull SerialPortIdentifier> ports = portManager.getIdentifiers();
            throw new StiebelHeatPumpException(
                    "Serial port " + device + "with given name does not exist. Available ports: " + ports.toString(),
                    e);
        } catch (Exception e) {
            throw new StiebelHeatPumpException("Could not init port : " + e.getMessage());
        }
    }

    @Override
    public void disconnect() {
        logger.debug("Interrupt serial connection");
        if (byteStreamPipe != null) {
            byteStreamPipe.stopTask();
        }

        logger.debug("Close serial stream");
        // try {
        // out.close();
        if (buffer != null) {
            buffer.stop();
        }

        if (serialPort != null) {
            serialPort.close();
        }
        // try {
        // Thread.sleep(1000);
        // } catch (InterruptedException e) {
        // }
        // } catch (IOException e) {
        // logger.warn("Could not fully shut down heat pump driver", e);
        // }
        this.scheduler = null;

        logger.debug("Disconnected");
    }

    @Override
    public byte get() throws StiebelHeatPumpException {
        return buffer.get();
    }

    @Override
    public short getShort() throws StiebelHeatPumpException {
        return buffer.getShort();
    }

    @Override
    public void get(byte[] data) throws StiebelHeatPumpException {
        buffer.get(data);
    }

    @Override
    public void mark() {
        buffer.mark();
    }

    @Override
    public void reset() {
        buffer.reset();
    }

    @Override
    public void write(byte[] data) throws StiebelHeatPumpException {
        try {
            String dataStr = DataParser.bytesToHex(data, true);
            logger.debug("Send request message : {}", dataStr);
            out.write(data);
            out.flush();
        } catch (IOException e) {
            throw new StiebelHeatPumpException("Could not write " + e.getMessage());
        }
    }

    @Override
    public void write(byte data) throws StiebelHeatPumpException {
        try {
            String byteStr = String.format("Send %02X", data);
            logger.trace("{}", byteStr);
            out.write(data);
            out.flush();
        } catch (IOException e) {
            throw new StiebelHeatPumpException("Could not write " + e.getMessage());
        }
    }

    /**
     * Sets the serial port parameters to xxxxbps-8N1
     *
     * @param baudrate
     *            used to initialize the serial connection
     */
    protected void setSerialPortParameters(int baudrate) throws IOException {

        try {
            // Set serial port to xxxbps-8N1
            serialPort.setSerialPortParams(baudrate, SerialPort.DATABITS_8, SerialPort.STOPBITS_1,
                    SerialPort.PARITY_NONE);
        } catch (UnsupportedCommOperationException ex) {
            throw new IOException("Unsupported serial port parameter for serial port");
        }
    }
}
