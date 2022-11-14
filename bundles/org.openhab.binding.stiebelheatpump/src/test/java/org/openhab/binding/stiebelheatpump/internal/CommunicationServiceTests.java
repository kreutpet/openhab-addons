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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.io.OutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.openhab.binding.stiebelheatpump.protocol.DataParser;
import org.openhab.binding.stiebelheatpump.protocol.ProtocolConnector;
import org.openhab.binding.stiebelheatpump.protocol.RecordDefinition;
import org.openhab.binding.stiebelheatpump.protocol.RecordDefinition.Type;
import org.openhab.core.io.transport.serial.SerialPort;
import org.openhab.core.io.transport.serial.SerialPortIdentifier;
import org.openhab.core.io.transport.serial.SerialPortManager;
import org.openhab.core.util.HexUtils;

/**
 * @author Stefan Triller
 */
public class CommunicationServiceTests {

    SerialPortManager serialPortManager = mock(SerialPortManager.class);
    SerialPortIdentifier serialPortIdentifier = mock(SerialPortIdentifier.class);
    SerialPort serialPort = mock(SerialPort.class);

    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);

    @BeforeEach
    public void setup() throws Exception {
        SerialPort serialPort = mockSerialPort();
        serialPortManager = mockSerialPortManager(serialPort);
    }

    @Test
    public void test() throws Exception {
        mockSerialPort();

        CommunicationService cs = new CommunicationService(serialPortManager, "", 9600, 1000, null);
        // cs.setConnector(mockSerialConnector());
        // cs.connect();

        RecordDefinition updateRecord = new RecordDefinition("myChannel",
                new byte[] { (byte) 0x0a, (byte) 0x05, (byte) 0x6c }, 1, 1, 1, Type.Settings, 0, 3, 1, "unitTest");
        // cs.writeData(1, "myChannel", updateRecord);

        byte[] request = cs.createRequestMessage(updateRecord.getRequestByte());
        String req = HexUtils.bytesToHex(request);

        String correctAnswer = "01007C0A056C1003";

        assertEquals(req, correctAnswer);
    }

    private SerialPort mockSerialPort() throws Exception {
        SerialPort serialPort = mock(SerialPort.class);
        when(serialPort.getOutputStream()).thenReturn(outputStream);
        when(serialPort.getInputStream()).thenReturn(inputStream);
        return serialPort;
    }

    private SerialPortManager mockSerialPortManager(SerialPort serialPort) throws Exception {
        SerialPortManager serialPortManager = mock(SerialPortManager.class);
        SerialPortIdentifier serialPortIdentifier = mock(SerialPortIdentifier.class);
        when(serialPortManager.getIdentifier(anyString())).thenReturn(serialPortIdentifier);
        when(serialPortIdentifier.open(anyString(), anyInt())).thenReturn(serialPort);
        return serialPortManager;
    }

    private ProtocolConnector mockSerialConnector() throws StiebelHeatPumpException {
        ProtocolConnector connector = mock(ProtocolConnector.class);
        when(connector.get()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                // System.out.println("count = " + count);
                count++;
                if (count == 1) {
                    return DataParser.ESCAPE;
                } else if (count == 2) {
                    return DataParser.STARTCOMMUNICATION;
                } else {
                    return null;
                }
            }
        });

        return connector;
    }
}
