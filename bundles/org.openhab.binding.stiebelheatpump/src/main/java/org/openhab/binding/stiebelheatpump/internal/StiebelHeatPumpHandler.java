/**
 * Copyright (c) 2010-2017 by the respective copyright holders.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */
package org.openhab.binding.stiebelheatpump.internal;

import static org.openhab.binding.stiebelheatpump.internal.StiebelHeatPumpBindingConstants.*;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.measure.quantity.Dimensionless;
import javax.measure.quantity.Power;
import javax.measure.quantity.Temperature;
import javax.xml.bind.DatatypeConverter;

import org.eclipse.smarthome.core.library.types.DecimalType;
import org.eclipse.smarthome.core.library.types.OnOffType;
import org.eclipse.smarthome.core.library.types.QuantityType;
import org.eclipse.smarthome.core.library.types.StringType;
import org.eclipse.smarthome.core.library.unit.SIUnits;
import org.eclipse.smarthome.core.library.unit.SmartHomeUnits;
import org.eclipse.smarthome.core.thing.Channel;
import org.eclipse.smarthome.core.thing.ChannelUID;
import org.eclipse.smarthome.core.thing.Thing;
import org.eclipse.smarthome.core.thing.ThingStatus;
import org.eclipse.smarthome.core.thing.ThingStatusDetail;
import org.eclipse.smarthome.core.thing.binding.BaseThingHandler;
import org.eclipse.smarthome.core.thing.type.ChannelTypeUID;
import org.eclipse.smarthome.core.thing.type.ThingType;
import org.eclipse.smarthome.core.types.Command;
import org.eclipse.smarthome.core.types.RefreshType;
import org.eclipse.smarthome.io.transport.serial.SerialPortIdentifier;
import org.eclipse.smarthome.io.transport.serial.SerialPortManager;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.openhab.binding.stiebelheatpump.protocol.RecordDefinition;
import org.openhab.binding.stiebelheatpump.protocol.RecordDefinition.Type;
import org.openhab.binding.stiebelheatpump.protocol.Request;
import org.openhab.binding.stiebelheatpump.protocol.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link StiebelHeatPumpHandler} is responsible for handling commands, which are
 * sent to one of the channels.
 *
 * @author Peter Kreutzer - Initial contribution
 */
public class StiebelHeatPumpHandler extends BaseThingHandler {

    private Logger logger = LoggerFactory.getLogger(StiebelHeatPumpHandler.class);
    private final SerialPortManager serialPortManager;
    private StiebelHeatPumpConfiguration config;
    CommunicationService communicationService;
    boolean communicationInUse = false;
    public static final String DATE_PATTERN_WITH_TZ = "yyyy-MM-dd HH:mm:ss z";

    /** heat pump request definition */
    private Requests heatPumpConfiguration = new Requests();
    private Requests heatPumpSensorConfiguration = new Requests();
    private Requests heatPumpSettingConfiguration = new Requests();
    private Requests heatPumpStatusConfiguration = new Requests();
    private Requests heatPumpRefresh = new Requests();
    private Request versionRequest;
    private Request timeRequest;

    /** cyclic pooling of data from heat pump */
    ScheduledFuture<?> refreshJob;
    /** cyclic update of time in the heat pump */
    ScheduledFuture<?> timeRefreshJob;

    private ThingType thingType;

    public StiebelHeatPumpHandler(Thing thing, ThingType thingType, final SerialPortManager serialPortManager) {
        super(thing);
        this.thingType = thingType;
        this.serialPortManager = serialPortManager;
    }

    @Override
    public void handleCommand(ChannelUID channelUID, Command command) {
        if (command instanceof RefreshType) {
            // refresh is handled with scheduled polling of data
            return;
        }
        logger.debug("Received command {} for channelUID {}", command, channelUID);

        // extract channelId
        String[] parts = channelUID.getId().split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
        if (parts.length != 2) {
            logger.debug("Channel {} to unlink has invalid structure channelgroup#channel", channelUID);
        }
        String channelId = parts[parts.length - 1];

        int retry = 0;
        while (communicationInUse & (retry < MAXRETRY)) {
            try {
                Thread.sleep(config.waitingTime);
            } catch (InterruptedException e) {
                logger.debug("Could not get access to heatpump, communication is in use {} !", retry);
                e.printStackTrace();
            }
            retry++;
        }
        if (communicationInUse) {
            logger.debug("Could not get access to heatpump, communication is in use ! Final");
            return;
        }
        communicationInUse = true;
        communicationService.connect();
        try {
            Map<String, Object> data = new HashMap<>();
            switch (channelUID.getId()) {
                case CHANNEL_SETTIME:
                    data = communicationService.setTime(timeRequest);
                    updateState(channelUID, OnOffType.OFF);
                    break;
                case CHANNEL_DUMPRESPONSE:
                    for (byte requestByte : DEBUGBYTES) {
                        communicationService.dumpResponse(requestByte);
                        Thread.sleep(config.waitingTime);
                    }
                    updateState(channelUID, OnOffType.OFF);
                    break;
                default:
                    // do checks if valid definition is available
                    RecordDefinition updateRecord = heatPumpConfiguration.getRecordDefinitionByChannelId(channelId);
                    if (updateRecord == null) {
                        return;
                    }
                    if (updateRecord.getDataType() != Type.Settings) {
                        logger.warn("The record {} can not be set as it is not a setable value!",
                                updateRecord.getChannelid());
                        return;
                    }
                    Object value = null;
                    if (command instanceof OnOffType) {
                        // the command come from a switch type , we need to map ON and OFF to 0 and 1 values
                        value = true;
                        if (command.equals(OnOffType.OFF)) {
                            value = false;
                        }
                    }
                    if (command instanceof QuantityType) {
                        QuantityType<?> newQtty = ((QuantityType<?>) command);
                        value = newQtty.doubleValue();
                    }
                    if (command instanceof DecimalType) {
                        value = ((DecimalType) command).doubleValue();
                    }
                    if (command instanceof StringType) {
                        DateTimeFormatter strictTimeFormatter = DateTimeFormatter.ofPattern("HH:mm")
                                .withResolverStyle(ResolverStyle.STRICT);
                        try {
                            LocalTime time = LocalTime.parse(command.toString(), strictTimeFormatter);
                            value = (short) (time.getHour() * 100 + time.getMinute());
                        } catch (DateTimeParseException e) {
                            logger.info("Time string is not valid ! : {}", e.getMessage());
                        }
                    }
                    data = communicationService.writeData(value, channelId, updateRecord);
            }
            updateChannels(data);
        } catch (Exception e) {
            logger.debug("Exception occurred during execution: {}", e.getMessage(), e);
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.COMMUNICATION_ERROR, e.getMessage());
        } finally {
            communicationService.disconnect();
            communicationInUse = false;
        }
    }

    @Override
    public void channelLinked(ChannelUID channelUID) {
        String channelId = channelUID.getId();
        String[] parts = channelId.split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
        if (parts.length != 2) {
            logger.debug("Channel {} to unlink has invalid structure channelgroup#channel", channelId);
        }
        channelId = parts[parts.length - 1];

        // verify if channel is already in considered refresh request
        Request request = heatPumpRefresh.getRequestByChannelId(channelId);
        if (request == null) {
            logger.debug("Could not find valid record definitionrequest in channel for: {}", channelId);
            return;
        }
        String requestStr = String.format("%02X", request.getRequestByte());
        logger.debug("Found valid record definition in request {} with ChannelID:{}", requestStr, channelId);
        heatPumpRefresh.getRequests().add(request);
    }

    @Override
    public void channelUnlinked(ChannelUID channelUID) {
        String channelId = channelUID.getId();
        String[] parts = channelId.split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
        if (parts.length != 2) {
            logger.debug("Channel {} to unlink has invalid structure channelgroup#channel", channelId);
        }
        channelId = parts[parts.length - 1];

        Request request = heatPumpRefresh.getRequestByChannelId(channelId);
        if (request == null) {
            logger.debug("No Request found for channelid {} !", channelId);
            return;
        }
        String requestStr = String.format("%02X", request.getRequestByte());

        // verify if there are other channels in the request that are linked
        List<Channel> channels = getThing().getChannels();
        for (Channel channel : channels) {
            if (this.isLinked(channel.getUID())) {
                parts = channel.getUID().getId()
                        .split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
                if (parts[parts.length - 1] == channelId) {
                    continue;
                }

                if (request.getRecordDefinitionByChannelId(channelId) != null) {
                    // no channel found which belongs to same request, remove request
                    heatPumpRefresh.getRequests().remove(request);
                    logger.debug("Request {} removed in refresh list no additionl channel from request linked",
                            requestStr);
                }
            }
        }
    }

    @Override
    public void initialize() {
        if (heatPumpConfiguration.getRequests().isEmpty()) {
            // get the records from the thing-type configuration file
            String configFile = thingType.getUID().getId();
            ConfigLocator configLocator = new ConfigLocator(configFile + ".xml");
            heatPumpConfiguration.setRequests(configLocator.getRequests());
        }
        categorizeHeatPumpConfiguration();
        updateRefreshRequests();

        this.config = getConfigAs(StiebelHeatPumpConfiguration.class);
        if (!validateConfiguration(config)) {
            return;
        }

        String availablePorts = serialPortManager.getIdentifiers().map(id -> id.getName())
                .collect(Collectors.joining(", "));

        logger.debug(
                "Initializing stiebel heat pump handler '{}' with configuration: port '{}', baudRate {}, refresh {}. Available ports are : {}",
                getThing().getUID(), config.port, config.baudRate, config.refresh, availablePorts);

        SerialPortIdentifier portId = serialPortManager.getIdentifier(config.port);
        if (portId == null) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR, "Port is not known!");
            logger.debug("Serial port {} was not found, available ports are : {} ", config.port, availablePorts);
            return;
        }

        communicationService = new CommunicationService(serialPortManager, config.port, config.baudRate,
                config.waitingTime);

        scheduler.schedule(this::getInitialHeatPumpSettings, 1, TimeUnit.SECONDS);
        updateStatus(ThingStatus.UNKNOWN, ThingStatusDetail.HANDLER_CONFIGURATION_PENDING,
                "Waiting for messages from device");

    }

    @Override
    public void dispose() {
        if (refreshJob != null && !refreshJob.isCancelled()) {
            refreshJob.cancel(true);
        }
        refreshJob = null;

        if (timeRefreshJob != null && !timeRefreshJob.isCancelled()) {
            timeRefreshJob.cancel(true);
        }
        timeRefreshJob = null;

        if (communicationService != null) {
            communicationService.finalizer();
            communicationInUse = false;
        }
    }

    private boolean validateConfiguration(StiebelHeatPumpConfiguration config) {
        if (config.port == null || config.port.isEmpty()) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR, "Port must be set!");
            return false;
        }

        if (config.baudRate < 9600 || config.baudRate > 115200) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR,
                    "BaudRate must be between 9600 and 115200");
            return false;
        }

        if (config.refresh < 10) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR,
                    "Refresh rate must be larger than 10");
            return false;
        }

        if (config.waitingTime <= 0) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR,
                    "Waiting time between requests must be larger than 0");
            return false;
        }

        return true;
    }

    /**
     * This method pools the meter data and updates the channels on a scheduler
     * once per refresh time defined in the thing properties
     */
    private void startAutomaticRefresh() {
        refreshJob = scheduler.scheduleWithFixedDelay(() -> {

            if (heatPumpRefresh.getRequests().isEmpty()) {
                logger.debug("nothing to update, refresh list is empty");
                return;
            }

            if (communicationInUse) {
                logger.debug("Communication service is in use , skip refresh data task this time.");
                return;
            }
            communicationInUse = true;
            try {
                communicationService.connect();
                Map<String, Object> data = communicationService.getRequestData(heatPumpRefresh.getRequests());
                updateChannels(data);
                LocalDateTime dt = DateTime.now().toLocalDateTime();
                updateState(CHANNEL_LASTUPDATE, new StringType(dt.toString(DATE_PATTERN_WITH_TZ)));
            } catch (Exception e) {
                logger.debug("Exception occurred during execution: {}", e.getMessage(), e);
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.COMMUNICATION_ERROR, e.getMessage());
            } finally {
                communicationService.disconnect();
                communicationInUse = false;
            }
        }, 2, config.refresh, TimeUnit.SECONDS);
    }

    /**
     * This method set the time in the heat pump to system time on a scheduler
     * once a week
     */
    private void startTimeRefresh() {
        timeRefreshJob = scheduler.scheduleWithFixedDelay(() -> {
            if (communicationInUse) {
                return;
            }
            communicationInUse = true;
            try {
                communicationService.connect();
                Map<String, Object> time = communicationService.setTime(timeRequest);
                updateChannels(time);
            } catch (StiebelHeatPumpException e) {
                logger.debug(e.getMessage());
            } finally {
                communicationService.disconnect();
                communicationInUse = false;
            }
        }, 1, 7, TimeUnit.DAYS);
    }

    /**
     * This method reads initial information from the heat pump. It reads
     * the configuration file and loads all defined record definitions of sensor
     * data, status information , actual time settings and setting parameter
     * values for the thing type definition.
     *
     * @return true if heat pump information could be successfully connected and read
     */
    private void getInitialHeatPumpSettings() {
        String thingFirmwareVersion = thingType.getProperties().get(Thing.PROPERTY_FIRMWARE_VERSION);

        // get version information from the heat pump
        communicationService.connect();
        try {
            String version = communicationService.getVersion(versionRequest);
            logger.info("Heat pump has version {}", version);
            if (!thingFirmwareVersion.equals(version)) {
                logger.error("Thingtype version of heatpump {} is not the same as the heatpump version {}",
                        thingFirmwareVersion, version);
                return;
            }
        } catch (StiebelHeatPumpException e) {
            logger.debug(e.getMessage());
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_ERROR,
                    "Communication problem with heatpump");
            communicationService.finalizer();
            return;
        } finally {
            communicationService.disconnect();
        }

        updateStatus(ThingStatus.ONLINE);
        startTimeRefresh();
        startAutomaticRefresh();
    }

    /**
     * This method updates the query data to the channels
     *
     * @param data
     *            Map<String, String> of data coming from heat pump
     */
    private void updateChannels(Map<String, Object> data) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            logger.debug("Data {} has value {}", entry.getKey(), entry.getValue());
            String channelId = entry.getKey();
            for (Channel ch : getThing().getChannels()) {
                ChannelUID channelUID = ch.getUID();
                String[] parts = channelUID.getId()
                        .split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
                if (parts[parts.length - 1].equals(channelId)) {
                    ChannelTypeUID channelTypeUID = ch.getChannelTypeUID();
                    String channelType = channelTypeUID.toString();

                    if (channelType.equalsIgnoreCase(CHANNELTYPE_TIMESETTING)
                            | channelType.equalsIgnoreCase(CHANNEL_CURRENTTIME)) {
                        updateTimeChannel(entry.getValue().toString(), channelUID);
                        continue;
                    }
                    if (channelType.equalsIgnoreCase(CHANNELTYPE_SWITCHSETTING)) {
                        updateSwitchSettingChannel((boolean) entry.getValue(), channelUID);
                        continue;
                    }
                    if (entry.getValue() instanceof Number) {
                        updateStatus((Number) entry.getValue(), ch, channelUID);
                        continue;
                    }
                    if (entry.getValue() instanceof Boolean) {
                        updateSwitchSettingChannel((boolean) entry.getValue(), channelUID);
                    }
                }
            }
        }

        updateStatus(ThingStatus.ONLINE);
    }

    private void updateStatus(Number value, Channel ch, ChannelUID channelUID) {
        String itemType = ch.getAcceptedItemType();

        if (value instanceof Double) {
            switch (itemType) {
                case "Number:Temperature":
                    QuantityType<Temperature> temperature = new QuantityType<>(value, SIUnits.CELSIUS);
                    updateState(channelUID, temperature);
                    break;
                case "Number:Energy":
                    // TODO: how to make this kW as these are coming from heatpump
                    QuantityType<Power> energy = new QuantityType<>(value, SmartHomeUnits.WATT);
                    updateState(channelUID, energy);
                    break;
                case "Number:Dimensionless:Percent":
                    QuantityType<Dimensionless> percent = new QuantityType<>(value, SmartHomeUnits.PERCENT);
                    updateState(channelUID, percent);
                    break;

                default:
                    updateState(channelUID, new DecimalType((Double) value));
            }
            return;
        }
        updateState(channelUID, new DecimalType((short) value));
    }

    private void updateSwitchSettingChannel(Boolean setting, ChannelUID channelUID) {
        if (setting) {
            updateState(channelUID, OnOffType.ON);
        } else {
            updateState(channelUID, OnOffType.OFF);
        }
    }

    private void updateTimeChannel(String timeString, ChannelUID channelUID) {
        String newTime = String.format("%04d", Integer.parseInt(timeString));
        newTime = new StringBuilder(newTime).insert(newTime.length() - 2, ":").toString();
        updateState(channelUID, new StringType(newTime));
    }

    /**
     * This method categorize the heat pump configuration into setting, sensor
     * and status
     *
     * @return true if heat pump configuration for version could be found and
     *         loaded
     */
    private boolean categorizeHeatPumpConfiguration() {
        for (Request request : heatPumpConfiguration.getRequests()) {
            String requestByte = DatatypeConverter.printHexBinary(new byte[] { request.getRequestByte() });
            logger.debug("Request : RequestByte -> {}", requestByte);

            // group requests in different categories
            for (RecordDefinition record : request.getRecordDefinitions()) {

                if (record.getRequestByte() == REQUEST_VERSION) {
                    versionRequest = request;
                    logger.debug("set version request : {}", requestByte);
                    continue;
                }

                if (timeRequest == null && record.getRequestByte() == REQUEST_TIME) {
                    timeRequest = request;
                    logger.debug("set time request : {}", requestByte);
                    continue;
                }

                if (record.getDataType() == Type.Settings
                        && !heatPumpSettingConfiguration.getRequests().contains(request)) {
                    heatPumpSettingConfiguration.getRequests().add(request);
                }
                if (record.getDataType() == Type.Status
                        && !heatPumpStatusConfiguration.getRequests().contains(request)) {
                    heatPumpStatusConfiguration.getRequests().add(request);
                }
                if (record.getDataType() == Type.Sensor
                        && !heatPumpSensorConfiguration.getRequests().contains(request)) {
                    heatPumpSensorConfiguration.getRequests().add(request);
                }
            }
        }

        if (versionRequest == null || timeRequest == null) {
            logger.debug("version or time request could not be found in configuration");
            return false;
        }
        return true;
    }

    private void updateRefreshRequests() {
        for (Channel channel : getThing().getChannels()) {
            // TODO: i am not too sure how expensive such a call is and I think this might not be necessary to check
            // if (isLinked(channel.getUID())) {
            ChannelUID channelUID = channel.getUID();
            String[] parts = channelUID.getId()
                    .split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
            String channelId = parts[parts.length - 1];
            Request request = heatPumpConfiguration.getRequestByChannelId(channelId);
            if (request != null && !heatPumpRefresh.getRequests().contains(request)) {
                heatPumpRefresh.getRequests().add(request);
                // there is still a channel link in the thing which will require updates
                String requestbyte = String.format("%02X", request.getRequestByte());
                logger.info("Request {} added to refresh scheduler.", requestbyte);
            }
            // }
        }
    }
}
