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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
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

import org.eclipse.smarthome.core.library.types.DecimalType;
import org.eclipse.smarthome.core.library.types.OnOffType;
import org.eclipse.smarthome.core.library.types.OpenClosedType;
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

    /** heat pump request definition */
    private Requests heatPumpConfiguration = new Requests();
    private Requests heatPumpSensorConfiguration = new Requests();
    private Requests heatPumpSettingConfiguration = new Requests();
    private Requests heatPumpStatusConfiguration = new Requests();
    private Requests heatPumpSensorStatusRefresh = new Requests();
    private Requests heatPumpSettingRefresh = new Requests();
    private Request versionRequest;
    private Request timeRequest;

    /** cyclic pooling of sensor/status data from heat pump */
    ScheduledFuture<?> refreshSensorStatusJob;
    /** cyclic pooling of sensor/status data from heat pump */
    ScheduledFuture<?> refreshSettingJob;
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
        String channelId = channelUID.getId();
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
                        logger.warn("The record {} can not be set as it is not a setable value!", channelId);
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
        Request request = heatPumpConfiguration.getRequestByChannelId(channelId);
        if (request == null) {
            logger.debug("Could not find valid record definitionrequest in channel for: {}", channelId);
            return;
        }
        String requestStr = String.format("%02X", request.getRequestByte());
        logger.debug("Found valid record definition in request {} with ChannelID:{}", requestStr, channelId);
        Type dataType = request.getRecordDefinitionByChannelId(channelId).getDataType();
        if (dataType == RecordDefinition.Type.Settings && !heatPumpSettingRefresh.getRequests().contains(request)) {
            heatPumpSettingRefresh.getRequests().add(request);
            getSettings(request);
        }
        if (dataType != RecordDefinition.Type.Settings
                && !heatPumpSensorStatusRefresh.getRequests().contains(request)) {
            heatPumpSensorStatusRefresh.getRequests().add(request);
        }
    }

    @Override
    public void channelUnlinked(ChannelUID channelUID) {
        String channelId = channelUID.getId();
        Request request = heatPumpConfiguration.getRequestByChannelId(channelId);
        if (request == null) {
            logger.debug("No Request found for channelid {} !", channelId);
            return;
        }
        String requestStr = String.format("%02X", request.getRequestByte());
        List<Channel> channels = getThing().getChannels();
        Boolean toBeRemoved = false;
        for (Channel channel : channels) {
            if (this.isLinked(channel.getUID())) {
                String channelSearch = channelUID.getId();
                if (channelSearch.equals(channelId)) {
                    toBeRemoved = true;
                    continue;
                }
                if (request.getRecordDefinitionByChannelId(channelId) != null) {
                    toBeRemoved = false;
                    break;
                }
            }
        }
        if (toBeRemoved) {
            // no channel found which belongs to same request, remove request
            if (heatPumpSettingRefresh.getRequests().remove(request)) {
                logger.debug(
                        "Request {} removed in setting refresh list because no additional channel from request linked",
                        requestStr);
            } else if (heatPumpSensorStatusRefresh.getRequests().remove(request)) {
                logger.debug(
                        "Request {} removed in sensor/status refresh list because no additional channel from request linked",
                        requestStr);
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

        scheduler.schedule(this::getInitialHeatPumpSettings, 0, TimeUnit.SECONDS);
        updateStatus(ThingStatus.UNKNOWN, ThingStatusDetail.HANDLER_CONFIGURATION_PENDING,
                "Waiting for messages from device");
    }

    @Override
    public void dispose() {
        if (refreshSettingJob != null && !refreshSettingJob.isCancelled()) {
            refreshSettingJob.cancel(true);
        }
        refreshSettingJob = null;

        if (refreshSensorStatusJob != null && !refreshSensorStatusJob.isCancelled()) {
            refreshSensorStatusJob.cancel(true);
        }
        refreshSensorStatusJob = null;

        if (timeRefreshJob != null && !timeRefreshJob.isCancelled()) {
            timeRefreshJob.cancel(true);
        }
        timeRefreshJob = null;

        communicationInUse = false;
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
     * This method pools the heat pump sensor/status data and updates the channels on a scheduler
     * once per refresh time defined in the thing properties
     */
    private void startAutomaticSensorStatusRefresh() {
        refreshSensorStatusJob = scheduler.scheduleWithFixedDelay(() -> {
            Instant start = Instant.now();
            if (heatPumpSensorStatusRefresh.getRequests().isEmpty()) {
                logger.debug("nothing to update, sensor/status refresh list is empty");
                return;
            }

            if (communicationInUse) {
                logger.debug("Communication service is in use , skip refresh data task this time.");
                return;
            }
            communicationInUse = true;
            logger.info("Refresh sensor/status data of heat pump.");
            try {

                communicationService.connect();
                Map<String, Object> data = communicationService
                        .getRequestData(heatPumpSensorStatusRefresh.getRequests());
                updateChannels(data);
            } catch (Exception e) {
                logger.debug("Exception occurred during execution: {}", e.getMessage(), e);
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.COMMUNICATION_ERROR, e.getMessage());
            } finally {
                communicationService.disconnect();
                communicationInUse = false;
            }
            Instant end = Instant.now();
            logger.debug("Sensor/Status refresh took {} seconds.", Duration.between(start, end).getSeconds());
        }, 20, config.refresh, TimeUnit.SECONDS);
    }

    /**
     * This method pools the heat pump setting data and updates the channels on a scheduler
     */
    private void startAutomaticSettingRefresh() {
        refreshSettingJob = scheduler.scheduleWithFixedDelay(() -> {
            Instant start = Instant.now();
            if (heatPumpSettingRefresh.getRequests().isEmpty()) {
                logger.debug("nothing to update, setting refresh list is empty");
                return;
            }

            if (communicationInUse) {
                logger.debug("Communication service is in use , skip refresh data task this time.");
                return;
            }
            communicationInUse = true;
            logger.info("Refresh setting data of heat pump.");
            try {
                communicationService.connect();
                Map<String, Object> data = communicationService.getRequestData(heatPumpSettingRefresh.getRequests());
                updateChannels(data);
            } catch (Exception e) {
                logger.debug("Exception occurred during execution: {}", e.getMessage(), e);
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.COMMUNICATION_ERROR, e.getMessage());
            } finally {
                communicationService.disconnect();
                communicationInUse = false;
            }
            Instant end = Instant.now();
            logger.debug("Setting refresh took {} seconds.", Duration.between(start, end).getSeconds());
        }, 0, 1, TimeUnit.DAYS);
    }

    /**
     * This method pools the heat pump setting data for one request and updates the channels on a scheduler
     */
    private void getSettings(Request request) {
        refreshSettingJob = scheduler.schedule(() -> {
            while (communicationInUse) {

            }
            communicationInUse = true;
            List<Request> requestList = new ArrayList<>();
            requestList.add(request);

            logger.info("Refresh for newly linked setting of heat pump.");
            try {
                communicationService.connect();
                Map<String, Object> data = communicationService.getRequestData(requestList);
                updateChannels(data);
            } catch (

            Exception e) {
                logger.debug("Exception occurred during execution: {}", e.getMessage(), e);
            } finally {
                communicationService.disconnect();
                communicationInUse = false;
            }

        }, 0, TimeUnit.SECONDS);
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
            logger.info("Refresh time of heat pump.");
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
        startAutomaticSettingRefresh();
        startAutomaticSensorStatusRefresh();
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
            Channel ch = getThing().getChannel(channelId);
            if (ch == null) {
                logger.debug("For channelid {} no configuration found. Review channel definitions.", channelId);
            }
            ChannelUID channelUID = ch.getUID();
            ChannelTypeUID channelTypeUID = ch.getChannelTypeUID();
            String channelType = channelTypeUID.toString();
            if (channelType.equalsIgnoreCase(CHANNELTYPE_TIMESETTING)) {
                updateTimeChannel(entry.getValue().toString(), channelUID);
                continue;
            }
            if (channelType.equalsIgnoreCase(CHANNELTYPE_SWITCHSETTING)) {
                updateSwitchSettingChannel((boolean) entry.getValue(), channelUID);
                continue;
            }
            if (channelType.equalsIgnoreCase(CHANNELTYPE_CONTACTSTATUS)) {
                updateContactChannel((boolean) entry.getValue(), channelUID);
                continue;
            }
            if (entry.getValue() instanceof Number) {
                updateStatus((Number) entry.getValue(), channelUID);
            }
            if (entry.getValue() instanceof Boolean) {
                updateSwitchSettingChannel((boolean) entry.getValue(), channelUID);
            }
            LocalDateTime dt = LocalDateTime.now();
            String formattedString = dt.format(DateTimeFormatter.ofPattern(DATE_PATTERN));
            updateState(CHANNEL_LASTUPDATE, new StringType(formattedString));
        }

        updateStatus(ThingStatus.ONLINE);
    }

    private void updateStatus(Number value, ChannelUID channelUID) {
        String itemType = getThing().getChannel(channelUID).getAcceptedItemType();
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
        if (Boolean.TRUE.equals(setting)) {
            updateState(channelUID, OnOffType.ON);
        } else {
            updateState(channelUID, OnOffType.OFF);
        }
    }

    private void updateContactChannel(Boolean setting, ChannelUID channelUID) {
        if (Boolean.TRUE.equals(setting)) {
            updateState(channelUID, OpenClosedType.OPEN);
        } else {
            updateState(channelUID, OpenClosedType.CLOSED);
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
            String requestByte = String.format("%02X", request.getRequestByte());
            logger.debug("Request : RequestByte -> {}", requestByte);

            if (request.getRequestByte() == REQUEST_VERSION) {
                versionRequest = request;
                logger.debug("set version request : {}", requestByte);
            }
            if (timeRequest == null && request.getRequestByte() == REQUEST_TIME) {
                timeRequest = request;
                logger.debug("set time request : {}", requestByte);
            }

            // group requests in different categories by investigating data type in first record
            RecordDefinition record = request.getRecordDefinitions().get(0);
            switch (record.getDataType()) {
                case Settings:
                    if (!heatPumpSettingConfiguration.getRequests().contains(request)) {
                        heatPumpSettingConfiguration.getRequests().add(request);
                    }
                    break;
                case Status:
                    if (!heatPumpStatusConfiguration.getRequests().contains(request)) {
                        heatPumpStatusConfiguration.getRequests().add(request);
                    }
                    break;
                case Sensor:
                    if (!heatPumpSensorConfiguration.getRequests().contains(request)) {
                        heatPumpSensorConfiguration.getRequests().add(request);
                    }
                    break;
                default:
                    break;
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
            ChannelUID channelUID = channel.getUID();
            String[] parts = channelUID.getId()
                    .split(Pattern.quote(StiebelHeatPumpBindingConstants.CHANNELGROUPSEPERATOR));
            String channelId = parts[parts.length - 1];
            Request request = heatPumpConfiguration.getRequestByChannelId(channelId);

            if (request != null) {
                String requestbyte = String.format("%02X", request.getRequestByte());
                Type dataType = request.getRecordDefinitionByChannelId(channelId).getDataType();
                RecordDefinition record = request.getRecordDefinitionByChannelId(channelId);
                record.setChannelid(channelUID.getId());
                switch (dataType) {
                    case Settings:
                        if (!heatPumpSettingRefresh.getRequests().contains(request)) {
                            heatPumpSettingRefresh.getRequests().add(request);
                            logger.info("Request {} added to setting refresh scheduler.", requestbyte);
                        }
                        break;
                    default:
                        if (!heatPumpSensorStatusRefresh.getRequests().contains(request)) {
                            heatPumpSensorStatusRefresh.getRequests().add(request);
                            logger.info("Request {} added to sensor/status refresh scheduler.", requestbyte);
                        }
                        break;
                }
            }
        }
    }
}
