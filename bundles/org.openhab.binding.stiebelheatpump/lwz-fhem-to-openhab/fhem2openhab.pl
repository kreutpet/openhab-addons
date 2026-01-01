#!/usr/bin/env perl

########################################################################################
# Conversion from FHEM to openHAB.
# Author: Robert Huitl <git@huitl.de>
#
# This file is derived from the original 00_THZ.pm file from FHEM (from the beginning to
# the end of the record definitions), and then a conversion part that transforms the
# definitions into the XML files used by org.openhab.binding.stiebelheatpump.
########################################################################################

##############################################
# 00_THZ
# $Id$
# by immi 06/2022
my $thzversion = "0.205";
# this code is based on the hard work of Robert; I just tried to port it
# http://robert.penz.name/heat-pump-lwz/
########################################################################################
#
#  This programm is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  The GNU General Public License can be found at
#  http://www.gnu.org/copyleft/gpl.html.
#  A copy is found in the textfile GPL.txt and important notices to the license
#  from the author is found in LICENSE.txt distributed with these scripts.
#
#  This script is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
########################################################################################

package main;
use strict;
use warnings;
#
# note: almost all imports have been removed for the conversion from FHEM to openHAB.
#

########################################################################################
#
# %parsinghash  - known type of message structure
# 
########################################################################################

my %parsinghash = (
  #msgtype => parsingrule  
  "01pxx206" => [["p37Fanstage1AirflowInlet: ", 4, 4, "hex", 1],	[" p38Fanstage2AirflowInlet: ", 8, 4, "hex", 1],	[" p39Fanstage3AirflowInlet: ", 12, 4, "hex", 1],
              [" p40Fanstage1AirflowOutlet: ", 16, 4, "hex", 1],	[" p41Fanstage2AirflowOutlet: ", 20, 4, "hex", 1],	[" p42Fanstage3AirflowOutlet: ", 24, 4, "hex", 1],
              [" p43UnschedVent3: ", 	       28, 4, "hex", 1],	[" p44UnschedVent2: ", 32, 4, "hex", 1],		[" p45UnschedVent1: ", 36, 4, "hex", 1],
              [" p46UnschedVent0: ", 	       40, 4, "hex", 1],	[" p75PassiveCooling: ", 44, 4, "hex", 1]
              ],
  "01pxx214" => [["p37Fanstage1AirflowInlet: ", 4, 2, "hex", 1],	[" p38Fanstage2AirflowInlet: ", 6, 2, "hex", 1],	[" p39Fanstage3AirflowInlet: ", 8, 2, "hex", 1],
	      [" p40Fanstage1AirflowOutlet: ", 10, 2, "hex", 1],	[" p41Fanstage2AirflowOutlet: ", 12, 2, "hex", 1],	[" p42Fanstage3AirflowOutlet: ", 14, 2, "hex", 1],
	      [" p43UnschedVent3: ", 	    16, 4, "hex", 1],		[" p44UnschedVent2: ", 20, 4, "hex", 1],		[" p45UnschedVent1: ", 24, 4, "hex", 1],
	      [" p46UnschedVent0: ", 	    28, 4, "hex", 1],		[" p75PassiveCooling: ", 32, 2, "hex", 1]
	      ],
  "03pxx206" => [["UpTempLimitDefrostEvaporatorEnd: ", 4, 4, "hex", 10],[" MaxTimeDefrostEvaporator: ", 8, 4, "hex", 1], 	[" LimitTempCondenserElectBoost: ", 12, 4, "hex", 10],
	      [" LimitTempCondenserDefrostTerm: ", 16, 4, "hex", 10],	[" p47CompressorRestartDelay: ", 20, 2, "hex", 1], 	[" p48MainFanSpeed: ", 22, 2, "hex", 1]
 	      ],
  "04pxx206" => [["MaxDefrostDurationAAExchenger: ", 4, 2, "hex", 1],	[" DefrostStartThreshold: ", 6, 4, "hex", 10],	[" VolumeFlowFilterReplacement: ", 10, 4, "hex", 1]
	      ],
  "05pxx206" => [["p13GradientHC1: ", 	4, 4, "hex", 10],	[" p14LowEndHC1: ", 8, 4, "hex", 10],			[" p15RoomInfluenceHC1: ",	12, 2, "hex", 10],
	      [" p16GradientHC2: ",	14, 4, "hex", 10],	[" p17LowEndHC2: ", 18, 4, "hex", 10],			[" p18RoomInfluenceHC2: ",	22, 2, "hex", 10],
	      [" p19FlowProportionHC1: ",24, 4, "hex", 1],	[" p20FlowProportionHC2: ", 28, 4, "hex", 1],		[" MaxSetHeatFlowTempHC1: ",	32, 4, "hex", 10],
	      [" MinSetHeatFlowTempHC1: ",36, 4, "hex", 10],	[" MaxSetHeatFlowTempHC2: ", 40, 4, "hex", 10],		[" MinSetHeatFlowTempHC2: ",	44, 4, "hex", 10],
	      ],
  "06pxx206" => [["p21Hyst1: ", 	 4, 2, "hex", 10],	[" p22Hyst2: ",			 6, 2, "hex", 10],	[" p23Hyst3: ", 		 8, 2, "hex", 10],
	      [" p24Hyst4: ", 		10, 2, "hex", 10],	[" p25Hyst5: ",			12, 2, "hex", 10],	[" p26Hyst6: ", 		14, 2, "hex", 10],
	      [" p27Hyst7: ", 		16, 2, "hex", 10],	[" p28Hyst8: ",			18, 2, "hex", 10],	[" p29HystAsymmetry: ",		20, 2, "hex", 1],
	      [" p30integralComponent: ",22, 4, "hex", 1],	[" p31MaxBoostStages: ",	26, 2, "hex", 1],	[" MaxHeatFlowTemp: ",		28, 4, "hex", 10],
	      [" p49SummerModeTemp: ", 	32, 4, "hex", 10],	[" p50SummerModeHysteresis: ",	36, 4, "hex", 10],	[" p77OutTempFilterTime: ",	40, 4, "hex", 1],
	      [" p78DualModePoint: ", 	44, 4, "hex2int", 10],	[" p79BoosterTimeoutHC: ",	48, 2, "hex", 1] 	      
          ],
  "07pxx206" => [["p32HystDHW: ", 	4, 2, "hex", 10],	[" p33BoosterTimeoutDHW: ",	6, 2, "hex", 1],	[" p34TempLimitBoostDHW: ",	8, 4, "hex2int", 10],	[" p35PasteurisationInterval: ", 12, 2, "hex", 1],
	      [" p36MaxDurationDHWLoad: ",14, 2, "hex", 1],	[" pasteurisationTemp: ",	16, 4, "hex", 10],	[" maxBoostStagesDHW: ",	20, 2, "hex", 1],
	      [" p84EnableDHWBuffer: ",	22, 2, "hex", 1]
	      ],
  "08pxx206" => [["p80EnableSolar: ", 	4, 2, "hex", 1],	[" p81DiffTempSolarLoading: ",	 6, 4, "hex", 10],	[" p82DelayCompStartSolar: ",	10, 2, "hex", 1],
	      [" p84DHWTempSolarMode: ",12, 4, "hex", 10],	[" HystDiffTempSolar: ",	16, 4, "hex", 10],	[" CollectLimitTempSolar: ",	20, 4, "hex", 10]
	      ],
  "09his" => [["compressorHeating: ", 	 4, 4,  "hex", 1],	[" compressorCooling: ",	 8, 4, "hex", 1],
	      [" compressorDHW: ",	12, 4, "hex", 1],	[" boosterDHW: ",		16, 4, "hex", 1],
	      [" boosterHeating: ",	20, 4, "hex", 1]
	      ],
  "09his206" => [["operatingHours1: ",	 4, 4, "hex", 1],	[" operatingHours2: ",	8, 4, "hex", 1],
	      [" heatingHours: ",	12, 4, "hex", 1],	[" DHWhours: ",		16, 4, "hex", 1],
	      [" coolingHours: ",	20, 4, "hex", 1]
	      ],
  "0Apxx206" => [["p54MinPumpCycles: ",	4, 2, "hex", 1],	[" p55MaxPumpCycles: ",	 6, 4, "hex", 1],	 [" p56OutTempMaxPumpCycles: ",	10, 4, "hex", 10],
	      [" p57OutTempMinPumpCycles: ",14, 4, "hex", 10],	[" p58SuppressTempCaptPumpStart: ", 18, 4, "hex", 1]
	      ],
  "0Bpxx206" => [["progHC1StartTime: ", 4, 4, "hex2time", 1], 	[" progHC1EndTime: ",	 8, 4, "hex2time", 1],
	      [" progHC1Monday: ",	13, 1, "bit0", 1],	[" progHC1Tuesday: ",	13, 1, "bit1", 1],	
		[" progHC1Wednesday: ",	13, 1, "bit2", 1],	[" progHC1Thursday: ",	13, 1, "bit3", 1],
		[" progHC1Friday: ",	12, 1, "bit0", 1],	[" progHC1Saturday: ",	12, 1, "bit1", 1],
		[" progHC1Sunday: ",	12, 1, "bit2", 1],	[" progHC1Enable: ",	14, 2, "hex", 1],
		[" progHC2StartTime: ",	16, 4, "hex2time", 1],	[" progHC2EndTime: ",	20, 4, "hex2time", 1],
		[" progHC2Monday: ", 	25, 1, "bit0", 1],	[" progHC2Tuesday: ",	25, 1, "bit1", 1],	
		[" progHC2Wednesday: ", 25, 1, "bit2", 1],	[" progHC2Thursday: ",	25, 1, "bit3", 1],
		[" progHC2Friday: ", 	24, 1, "bit0", 1],	[" progHC2Saturday: ",	24, 1, "bit1", 1],
		[" progHC2Sunday: ", 	24, 1, "bit2", 1],	[" progHC2Enable: ",	26, 2, "hex", 1]
	      ],
  "0Cpxx206" => [["progDHWStartTime: ",	 4, 4, "hex2time", 1],	[" progDHWEndTime: ",	 8, 4, "hex2time", 1],
		[" progDHWMonday: ",	13, 1, "bit0", 1],	[" progDHWTuesday: ",	13, 1, "bit1", 1],	
		[" progDHWWednesday: ",	13, 1, "bit2", 1],	[" progDHWThursday: ",	13, 1, "bit3", 1],
		[" progDHWFriday: ",	12, 1, "bit0", 1],	[" progDHWSaturday: ",	12, 1, "bit1", 1],
		[" progDHWSunday: ",	12, 1, "bit2", 1],	[" progDHWEnable: ",	14, 2, "hex", 1],
 	      ],
  "0Dpxx206" => [["progFAN1StartTime: ",4, 4, "hex2time", 1],	[" progFAN1EndTime: ",	 8, 4, "hex2time", 1],
		[" progFAN1Monday: ", 	13, 1, "bit0", 1],	[" progFAN1Tuesday: ",	13, 1, "bit1", 1],	
		[" progFAN1Wednesday: ",13, 1, "bit2", 1],	[" progFAN1Thursday: ",	13, 1, "bit3", 1],
		[" progFAN1Friday: ",	12, 1, "bit0", 1],	[" progFAN1Saturday: ",	12, 1, "bit1", 1],
		[" progFAN1Sunday: ",	12, 1, "bit2", 1],	[" progFAN1Enable: ",	14, 2, "hex", 1],
		[" progFAN2StartTime: ",16, 4, "hex2time", 1],	[" progFAN2EndTime: ",	20, 4, "hex2time", 1],
		[" progFAN2Monday: ",	25, 1, "bit0", 1],	[" progFAN2Tuesday: ",	25, 1, "bit1", 1],	
		[" progFAN2Wednesday: ",25, 1, "bit2", 1],	[" progFAN2Thursday: ",	25, 1, "bit3", 1],
		[" progFAN2Friday: ",	24, 1, "bit0", 1],	[" progFAN2Saturday: ",	24, 1, "bit1", 1],
		[" progFAN2Sunday: ",	24, 1, "bit2", 1],	[" progFAN2Enable: ",	26, 2, "hex", 1]
 	      ],  
  "0Epxx206" => [["p59RestartBeforeSetbackEnd: ", 4, 4, "hex", 1]
	      ], 
  "0Fpxx206" => [["pA0DurationUntilAbsenceStart: ", 4, 4, "hex", 10],	[" pA0AbsenceDuration: ",	 8, 4, "hex", 10], [" pA0EnableAbsenceProg: ", 12, 2, "hex", 1]
	      ], 	 
  "10pxx206" => [["p70StartDryHeat: ", 	 4, 2, "hex", 1],	[" p71BaseTemp: ",	 6, 4, "hex", 10],	[" p72PeakTemp: ", 10, 4, "hex", 10],
	      [" p73TempDuration: ", 	14, 4, "hex", 1],	[" p74TempIncrease: ",	18, 4, "hex", 10]
	      ],
  "16sol" => [["collectorTemp: ",	4, 4, "hex2int", 10],	[" dhwTemp: ", 	 8, 4, "hex2int", 10],
	      [" flowTemp: ",		12, 4, "hex2int", 10],	[" edSolPump: ",	16, 2, "hex2int", 1],
	      [" out: ",		26, 4, "raw", 1],	[" status: ",	30, 2, "raw", 1]
	      ],
  "17pxx206" => [["p01RoomTempDay: ", 	 4, 4,  "hex", 10],	[" p02RoomTempNight: ",	 8, 4, "hex", 10],
	      [" p03RoomTempStandby: ",	12, 4,  "hex", 10], 	[" p04DHWsetTempDay: ",	16, 4,  "hex", 10], 
	      [" p05DHWsetTempNight: ",	20, 4,  "hex", 10], 	[" p06DHWsetTempStandby: ",24, 4,  "hex", 10], 
	      [" p07FanStageDay: ",	28, 2,  "hex", 1], 	[" p08FanStageNight: ",	30, 2,  "hex", 1],
	      [" p09FanStageStandby: ",	32, 2,  "hex", 1], 	[" p10HCTempManual: ",	34, 4,  "hex", 10],
	      [" p11DHWsetTempManual: ",38, 4,  "hex", 10],  	[" p12FanStageManual: ",	42, 2,  "hex", 1],
	      ],
  "D1last" => [["number_of_faults: ",	 4, 2, "hex", 1],	
	      [" fault0CODE: ",		 8, 2, "faultmap", 1],	[" fault0TIME: ",	12, 4, "turnhex2time", 1],  [" fault0DATE: ",	16, 4, "turnhexdate", 1],
	      [" fault1CODE: ",		20, 2, "faultmap", 1],	[" fault1TIME: ",	24, 4, "turnhex2time", 1],  [" fault1DATE: ",	28, 4, "turnhexdate", 1],
	      [" fault2CODE: ",		32, 2, "faultmap", 1],	[" fault2TIME: ",	36, 4, "turnhex2time", 1],  [" fault2DATE: ",	40, 4, "turnhexdate", 1],
	      [" fault3CODE: ",		44, 2, "faultmap", 1],	[" fault3TIME: ",	48, 4, "turnhex2time", 1],  [" fault3DATE: ",	52, 4, "turnhexdate", 1]
	      ],
  "D1last206" => [["number_of_faults: ",	 4, 2, "hex", 1],	
	      [" fault0CODE: ",		 8, 4, "faultmap", 1],	[" fault0TIME: ",	12, 4, "hex2time", 1],  [" fault0DATE: ",	16, 4, "hexdate", 1],
	      [" fault1CODE: ",		20, 4, "faultmap", 1],	[" fault1TIME: ",	24, 4, "hex2time", 1],  [" fault1DATE: ",	28, 4, "hexdate", 1],
	      [" fault2CODE: ",		32, 4, "faultmap", 1],	[" fault2TIME: ",	36, 4, "hex2time", 1],  [" fault2DATE: ",	40, 4, "hexdate", 1],
	      [" fault3CODE: ",		44, 4, "faultmap", 1],	[" fault3TIME: ",	48, 4, "hex2time", 1],  [" fault3DATE: ",	52, 4, "hexdate", 1]
	      ],
  "E8fan"	=> [[" inputFanSpeed: ",	58, 2, "hex", 1],    # like in sGlobal
		[" outputFanSpeed: ",		60, 2, "hex", 1],    # like in sGlobal
		[" pFanstageXAirflowInlet: ",	62, 4, "hex", 1],    # m3/h  corresponding to p37Fanstage1AirflowInlet or p38Fanstage2AirflowInlet
		[" pFanstageXAirflowOutlet: ",	66, 4, "hex", 1],    # m3/h corresponding to p40Fanstage1AirflowOutlet or p41Fanstage2AirflowOutlet
		[" inputFanPower: ",		70, 2, "hex", 1],    # like in sGlobal
		[" outputFanPower: ",		72, 2, "hex", 1],	 # like in sGlobal		
		  ],
  "E8fan206" => [["statusAFC: ",		4, 4, "hex", 1],		# 0=init air flow calibration (16:00) 4=normal fan operation
		[" supplyFanSpeedCAL: ",	 8, 4, "hex", 60],	# calibration speed
		[" exhaustFanSpeedCAL: ",	12, 4, "hex", 60], 	
		[" supplyFanAirflowCAL: ",	16, 4, "hex", 100], 	# calibration air flow volume
		[" exhaustFanAirflowCAL: ",	20, 4, "hex", 100],
		[" supplyFanSpeed: ",		24, 4, "hex", 1], 	# actual fan speed in 1/s
		[" exhaustFanSpeed: ",		28, 4, "hex", 1],
		[" supplyFanAirflowSet: ",	32, 4, "hex", 1],		# actual air flow volume setting in m3/h
		[" exhaustFanAirflowSet: ",	36, 4, "hex", 1],
		[" supplyFanSpeedTarget: ",	40, 4, "hex", 1],		# target fan speed in %
		[" exhaustFanSpeedTarget: ",	44, 4, "hex", 1],
		[" supplyFanSpeed0: ",		48, 4, "hex", 10], 		
		[" exhaustFanSpeed0: ",		52, 4, "hex", 10], 	
		[" supplyFanSpeed200: ",	56, 4, "hex", 10],
		[" exhaustFanSpeed200: ",	60, 4, "hex", 10],
		[" airflowTolerance: ",		64, 2, "hex", 1],
		[" airflowCalibrationInterval: ",66, 2, "hex", 1],		# calibration interval
		[" timeToCalibration: ",	68, 2, "hex", 1]		# days to next calibration
		  ],
  "EEprg206" => [["opMode: ",		4, 2, 	"opmode2", 1],	[" ProgStateHC: ", 	10, 2, "opmodehc", 1],	[" ProgStateDHW: ",	12, 2, "opmodehc", 1],
	      [" ProgStateFAN: ", 	14, 2, 	"opmodehc", 1],	[" BaseTimeAP0: ", 	16, 8, "hex", 1],		[" StatusAP0: ",		24, 2, "hex", 1],
	      [" StartTimeAP0: ",	26, 8, 	"hex", 1], 	[" EndTimeAP0: ", 	34, 8, "hex", 1]
	      ],
  "F2ctrl"  => [["heatRequest: ",		 4, 2, "hex", 1],	# 0=DHW 2=heat 5=off 6=defrostEva
		[" heatRequest2: ",		 6, 2, "hex", 1],	# same as heatRequest
		[" hcStage: ", 			 8, 2, "hex", 1],  	# 0=off 1=solar 2=heatPump 3=boost1 4=boost2 5=boost3
		[" dhwStage: ",			10, 2, "hex", 1],	# 0=off, 1=solar, 2=heatPump 3=boostMax
		[" heatStageControlModul: ",	12, 2, "hex", 1], 	# either hcStage or dhwStage depending from heatRequest
		[" compBlockTime: ", 		14, 4, "hex2int", 1],	# remaining compressor block time
		[" pasteurisationMode: ",	18, 2, "hex",  1],	# 0=off 1=on
		[" defrostEvaporator: ",	20, 2, "raw",  1],	# 10=off 30=defrostEva
		[" boosterStage2: ",		22, 1, "bit3", 1],	# booster 2		
		[" solarPump: ",		22, 1, "bit2", 1],	# solar pump
		[" boosterStage1: ",		22, 1, "bit1", 1],	# booster 1
		[" compressor: ",		22, 1, "bit0", 1],	# compressor
		[" heatPipeValve: ",		23, 1, "bit3", 1],	# heat pipe valve
		[" diverterValve: ",		23, 1, "bit2", 1],	# diverter valve
		[" dhwPump: ",			23, 1, "bit1", 1],	# dhw pump
		[" heatingCircuitPump: ",	23, 1, "bit0", 1],	# hc pump
		[" mixerOpen: ",		25, 1, "bit1", 1],	# mixer open
		[" mixerClosed: ",		25, 1, "bit0", 1],	# mixer closed
		[" sensorBits1: ", 		26, 2, "raw", 1],	# sensor condenser temperature ??
		[" sensorBits2: ", 		28, 2, "raw", 1],	# sensor low pressure ??
		[" boostBlockTimeAfterPumpStart: ", 30, 4, "hex2int", 1],# after each  pump start (dhw or heat circuit)
		[" boostBlockTimeAfterHD: ",	34, 4, "hex2int", 1]	# ??
          ],
  "F3dhw"  => [["dhwTemp: ",		 4, 4, "hex2int", 10],	[" outsideTemp: ", 	 8, 4, "hex2int", 10],
	      [" dhwSetTemp: ",		12, 4, "hex2int", 10],	[" compBlockTime: ",	16, 4, "hex2int", 1],
	      [" out: ", 		20, 4, "raw", 1],	[" heatBlockTime: ",	24, 4, "hex2int", 1],
	      [" dhwBoosterStage: ",	28, 2, "hex", 1],	[" pasteurisationMode: ",32, 2, "hex", 1],
	      [" dhwOpMode: ",		34, 2, "opmodehc", 1],	[" x36: ",		36, 4, "raw", 1]
  	      ],
  "F4hc1"  => [["outsideTemp: ",	4,  4, "hex2int", 10],	[" x08: ",	 	 8, 4, "hex2int", 10],
	      [" returnTemp: ",		12, 4, "hex2int", 10],	[" integralHeat: ",	16, 4, "hex2int", 1],
	      [" flowTemp: ",		20, 4, "hex2int", 10],	[" heatSetTemp: ", 	24, 4, "hex2int", 10], 
	      [" heatTemp: ",		28, 4, "hex2int", 10],  
	      [" seasonMode: ",		38, 2, "somwinmode", 1],#	[" x40: ",		40, 4, "hex2int", 1],
	      [" integralSwitch: ",	44, 4, "hex2int", 1],	[" hcOpMode: ",		48, 2, "opmodehc", 1],
		#[" x52: ",		52, 4, "hex2int", 1],
	      [" roomSetTemp: ",	56, 4, "hex2int", 10],	[" x60: ", 		60, 4, "hex2int", 10],
	      [" x64: ", 		64, 4, "hex2int", 10],	[" insideTempRC: ",	68, 4, "hex2int", 10],
	      [" x72: ", 		72, 4, "hex2int", 10],	[" x76: ", 		76, 4, "hex2int", 10],
	      [" onHysteresisNo: ", 	32, 2, "hex", 1],	[" offHysteresisNo: ",	34, 2, "hex", 1],
	      [" hcBoosterStage: ",	36, 2, "hex", 1]
         ],
  "F4hc1214" => [["outsideTemp: ",	 4, 4, "hex2int", 10],	[" x08: ",		 8, 4, "raw", 1],
 	      [" returnTemp: ",		12, 4, "hex2int", 10],	[" integralHeat: ",	16, 4, "hex2int", 1],
	      [" flowTemp: ",		20, 4, "hex2int", 10],	[" heatSetTemp: ", 	24, 4, "hex2int", 10], 
	      [" heatTemp: ",		28, 4, "hex2int", 10],  
	      [" seasonMode: ",		38, 2, "somwinmode", 1],
	      [" integralSwitch: ",	44, 4, "hex2int", 1],	[" hcOpMode: ",		48, 2, "opmodehc", 1], 	      
	      [" roomSetTemp: ",	62, 4, "hex2int", 10],	[" x60: ", 		60, 4, "hex2int", 10],
	      [" x64: ", 		64, 4, "raw", 1],	[" insideTempRC: ", 	68, 4, "hex2int", 10],
	      [" x72: ", 		72, 4, "raw", 1],	[" x76: ", 		76, 4, "raw", 1],
	      [" onHysteresisNo: ",	32, 2, "hex", 1],	[" offHysteresisNo: ",	34, 2, "hex", 1],
	      [" hcBoosterStage: ",	36, 2, "hex", 1]
         ],
  # contribution from joerg  hellijo  Antwort #1085  Juni 2022
   "F4hc1214j" => [["outsideTemp: ",	4, 4, "hex2int", 10],	[" x08: ",		8, 4, "raw", 1],
 	      [" returnTemp: ",		12, 4, "hex2int", 10],	[" integralHeat: ",	16, 4, "hex2int", 1],
	      [" flowTemp: ",		20, 4, "hex2int", 10],	[" heatSetTemp: ", 	24, 4, "hex2int", 10],
	      [" heatTemp: ",		28, 4, "hex2int", 10], 
	      [" seasonMode: ",		38, 2, "somwinmode", 1],
	      [" integralSwitch: ",	44, 4, "hex2int", 1],	[" hcOpMode: ",		48, 2, "opmodehc", 1],
	      [" roomSetTemp: ",	62, 4, "hex2int", 10],	[" x50: ", 		50, 4, "hex2int", 10],
	      [" x66: ", 		66, 4, "raw", 1],	[" insideTempRC: ", 	74, 4, "hex2int", 10],
	      [" x70: ", 		70, 4, "raw", 1],	[" x76: ", 		78, 4, "raw", 1],
	      [" onHysteresisNo: ",	32, 2, "hex", 1],	[" offHysteresisNo: ",	34, 2, "hex", 1],
	      [" hcStage: ",		36, 2, "hex", 1],# 0=Aus; 1=Solar; 2=V1
	      [" boosterStage2: ", 	40, 1, "bit3", 1],
	      [" x58: ", 		58, 4, "raw", 1],	[" x54: ", 		54, 4, "raw", 1],
	      [" blockTimeAfterCompStart: ", 82, 4, "hex2int", 1], [" insideTemp: ", 	86, 4, "hex2int", 10],
	      [" solarPump: ",		40, 1, 	"bit2", 1],	[" boosterStage1: ",	40, 1, 	"bit1", 1],
	      [" compressor: ",		40, 1, 	"bit0", 1],	[" heatPipeValve: ",	41, 1, 	"bit3", 1],
	      [" diverterValve: ",	41, 1, 	"bit2", 1],	[" dhwPump: ",		41, 1, 	"bit1", 1],
	      [" heatingCircuitPump: ",	41, 1, 	"bit0", 1],     [" mixerOpen: ",	43, 1, 	"bit1", 1],
	      [" mixerClosed: ",	43, 1, 	"bit0", 1]
         ],
  "F5hc2"  => [["outsideTemp: ",	4, 4, "hex2int", 10],	[" returnTemp: ",	 8, 4, "hex2int", 10],
	      [" vorlaufTemp: ",	12, 4, "hex2int", 10],	[" heatSetTemp: ",	16, 4, "hex2int", 10],
	      [" heatTemp: ", 		20, 4, "hex2int", 10],	[" stellgroesse: ",	24, 4, "hex2int", 10], 
	      [" seasonMode: ",		30, 2, "somwinmode",1],	[" hcOpMode: ",		36, 2, "opmodehc", 1] 
         ],
  "F6sys206" => [["userSetFanStage: ",	30, 2, "hex", 1],	[" userSetFanRemainingTime: ", 36, 4, "hex", 1],
	      [" lastErrors: ",		4, 8, "hex2error", 1],	     
         ],
  "FBglob" => [["outsideTemp: ",	8, 4, "hex2int", 10],	[" flowTemp: ",		12, 4, "hex2int", 10],
	      [" returnTemp: ",		16, 4, "hex2int", 10],	[" hotGasTemp: ", 	20, 4, "hex2int", 10],
	      [" dhwTemp: ",	 	24, 4, "hex2int", 10],	[" flowTempHC2: ",	28, 4, "hex2int", 10],
	      [" evaporatorTemp: ",	36, 4, "hex2int", 10],	[" condenserTemp: ",	40, 4, "hex2int", 10],
	      [" mixerOpen: ",		45, 1, "bit0", 1],	[" mixerClosed: ",	45, 1, "bit1", 1],
	      [" heatPipeValve: ",	45, 1, "bit2", 1],	[" diverterValve: ",	45, 1, "bit3", 1],
	      [" dhwPump: ",		44, 1, "bit0", 1],	[" heatingCircuitPump: ",44, 1, "bit1", 1],
	      [" solarPump: ",		44, 1, "bit3", 1],	[" compressor: ",	47, 1, "bit3", 1],
	      [" boosterStage3: ",	46, 1, "bit0", 1],	[" boosterStage2: ",	46, 1, "bit1", 1],
	      [" boosterStage1: ",	46, 1, "bit2", 1],	[" highPressureSensor: ",49, 1, "nbit0", 1],
	      [" lowPressureSensor: ",	49, 1, "nbit1", 1],	[" evaporatorIceMonitor: ",49, 1, "bit2", 1],
	      [" signalAnode: ",	49, 1, "bit3", 1],	[" evuRelease: ",	48, 1, "bit0", 1],
	      [" ovenFireplace: ",	48, 1, "bit1", 1],	[" STB: ",		48, 1, "bit2", 1],
	      [" outputVentilatorPower: ",50, 4, "hex", 10],	[" inputVentilatorPower: ",54, 4, "hex", 10],	[" mainVentilatorPower: ",	58, 4, "hex", 10],
	      [" outputVentilatorSpeed: ",62, 4, "hex", 1],	[" inputVentilatorSpeed: ",66, 4, "hex", 1],		[" mainVentilatorSpeed: ",	70, 4, "hex", 1],
	      [" outside_tempFiltered: ",74, 4, "hex2int", 10],	[" relHumidity: ",	78, 4, "hex2int", 10],
	      [" dewPoint: ",		82, 4, "hex2int", 10],
	      [" P_Nd: ",		86, 4, "hex2int", 100],	[" P_Hd: ",		90, 4, "hex2int", 100],
	      [" actualPower_Qc: ",	94, 8, "esp_mant", 1],	[" actualPower_Pel: ",	102, 8, "esp_mant", 1],
	      [" collectorTemp: ",	4,  4, "hex2int", 10],	[" insideTemp: ",	32, 4, "hex2int", 10], 
	      [" windowOpen: ",		47, 1, "bit2", 1], 	# board X18-1 clamp X4-FA (FensterAuf): window open - signal out 230V
	      [" quickAirVent: ",	48, 1, "bit3", 1], 	# board X15-8 clamp X4-SL (SchnellLüftung): quickAirVent - signal in 230V
	      [" flowRate: ",		110, 4, "hex", 100]	# board X51 sensor P5 (on newer models B1 flow temp as well) changed to l/min as suggested by TheTrumpeter Antwort #771
#	      [" p_HCw: ",		114, 4, "hex", 100],	# board X4-1..3 sensor P4 HC water pressure
#	      [" humidityAirOut: ",	154, 4, "hex", 100]	# board X4-4..6 sensor B15
#	      TODO I could not read these on LWZ 303 SOL with 4.39.
          ],
  "FBglob214" => [["outsideTemp: ", 	8, 4, "hex2int", 10],	[" flowTemp: ",		12, 4, "hex2int", 10],
	      [" returnTemp: ",		16, 4, "hex2int", 10],	[" hotGasTemp: ", 	20, 4, "hex2int", 10],
	      [" dhwTemp: ",	 	24, 4, "hex2int", 10],	[" flowTempHC2: ",	28, 4, "hex2int", 10],
	      [" evaporatorTemp: ",	36, 4, "hex2int", 10],	[" condenserTemp: ",	40, 4, "hex2int", 10],
	      [" mixerOpen: ",		47, 1, "bit1", 1],	[" mixerClosed: ",	47, 1, "bit0", 1],
	      [" heatPipeValve: ",	45, 1, "bit3", 1],	[" diverterValve: ",	45, 1, "bit2", 1],
	      [" dhwPump: ",		45, 1, "bit1", 1],	[" heatingCircuitPump: ",	45, 1, "bit0", 1],
	      [" solarPump: ",		44, 1, "bit2", 1],	[" compressor: ",		44, 1, "bit0", 1],
	      [" boosterStage2: ",	44, 1, "bit3", 1],	[" boosterStage3: ",	44, 1, "n.a.", 1],
	      [" boosterStage1: ",	44, 1, "bit1", 1],	[" highPressureSensor: ",	54, 1, "bit3", 1],
	      [" lowPressureSensor: ",	54, 1, "bit2", 1],	[" evaporatorIceMonitor: ",55, 1, "bit3", 1],
	      [" signalAnode: ",	54, 1, "bit1", 1],	[" evuRelease: ",		48, 1, "n.a.", 1],
	      [" ovenFireplace: ",	54, 1, "bit0", 1],	[" STB: ",		48, 1, "n.a.", 1],
	      [" outputVentilatorPower: ",48, 2, "hex", 1],	[" inputVentilatorPower: ",50, 2, "hex", 1],	[" mainVentilatorPower: ",	52, 2, "hex", 255/100],         
	      [" outputVentilatorSpeed: ",56, 2, "hex", 1],	[" inputVentilatorSpeed: ",58, 2, "hex", 1],	[" mainVentilatorSpeed: ",	60, 2, "hex", 1],
	      [" outsideTempFiltered: ",64, 4, "hex2int", 10],	[" relHumidity: ",	70, 4, "n.a.", 1],
	      [" dewPoint: ",		5, 4, "n.a.", 1],
	      [" P_Nd: ",		5, 4, "n.a.", 1],	[" P_Hd: ",		5, 4, "n.a.", 1],
	      [" actualPower_Qc: ",	5, 8, "n.a.", 1],	[" actualPower_Pel: ",	5, 8, "n.a.", 1],
	      [" collectorTemp: ",	4,  4, "hex2int", 10],	[" insideTemp: ",	32, 4, "hex2int", 10]
	      ],
  "FBglob206" => [["outsideTemp: ", 	8, 4, "hex2int", 10],	[" flowTemp: ",		12, 4, "hex2int", 10],
	      [" returnTemp: ",		16, 4, "hex2int", 10],	[" hotGasTemp: ", 	20, 4, "hex2int", 10],
	      [" dhwTemp: ",		24, 4, "hex2int", 10],	[" flowTempHC2: ",	28, 4, "hex2int", 10],
	      [" evaporatorTemp: ",	36, 4, "hex2int", 10],	[" condenserTemp: ",	40, 4, "hex2int", 10],
	      [" mixerOpen: ",		47, 1, "bit1", 1],	[" mixerClosed: ",	47, 1, "bit0", 1],
	      [" heatPipeValve: ",	45, 1, "bit3", 1],	[" diverterValve: ",	45, 1, "bit2", 1],
	      [" dhwPump: ",		45, 1, "bit1", 1],	[" heatingCircuitPump: ",45, 1, "bit0", 1],
	      [" solarPump: ",		44, 1, "n.a", 1],	[" compressor: ",	44, 1, "bit0", 1],
	      [" boosterStage3: ",	44, 1, "bit3", 1],	[" boosterStage2: ",	44, 1, "bit2", 1], 	      
	      [" boosterStage1: ",	44, 1, "bit1", 1],	[" highPressureSensor: ",54, 1, "bit3", 1],
	      [" lowPressureSensor: ",	54, 1, "bit2", 1],	[" evaporatorIceMonitor: ",55, 1, "bit3", 1],
	      [" signalAnode: ",	54, 1, "bit1", 1],	[" evuRelease: ",	48, 1, "n.a.", 1],
	      [" ovenFireplace: ",	54, 1, "bit0", 1],	[" STB: ",		48, 1, "n.a.", 1],
	      [" outputVentilatorPower: ",48,2, "hex", 1],	[" inputVentilatorPower: ",50, 2, "hex", 1],	[" mainVentilatorPower: ",	52, 2, "hex", 255/100],          
	      [" outputVentilatorSpeed: ",56,2, "hex", 1],	[" inputVentilatorSpeed: ",58, 2, "hex", 1],	[" mainVentilatorSpeed: ",	60, 2, "hex", 1],
	      [" outsideTempFiltered: ",64, 4, "hex2int", 10],	[" relHumidity: ",	70, 4, "n.a.", 1],
	      [" dewPoint: ",		 5, 4, "n.a.", 1],
	      [" P_Nd: ",		 5, 4, "n.a.", 1],	[" P_Hd: ",		5, 4, "n.a.", 1],
	      [" actualPower_Qc: ",	 5, 8, "n.a.", 1],	[" actualPower_Pel: ",	5, 8, "n.a.", 1],
	      [" collectorTemp: ",	 4, 4, "hex2int", 10],	[" insideTemp: ",	32, 4, "hex2int", 10] 
	      ],
  "FCtime" => [["Weekday: ",	5, 1, "weekday", 1],	[" Hour: ",	6, 2, "hex", 1],
	      [" Min: ",	8, 2, "hex", 1],	[" Sec: ",	10, 2, "hex", 1],
	      [" Date: ", 	12, 2, "year", 1],	["/", 		14, 2, "hex", 1],
	      ["/", 		16, 2, "hex", 1]
	     ],
  "FCtime206" => [["Weekday: ",		7, 1,  "weekday", 1],	[" pClockHour: ",	8, 2, "hex", 1],
	      [" pClockMinutes: ",	10, 2, "hex", 1],	[" Sec: ",		12, 2, "hex", 1],
	      [" pClockYear: ",		14, 2, "hex", 1],	[" pClockMonth: ",	18, 2, "hex", 1],
	      [" pClockDay: ",		20, 2, "hex", 1]
	     ],
  "FDfirm" => [["version: ", 	4, 4, "hexdate", 1]
	     ],
  "FEfirmId" => [[" HW: ",		30,  2, "hex", 1], 	[" SW: ",	32,  4, "swver", 1],
	      [" Date: ",		36, 22, "hex2ascii", 1]
	     ],
  "0A0176Dis" => [[" switchingProg: ",	11, 1, "bit0", 1],  [" compressor: ",	11, 1, "bit1", 1],
	      [" heatingHC: ",		11, 1, "bit2", 1],  [" heatingDHW: ",	10, 1, "bit0", 1],
	      [" boosterHC: ",		10, 1, "bit1", 1],  [" filterBoth: ",	 9, 1, "bit0", 1],
	      [" ventStage: ",		 9, 1, "bit1", 1],  [" pumpHC: ",	 9, 1, "bit2", 1],
	      [" defrost: ",		 9, 1, "bit3", 1],  [" filterUp: ",	 8, 1, "bit0", 1],
	      [" filterDown: ",		 8, 1, "bit1", 1],  [" cooling: ",	11, 1, "bit3", 1],
	      [" service: ",		10, 1, "bit2", 1]
	      ],
  "0clean"    => [["", 8, 2, "hex", 1]             
              ],
  "1clean"    => [["", 8, 4, "hex2int", 1]             		#fix for celle; handl neg values 20NoV2021
              ],
  "2opmode"   => [["", 8, 2, "opmode", 1]             
              ],
  "4temp"     => [["", 8, 4, "hex2int",2560]             
	      ],
  "5temp"     => [["", 8, 4, "hex2int",10]             
	      ],
  "6gradient" => [["", 8, 4, "hex", 100]             
              ],
  "7prog"     => [["", 8, 2, "quater", 1],	["--", 10, 2, "quater", 1]
              ],
  "8party"    => [["", 10, 2, "quater", 1],	["--", 8, 2, "quater", 1]
              ],
  "9holy"     => [["", 10, 2, "quater", 1]
              ]
);


########################################################################################
#
# %sets - all supported protocols are listed  59E
# 
########################################################################################

my %sets439technician =(
#   "zResetLast10errors"		=> {cmd2=>"D1",     argMin =>   "0",	argMax =>  "0",	type =>"0clean",  unit =>""},
   "zResetLast10errors"		=> {cmd2=>"D1",     argMin =>   "0",	argMax =>  "0",	type =>"D1last",  unit =>""},
#  "zPassiveCoolingtrigger"	=> {cmd2=>"0A0597", argMin =>   "0",	argMax =>  "50",	type =>"1clean",  unit =>""},
  "zPumpHC"			=> {cmd2=>"0A0052", argMin =>   "0",	argMax =>  "1",	type =>"0clean",  unit =>""},  
  "zPumpDHW"			=> {cmd2=>"0A0056", argMin =>   "0",	argMax =>  "1",	type =>"0clean",  unit =>""}
 );



my %sets439539common = (
  "pOpMode"			=> {cmd2=>"0A0112", type   =>  "2opmode"},  # 1 Standby bereitschaft; 11 in Automatic; 3 DAYmode; SetbackMode; DHWmode; Manual; Emergency 
  "p01RoomTempDayHC1"		=> {cmd2=>"0B0005", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p02RoomTempNightHC1"		=> {cmd2=>"0B0008", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p03RoomTempStandbyHC1"	=> {cmd2=>"0B013D", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p01RoomTempDayHC1SummerMode"	=> {cmd2=>"0B0569", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p02RoomTempNightHC1SummerMode"=> {cmd2=>"0B056B", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p03RoomTempStandbyHC1SummerMode"=> {cmd2=>"0B056A", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p13GradientHC1"		=> {cmd2=>"0B010E", argMin => "0.1",	argMax =>    "5",	type =>"6gradient", unit =>""}, # 0..5 rappresentato/100
  "p14LowEndHC1"		=> {cmd2=>"0B059E", argMin =>   "0",	argMax =>   "10",	type =>"5temp",  unit =>" K"},   #in °K 0..20°K rappresentato/10
  "p15RoomInfluenceHC1"		=> {cmd2=>"0B010F", argMin =>   "0",	argMax =>  "100",	type =>"0clean", unit =>" %"},
  "p19FlowProportionHC1"	=> {cmd2=>"0B059D", argMin =>   "0",	argMax =>  "100",	type =>"1clean", unit =>" %"}, #in % 0..100%
  "p01RoomTempDayHC2"		=> {cmd2=>"0C0005", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p02RoomTempNightHC2"		=> {cmd2=>"0C0008", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p03RoomTempStandbyHC2"	=> {cmd2=>"0C013D", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p01RoomTempDayHC2SummerMode"	=> {cmd2=>"0C0569", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p02RoomTempNightHC2SummerMode"=> {cmd2=>"0C056B", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p03RoomTempStandbyHC2SummerMode"=> {cmd2=>"0C056A", argMin =>  "12",	argMax =>   "32",	type =>"5temp",  unit =>" °C"},
  "p16GradientHC2"		=> {cmd2=>"0C010E", argMin => "0.1",	argMax =>    "5",	type =>"6gradient", unit =>""}, # /100
  "p17LowEndHC2"		=> {cmd2=>"0C059E", argMin =>   "0",	argMax =>   "10", type =>"5temp",  unit =>" K"},
  "p18RoomInfluenceHC2"		=> {cmd2=>"0C010F", argMin =>   "0",	argMax =>  "100",	type =>"0clean", unit =>" %"}, 
  "p04DHWsetDayTemp"		=> {cmd2=>"0A0013", argMin =>  "10",	argMax =>   "55",	type =>"5temp",  unit =>" °C"},
  "p05DHWsetNightTemp"		=> {cmd2=>"0A05BF", argMin =>  "10",	argMax =>   "55",	type =>"5temp",  unit =>" °C"},
  "p83DHWsetSolarTemp"		=> {cmd2=>"0A05BE", argMin =>  "10",	argMax =>   "75",	type =>"5temp",  unit =>" °C"},
  "p06DHWsetStandbyTemp"	=> {cmd2=>"0A0581", argMin =>  "10",	argMax =>   "55",	type =>"5temp",  unit =>" °C"},
  "p11DHWsetManualTemp"		=> {cmd2=>"0A0580", argMin =>  "10",	argMax =>   "55",	type =>"5temp",  unit =>" °C"},
  "p36DHWMaxTime"		=> {cmd2=>"0A0180", argMin =>   "6",	argMax =>   "12",	type =>"1clean", unit =>""},
  "p07FanStageDay"		=> {cmd2=>"0A056C", argMin =>   "0",	argMax =>    "3",	type =>"1clean", unit =>""},
  "p08FanStageNight"		=> {cmd2=>"0A056D", argMin =>   "0",	argMax =>    "3",	type =>"1clean", unit =>""},
  "p09FanStageStandby"		=> {cmd2=>"0A056F", argMin =>   "0",	argMax =>    "3",	type =>"1clean", unit =>""},
  "p99FanStageParty"		=> {cmd2=>"0A0570", argMin =>   "0",	argMax =>    "3",	type =>"1clean", unit =>""},
  "p21Hyst1"			=> {cmd2=>"0A05C0", argMin =>   "0",	argMax =>   "10",	type =>"5temp",  unit =>" K"},
  "p22Hyst2"			=> {cmd2=>"0A05C1", argMin =>   "0",	argMax =>   "10",	type =>"5temp",  unit =>" K"},
  "p23Hyst3"			=> {cmd2=>"0A05C2", argMin =>   "0",	argMax =>    "5",	type =>"5temp",  unit =>" K"},
  "p24Hyst4"			=> {cmd2=>"0A05C3", argMin =>   "0",	argMax =>    "5",	type =>"5temp",  unit =>" K"},
  "p25Hyst5"			=> {cmd2=>"0A05C4", argMin =>   "0",	argMax =>    "5",	type =>"5temp",  unit =>" K"},
  "p29HystAsymmetry"		=> {cmd2=>"0A05C5", argMin =>   "1",	argMax =>    "5",	type =>"1clean", unit =>""}, 
  "p30integralComponent"	=> {cmd2=>"0A0162", argMin =>  "10",	argMax =>  "999",	type =>"1clean", unit =>" Kmin"},
  "p31MaxBoosterStgHtg"		=> {cmd2=>"0A059F", argMin =>   "0",	argMax =>    "3",	type =>"1clean", unit =>""},
  "p32HystDHW"			=> {cmd2=>"0A0140", argMin =>   "0",	argMax =>   "10",	type =>"5temp",  unit =>" K"},
  "p33BoosterTimeoutDHW"	=> {cmd2=>"0A0588", argMin =>   "0",	argMax =>  "200",	type =>"1clean", unit =>" min"}, #during DHW heating
  "p79BoosterTimeoutHC"		=> {cmd2=>"0A05A0", argMin =>   "0",	argMax =>   "60",	type =>"1clean", unit =>" min"}, #delayed enabling of booster heater
  "p46UnschedVent0"		=> {cmd2=>"0A0571", argMin =>   "0",	argMax => "1000",	type =>"1clean", unit =>" min"},	 #in min
  "p45UnschedVent1"		=> {cmd2=>"0A0572", argMin =>   "0",	argMax => "1000",	type =>"1clean", unit =>" min"},	 #in min
  "p44UnschedVent2"		=> {cmd2=>"0A0573", argMin =>   "0",	argMax => "1000",	type =>"1clean", unit =>" min"},	 #in min
  "p43UnschedVent3"		=> {cmd2=>"0A0574", argMin =>   "0",	argMax => "1000",	type =>"1clean", unit =>" min"},	 #in min
  "p37Fanstage1AirflowInlet"	=> {cmd2=>"0A0576", argMin =>  "50",	argMax =>  "300",	type =>"1clean", unit =>" m3/h"},	#zuluft 
  "p38Fanstage2AirflowInlet"	=> {cmd2=>"0A0577", argMin =>  "50",	argMax =>  "300",	type =>"1clean", unit =>" m3/h"},	#zuluft 
  "p39Fanstage3AirflowInlet"	=> {cmd2=>"0A0578", argMin =>  "50",	argMax =>  "300",	type =>"1clean", unit =>" m3/h"},	#zuluft 
  "p40Fanstage1AirflowOutlet"	=> {cmd2=>"0A0579", argMin =>  "50",	argMax =>  "300",	type =>"1clean", unit =>" m3/h"},	#abluft extrated
  "p41Fanstage2AirflowOutlet"	=> {cmd2=>"0A057A", argMin =>  "50",	argMax =>  "300",	type =>"1clean", unit =>" m3/h"},	#abluft extrated
  "p42Fanstage3AirflowOutlet"	=> {cmd2=>"0A057B", argMin =>  "50",	argMax =>  "300",	type =>"1clean", unit =>" m3/h"},	#abluft extrated
  "p49SummerModeTemp"		=> {cmd2=>"0A0116", argMin =>  "10",	argMax =>   "24",	type =>"5temp",  unit =>" °C"},		#threshold for summer mode !! 
  "p50SummerModeHysteresis"	=> {cmd2=>"0A05A2", argMin => "0.5",	argMax =>    "5",	type =>"5temp",  unit =>" K"},		#Hysteresis for summer mode !! 
  "p78DualModePoint"		=> {cmd2=>"0A01AC", argMin => "-10",	argMax =>   "20",	type =>"5temp",  unit =>" °C"},
  "p54MinPumpCycles"		=> {cmd2=>"0A05B8", argMin =>   "1",	argMax =>   "24",	type =>"1clean", unit =>""},
  "p55MaxPumpCycles"		=> {cmd2=>"0A05B7", argMin =>  "25",	argMax =>  "288",	type =>"1clean", unit =>""},
  "p56OutTempMaxPumpCycles"	=> {cmd2=>"0A05B9", argMin =>   "0",	argMax =>   "20",	type =>"5temp",  unit =>" °C"},
  "p57OutTempMinPumpCycles"	=> {cmd2=>"0A05BA", argMin =>   "0",	argMax =>   "25",	type =>"5temp",  unit =>" °C"},
  "p58SuppressTempCaptPumpStart"=> {cmd2=>"0A0611", argMin =>   "0",	argMax =>  "120",	type =>"1clean", unit =>" s"},
  "p76RoomThermCorrection"	=> {cmd2=>"0A0109", argMin =>  "-5",	argMax =>    "5",	type =>"4temp",  unit =>" K"},
  "p77OutThermFilterTime"	=> {cmd2=>"0A010C", argMin =>   "1",	argMax =>   "24",	type =>"0clean", unit =>" h"},
  "p35PasteurisationInterval"	=> {cmd2=>"0A0586", argMin =>   "1",	argMax =>   "30",	type =>"1clean", unit =>""},
  "p80EnableSolar"		=> {cmd2=>"0A03C1", argMin =>   "0",	argMax =>    "1",	type =>"1clean", unit =>""},
  "p35PasteurisationTemp"	=> {cmd2=>"0A0587", argMin =>  "10",	argMax =>   "65",	type =>"5temp",  unit =>" °C"},
  "p34BoosterDHWTempAct"	=> {cmd2=>"0A0589", argMin => "-10",	argMax =>   "10",	type =>"5temp",  unit =>" °C"},
  "p99DHWmaxFlowTemp"		=> {cmd2=>"0A058C", argMin =>  "10",	argMax =>   "75",	type =>"5temp",  unit =>" °C"},
  "p99HC1maxFlowTemp"		=> {cmd2=>"0A0027", argMin =>  "10",	argMax =>   "75",	type =>"5temp",  unit =>" °C"},
  "p89DHWeco"			=> {cmd2=>"0A058D", argMin =>   "0",	argMax =>    "1",	type =>"1clean", unit =>""},
  "p99startUnschedVent"		=> {cmd2=>"0A05DD", argMin =>   "0",	argMax =>    "3",	type =>"1clean", unit =>""},
  "p99FrostProtectionBoost"	=> {cmd2=>"0A05B3", argMin =>  "10",	argMax =>   "30",	type =>"5temp",  unit =>" °C"}, #added by TheTrumpeter __EINFRIERSCHUTZ NE
  "p99FrostProtectionCancel"	=> {cmd2=>"0A05B4", argMin =>   "0",	argMax =>   "20",	type =>"5temp",  unit =>" °C"}, #added by TheTrumpeter __ABTAUABBR.
  "pClockDay"			=> {cmd2=>"0A0122", argMin =>   "1",	argMax =>   "31",	type =>"0clean", unit =>""},
  "pClockMonth"			=> {cmd2=>"0A0123", argMin =>   "1",	argMax =>   "12",	type =>"0clean", unit =>""},
  "pClockYear"			=> {cmd2=>"0A0124", argMin =>  "12",	argMax =>   "20",	type =>"0clean", unit =>""},
  "pClockHour"			=> {cmd2=>"0A0125", argMin =>   "0",	argMax =>   "23",	type =>"0clean", unit =>""},
  "pClockMinutes"		=> {cmd2=>"0A0126", argMin =>   "0",	argMax =>   "59",	type =>"0clean", unit =>""},
  "pHolidayBeginDay"		=> {cmd2=>"0A011B", argMin =>   "1",	argMax =>   "31",	type =>"0clean", unit =>""},
  "pHolidayBeginMonth"		=> {cmd2=>"0A011C", argMin =>   "1",	argMax =>   "12",	type =>"0clean", unit =>""},
  "pHolidayBeginYear"		=> {cmd2=>"0A011D", argMin =>  "12",	argMax =>   "30",	type =>"0clean", unit =>""},
  "pHolidayBeginTime"		=> {cmd2=>"0A05D3", argMin =>  "00:00",	argMax =>"23:59",	type =>"9holy",  unit =>""},
  "pHolidayEndDay"		=> {cmd2=>"0A011E", argMin =>   "1",	argMax =>   "31",	type =>"0clean", unit =>""},
  "pHolidayEndMonth"		=> {cmd2=>"0A011F", argMin =>   "1",	argMax =>   "12",	type =>"0clean", unit =>""},
  "pHolidayEndYear"		=> {cmd2=>"0A0120", argMin =>  "12",	argMax =>   "30",	type =>"0clean", unit =>""},
  "pHolidayEndTime"		=> {cmd2=>"0A05D4", argMin =>  "00:00",	argMax =>"23:59",	type =>"9holy",  unit =>""}, # the answer look like  0A05D4-0D0A05D40029 for year 41 which is 10:15
  #"party-time"			=> {cmd2=>"0A05D1", argMin =>  "00:00",	argMax =>"23:59",	type =>"8party", unit =>""}, # value 1Ch 28dec is 7 ; value 1Eh 30dec is 7:30
  "programHC1_Mo_0"		=> {cmd2=>"0B1410", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},  #1 is monday 0 is first prog; start and end; value 1Ch 28dec is 7 ; value 1Eh 30dec is 7:30
  "programHC1_Mo_1"		=> {cmd2=>"0B1411", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo_2"		=> {cmd2=>"0B1412", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Tu_0"		=> {cmd2=>"0B1420", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Tu_1"		=> {cmd2=>"0B1421", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Tu_2"		=> {cmd2=>"0B1422", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_We_0"		=> {cmd2=>"0B1430", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_We_1"		=> {cmd2=>"0B1431", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_We_2"		=> {cmd2=>"0B1432", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Th_0"		=> {cmd2=>"0B1440", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Th_1"		=> {cmd2=>"0B1441", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Th_2"		=> {cmd2=>"0B1442", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Fr_0"		=> {cmd2=>"0B1450", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Fr_1"		=> {cmd2=>"0B1451", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Fr_2"		=> {cmd2=>"0B1452", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Sa_0"		=> {cmd2=>"0B1460", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Sa_1"		=> {cmd2=>"0B1461", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Sa_2"		=> {cmd2=>"0B1462", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_So_0"		=> {cmd2=>"0B1470", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_So_1"		=> {cmd2=>"0B1471", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_So_2"		=> {cmd2=>"0B1472", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo-Fr_0"		=> {cmd2=>"0B1480", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo-Fr_1"		=> {cmd2=>"0B1481", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo-Fr_2"		=> {cmd2=>"0B1482", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Sa-So_0"		=> {cmd2=>"0B1490", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Sa-So_1"		=> {cmd2=>"0B1491", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Sa-So_2"		=> {cmd2=>"0B1492", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo-So_0"		=> {cmd2=>"0B14A0", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo-So_1"		=> {cmd2=>"0B14A1", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC1_Mo-So_2"		=> {cmd2=>"0B14A2", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo_0"		=> {cmd2=>"0C1510", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},  #1 is monday 0 is first prog; start and end; value 1Ch 28dec is 7 ; value 1Eh 30dec is 7:30
  "programHC2_Mo_1"		=> {cmd2=>"0C1511", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo_2"		=> {cmd2=>"0C1512", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Tu_0"		=> {cmd2=>"0C1520", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Tu_1"		=> {cmd2=>"0C1521", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Tu_2"		=> {cmd2=>"0C1522", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_We_0"		=> {cmd2=>"0C1530", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_We_1"		=> {cmd2=>"0C1531", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_We_2"		=> {cmd2=>"0C1532", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Th_0"		=> {cmd2=>"0C1540", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Th_1"		=> {cmd2=>"0C1541", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Th_2"		=> {cmd2=>"0C1542", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Fr_0"		=> {cmd2=>"0C1550", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Fr_1"		=> {cmd2=>"0C1551", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Fr_2"		=> {cmd2=>"0C1552", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Sa_0"		=> {cmd2=>"0C1560", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Sa_1"		=> {cmd2=>"0C1561", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Sa_2"		=> {cmd2=>"0C1562", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_So_0"		=> {cmd2=>"0C1570", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_So_1"		=> {cmd2=>"0C1571", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_So_2"		=> {cmd2=>"0C1572", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo-Fr_0"		=> {cmd2=>"0C1580", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo-Fr_1"		=> {cmd2=>"0C1581", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo-Fr_2"		=> {cmd2=>"0C1582", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Sa-So_0"		=> {cmd2=>"0C1590", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Sa-So_1"		=> {cmd2=>"0C1591", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Sa-So_2"		=> {cmd2=>"0C1592", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo-So_0"		=> {cmd2=>"0C15A0", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo-So_1"		=> {cmd2=>"0C15A1", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programHC2_Mo-So_2"		=> {cmd2=>"0C15A2", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo_0"		=> {cmd2=>"0A1710", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo_1"		=> {cmd2=>"0A1711", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo_2"		=> {cmd2=>"0A1712", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Tu_0"		=> {cmd2=>"0A1720", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Tu_1"		=> {cmd2=>"0A1721", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Tu_2"		=> {cmd2=>"0A1722", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_We_0"		=> {cmd2=>"0A1730", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_We_1"		=> {cmd2=>"0A1731", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_We_2"		=> {cmd2=>"0A1732", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Th_0"		=> {cmd2=>"0A1740", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Th_1"		=> {cmd2=>"0A1741", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Th_2"		=> {cmd2=>"0A1742", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Fr_0"		=> {cmd2=>"0A1750", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Fr_1"		=> {cmd2=>"0A1751", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Fr_2"		=> {cmd2=>"0A1752", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Sa_0"		=> {cmd2=>"0A1760", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Sa_1"		=> {cmd2=>"0A1761", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Sa_2"		=> {cmd2=>"0A1762", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_So_0"		=> {cmd2=>"0A1770", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_So_1"		=> {cmd2=>"0A1771", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_So_2"		=> {cmd2=>"0A1772", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo-Fr_0"		=> {cmd2=>"0A1780", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo-Fr_1"		=> {cmd2=>"0A1781", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo-Fr_2"		=> {cmd2=>"0A1782", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Sa-So_0"		=> {cmd2=>"0A1790", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Sa-So_1"		=> {cmd2=>"0A1791", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Sa-So_2"		=> {cmd2=>"0A1792", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo-So_0"		=> {cmd2=>"0A17A0", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo-So_1"		=> {cmd2=>"0A17A1", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programDHW_Mo-So_2"		=> {cmd2=>"0A17A2", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo_0"		=> {cmd2=>"0A1D10", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo_1"		=> {cmd2=>"0A1D11", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo_2"		=> {cmd2=>"0A1D12", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Tu_0"		=> {cmd2=>"0A1D20", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Tu_1"		=> {cmd2=>"0A1D21", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Tu_2"		=> {cmd2=>"0A1D22", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_We_0"		=> {cmd2=>"0A1D30", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_We_1"		=> {cmd2=>"0A1D31", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_We_2"		=> {cmd2=>"0A1D32", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Th_0"		=> {cmd2=>"0A1D40", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Th_1"		=> {cmd2=>"0A1D41", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Th_2"		=> {cmd2=>"0A1D42", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Fr_0"		=> {cmd2=>"0A1D50", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Fr_1"		=> {cmd2=>"0A1D51", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Fr_2"		=> {cmd2=>"0A1D52", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Sa_0"		=> {cmd2=>"0A1D60", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Sa_1"		=> {cmd2=>"0A1D61", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Sa_2"		=> {cmd2=>"0A1D62", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_So_0"		=> {cmd2=>"0A1D70", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_So_1"		=> {cmd2=>"0A1D71", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_So_2"		=> {cmd2=>"0A1D72", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo-Fr_0"		=> {cmd2=>"0A1D80", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo-Fr_1"		=> {cmd2=>"0A1D81", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo-Fr_2"		=> {cmd2=>"0A1D82", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Sa-So_0"		=> {cmd2=>"0A1D90", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Sa-So_1"		=> {cmd2=>"0A1D91", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Sa-So_2"		=> {cmd2=>"0A1D92", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo-So_0"		=> {cmd2=>"0A1DA0", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo-So_1"		=> {cmd2=>"0A1DA1", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "programFan_Mo-So_2"		=> {cmd2=>"0A1DA2", argMin =>  "00:00",	argMax =>"24:00",	type =>"7prog",  unit =>""},
  "pOvenFireplace"		=> {cmd2=>"0A057C", argMin =>      "0",	argMax =>    "4",	type =>"1clean", unit =>""}, #Ofen / Kamin (0=Aus … 4= oeffner - ueberwachung)
    #OFF/    N-O_CONTACT_OFF/       N-C_CONTACT_OFF/     N-O_MONITORING/    N-C_MONITORING
    #LLWT = LuftLuftWärmeTauscher - AirAirHeatExchanger
  "p85MaxDefrostDur"		=> {cmd2=>"0A057D", argMin =>  "60",	argMax =>  "250",	type =>"1clean", unit =>" min"},
  "p85DefrStartThreshold"	=> {cmd2=>"0A057E", argMin =>   "0",	argMax =>   "50",	type =>"1clean", unit =>" %"},  #LLWT_Abtaubeginnschwelle (%) -
  "p85FilterSpeed"		=> {cmd2=>"0A057F", argMin =>   "0",	argMax =>  "100",	type =>"1clean", unit =>" %"},   #LLWT_DrehzahlFilter (%) - increase in VentSpeed to indicate dirt / replacement needed 
  "p86OutTempCorrection"	=> {cmd2=>"0A05A3", argMin =>   "-20",	argMax =>  "30",	type =>"5temp",  unit =>" K"}   
  );


  
my %sets439only =(
  "p75passiveCooling"		=> {cmd2=>"0A0575", argMin =>   "0",	argMax =>   "2",	type =>"1clean",  unit =>""}   
 );
  
my %sets539only =(
  "p75passiveCooling"		=> {cmd2=>"0A0575", argMin =>   "0",	argMax =>  "4",		type =>"1clean", unit =>""},    
  "p99PumpRateHC"		=> {cmd2=>"0A02CB", argMin =>   "0",	argMax =>  "100",	type =>"5temp",  unit =>" %"},  
  "p99PumpRateDHW"		=> {cmd2=>"0A02CC", argMin =>   "0",	argMax =>  "100",	type =>"5temp",  unit =>" %"} ,
  "p99CoolingHC1Switch"		=> {cmd2=>"0B0287", argMin =>   "0",	argMax =>  "1",		type =>"1clean", unit =>""},
  "p99CoolingHC1SetTemp"	=> {cmd2=>"0B0582", argMin =>  "12",	argMax =>  "27",	type =>"5temp",  unit =>" °C"},    #suggested by TheTrumpeter
  "p99CoolingHC1HysterFlowTemp"	=> {cmd2=>"0B0583", argMin =>  "0.5",	argMax =>  "5",		type =>"5temp",  unit =>" K"}, #suggested by TheTrumpeter
  "p99CoolingHC1HysterRoomTemp"	=> {cmd2=>"0B0584", argMin =>  "0.5",	argMax =>  "3",		type =>"5temp",  unit =>" K"}  #suggested by TheTrumpeter
);
  

my %sets206 = (
  "p01RoomTempDay"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "30",	type =>"pclean", unit =>" °C"},
  "p02RoomTempNight"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "30",	type =>"pclean", unit =>" °C"},
  "p03RoomTempStandby"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "30",	type =>"pclean", unit =>" °C"},
  "p04DHWsetTempDay"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "55",	type =>"pclean", unit =>" °C"},
  "p05DHWsetTempNight"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "55",	type =>"pclean", unit =>" °C"},
  "p06DHWsetTempStandby"	=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "55",	type =>"pclean", unit =>" °C"},
  "p07FanStageDay"		=> {parent=>"p01-p12",	argMin => "0",	argMax =>    "3",	type =>"pclean", unit =>""},
  "p08FanStageNight"		=> {parent=>"p01-p12",	argMin => "0",	argMax =>    "3",	type =>"pclean", unit =>""},
  "p09FanStageStandby"		=> {parent=>"p01-p12",	argMin => "0",	argMax =>    "3",	type =>"pclean", unit =>""},
  "p10HCTempManual"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "65",	type =>"pclean", unit =>" °C"},
  "p11DHWsetTempManual"		=> {parent=>"p01-p12",	argMin => "10",	argMax =>   "65",	type =>"pclean", unit =>" °C"},
  "p12FanStageManual"      	=> {parent=>"p01-p12",	argMin => "0",	argMax =>    "3",	type =>"pclean", unit =>""},
  "p13GradientHC1"		=> {parent=>"pHeat1", 	argMin => "0",	argMax =>    "5",	type =>"pclean", unit =>""},
  "p14LowEndHC1"		=> {parent=>"pHeat1",	argMin => "0",	argMax =>   "20",	type =>"pclean", unit =>" K"},
  "p15RoomInfluenceHC1"		=> {parent=>"pHeat1",	argMin => "0",	argMax =>   "10",	type =>"pclean", unit =>""},
  "p16GradientHC2"		=> {parent=>"pHeat1",	argMin => "0",	argMax =>    "5",	type =>"pclean", unit =>""},
  "p17LowEndHC2"	    	=> {parent=>"pHeat1",	argMin => "0",	argMax =>   "10",	type =>"pclean", unit =>" K"},
  "p18RoomInfluenceHC2"		=> {parent=>"pHeat1",	argMin => "0",	argMax =>   "10",	type =>"pclean", unit =>""},
  "p19FlowProportionHC1"	=> {parent=>"pHeat1",	argMin => "0",	argMax =>  "100",	type =>"pclean", unit =>" %"},
  "p20FlowProportionHC2"	=> {parent=>"pHeat1",	argMin => "0",	argMax =>  "100",	type =>"pclean", unit =>" %"},
  "p21Hyst1"			=> {parent=>"pHeat2",	argMin => "0",	argMax =>   "10",	type =>"pclean", unit =>" K"},
  "p22Hyst2"			=> {parent=>"pHeat2",	argMin => "0",	argMax =>   "10",	type =>"pclean", unit =>" K"},
  "p23Hyst3"			=> {parent=>"pHeat2",	argMin => "0",	argMax =>    "5",	type =>"pclean", unit =>" K"},
  "p24Hyst4"			=> {parent=>"pHeat2",	argMin => "0",	argMax =>    "5",	type =>"pclean", unit =>" K"},
  "p25Hyst5"			=> {parent=>"pHeat2",	argMin => "0",	argMax =>    "5",	type =>"pclean", unit =>" K"},
  "p29HystAsymmetry"		=> {parent=>"pHeat2",	argMin => "1",	argMax =>    "5",	type =>"pclean", unit =>""},
  "p30integralComponent"	=> {parent=>"pHeat2",	argMin => "10",	argMax =>  "999",	type =>"pclean", unit =>" Kmin"},
  "p32HystDHW"			=> {parent=>"pDHW",	argMin => "2",	argMax =>   "10",	type =>"pclean", unit =>" K"},
  "p33BoosterTimeoutDHW"	=> {parent=>"pDHW",	argMin => "0",	argMax =>  "240",	type =>"pclean", unit =>" min"},
  "p34TempLimitBoostDHW"    	=> {parent=>"pDHW",	argMin => "-10",argMax =>   "10",	type =>"pclean", unit =>" °C"},
  "p35PasteurisationInterval"  	=> {parent=>"pDHW",	argMin => "3",	argMax =>   "30",	type =>"pclean", unit =>" Days"},
  "p36MaxDurationDHWLoad"    	=> {parent=>"pDHW",	argMin => "6",	argMax =>   "12",	type =>"pclean", unit =>" h"},
  "p37Fanstage1AirflowInlet"	=> {parent=>"pFan",	argMin => "60",	argMax =>  "250",	type =>"pclean", unit =>" m3/h"},
  "p38Fanstage2AirflowInlet"	=> {parent=>"pFan",	argMin => "60",	argMax =>  "250",	type =>"pclean", unit =>" m3/h"},
  "p39Fanstage3AirflowInlet"	=> {parent=>"pFan",	argMin => "60",	argMax =>  "250",	type =>"pclean", unit =>" m3/h"},
  "p40Fanstage1AirflowOutlet"	=> {parent=>"pFan",	argMin => "60",	argMax =>  "250",	type =>"pclean", unit =>" m3/h"},
  "p41Fanstage2AirflowOutlet"	=> {parent=>"pFan",	argMin => "60",	argMax =>  "250",	type =>"pclean", unit =>" m3/h"},
  "p42Fanstage3AirflowOutlet"	=> {parent=>"pFan",	argMin => "60",	argMax =>  "250",	type =>"pclean", unit =>" m3/h"},
  "p43UnschedVent3"		=> {parent=>"pFan",	argMin => "0",	argMax => "1000",	type =>"pclean", unit =>" min"},
  "p44UnschedVent2"		=> {parent=>"pFan",	argMin => "0",	argMax => "1000",	type =>"pclean", unit =>" min"},
  "p45UnschedVent1"		=> {parent=>"pFan",	argMin => "0",	argMax => "1000",	type =>"pclean", unit =>" min"},
  "p46UnschedVent0"		=> {parent=>"pFan",	argMin => "0",	argMax => "1000",	type =>"pclean", unit =>" min"},
  "p47CompressorRestartDelay"	=> {parent=>"pDefrostEva",argMin => "0",argMax =>   "20",	type =>"pclean", unit =>" min"},
  "p48MainFanSpeed"		=> {parent=>"pDefrostEva",argMin => "0",argMax =>  "100",	type =>"pclean", unit =>" %"},
  "p49SummerModeTemp"		=> {parent=>"pHeat2",	argMin => "10",	argMax =>   "24",	type =>"pclean", unit =>" °C"},
  "p50SummerModeHysteresis"	=> {parent=>"pHeat2",	argMin => "1",	argMax =>    "5",	type =>"pclean", unit =>" K"},
  "p54MinPumpCycles"		=> {parent=>"pCircPump",argMin => "1",	argMax =>   "24",	type =>"pclean", unit =>" /Day"},
  "p55MaxPumpCycles"		=> {parent=>"pCircPump",argMin => "25",	argMax =>  "288",	type =>"pclean", unit =>" /Day"},
  "p56OutTempMaxPumpCycles"	=> {parent=>"pCircPump",argMin => "0",	argMax =>   "20",	type =>"pclean", unit =>" °C"},
  "p57OutTempMinPumpCycles"	=> {parent=>"pCircPump",argMin => "0",	argMax =>   "25",	type =>"pclean", unit =>" °C"},
  "p58SuppressTempCaptPumpStart"=> {parent=>"pCircPump",argMin => "0",	argMax =>  "120",	type =>"pclean", unit =>" s"},
  "p75PassiveCooling"		=> {parent=>"pFan", 	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "p77OutTempFilterTime"	=> {parent=>"pHeat2", 	argMin => "0",	argMax =>   "24",	type =>"pclean", unit =>" h"},
  "p78DualModePoint"		=> {parent=>"pHeat2",	argMin => "-10",argMax =>   "20",	type =>"pclean", unit =>" °C"},
  "p79BoosterTimeoutHC"		=> {parent=>"pHeat2",	argMin => "0",	argMax =>   "60",	type =>"pclean", unit =>" min"},
  "p80EnableSolar"		=> {parent=>"pSolar",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "pClockDay"			=> {parent=>"sTimedate",argMin => "1",	argMax =>   "31",	type =>"pclean", unit =>""},
  "pClockMonth"			=> {parent=>"sTimedate",argMin => "1",	argMax =>   "12",	type =>"pclean", unit =>""},
  "pClockYear"			=> {parent=>"sTimedate",argMin => "12",	argMax =>   "20",	type =>"pclean", unit =>""},
  "pClockHour"			=> {parent=>"sTimedate",argMin => "0",	argMax =>   "23",	type =>"pclean", unit =>""},
  "pClockMinutes"		=> {parent=>"sTimedate",argMin => "0",	argMax =>   "59",	type =>"pclean", unit =>""},
  "progDHWStartTime"		=> {parent=>"pDHWProg",	argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progDHWEndTime"		=> {parent=>"pDHWProg",	argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progDHWEnable"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWMonday"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWTuesday"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWWednesday"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWThursday"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWFriday"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWSaturday"		=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progDHWSunday"	 	=> {parent=>"pDHWProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1StartTime"           	=> {parent=>"pHeatProg",argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""}, 
  "progHC1EndTime"		=> {parent=>"pHeatProg",argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progHC1Enable"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Monday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Tuesday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Wednesday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Thursday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Friday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Saturday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC1Sunday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2StartTime"		=> {parent=>"pHeatProg",argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""}, 
  "progHC2EndTime"		=> {parent=>"pHeatProg",argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progHC2Enable"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Monday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Tuesday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Wednesday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Thursday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Friday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Saturday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progHC2Sunday"		=> {parent=>"pHeatProg",argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1StartTime"		=> {parent=>"pFanProg",	argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progFAN1EndTime"		=> {parent=>"pFanProg",	argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progFAN1Enable"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Monday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Tuesday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Wednesday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Thursday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Friday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Saturday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN1Sunday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2StartTime"		=> {parent=>"pFanProg",	argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progFAN2EndTime"		=> {parent=>"pFanProg",	argMin =>"00:00",argMax =>"23:59",	type =>"ptime",  unit =>""},
  "progFAN2Enable"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Monday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Tuesday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Wednesday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Thursday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Friday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Saturday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""},
  "progFAN2Sunday"		=> {parent=>"pFanProg",	argMin => "0",	argMax =>    "1",	type =>"pclean", unit =>""} 
 );

my %setsonly214 = (
  "ResetErrors"			=> {cmd2=>"F8",		argMin => "0",	 argMax =>   "0",	type =>"0clean", unit =>""}
);


########################################################################################
#
# %gets - all supported protocols are listed without header and footer
#
########################################################################################

my %getsonly439 = (
#"debug_read_raw_register_slow"	=> { },
  "sSol"		=> {cmd2=>"16",     type =>"16sol",  unit =>""},
  "sHistory"		=> {cmd2=>"09",     type =>"09his",  unit =>""},
  "sLast10errors"	=> {cmd2=>"D1",     type =>"D1last", unit =>""},
  "sFan"  		=> {cmd2=>"E8",     type =>"E8fan",  unit =>""},
  "sDHW"		=> {cmd2=>"F3",     type =>"F3dhw",  unit =>""},
  "sHC1"		=> {cmd2=>"F4",     type =>"F4hc1",  unit =>""},
  "sHC2"		=> {cmd2=>"F5",     type =>"F5hc2",  unit =>""},
  "sControl"		=> {cmd2=>"F2",     type =>"F2ctrl", unit =>""},
  "sGlobal"		=> {cmd2=>"FB",     type =>"FBglob", unit =>""},  #allFB
  "sTimedate"		=> {cmd2=>"FC",     type =>"FCtime", unit =>""},
  "sFirmware"		=> {cmd2=>"FD",     type =>"FDfirm", unit =>""},
  "sFirmware-Id" 	=> {cmd2=>"FE",     type =>"FEfirmId", unit =>""},
  "sDisplay"		=> {cmd2=>"0A0176", type =>"0A0176Dis", unit =>""},
  "sBoostDHWTotal"	=> {cmd2=>"0A0924", cmd3=>"0A0925",	type =>"1clean", unit =>" kWh"},
  "sBoostHCTotal"	=> {cmd2=>"0A0928", cmd3=>"0A0929",	type =>"1clean", unit =>" kWh"},
  "sHeatRecoveredDay"	=> {cmd2=>"0A03AE", cmd3=>"0A03AF",	type =>"1clean", unit =>" Wh"},
  "sHeatRecoveredTotal"	=> {cmd2=>"0A03B0", cmd3=>"0A03B1",	type =>"1clean", unit =>" kWh"},
  "sHeatDHWDay"		=> {cmd2=>"0A092A", cmd3=>"0A092B",	type =>"1clean", unit =>" Wh"},
  "sHeatDHWTotal"	=> {cmd2=>"0A092C", cmd3=>"0A092D",	type =>"1clean", unit =>" kWh"},
  "sHeatHCDay"		=> {cmd2=>"0A092E", cmd3=>"0A092F",	type =>"1clean", unit =>" Wh"},
  "sHeatHCTotal" 	=> {cmd2=>"0A0930", cmd3=>"0A0931",	type =>"1clean", unit =>" kWh"},
  "sElectrDHWDay"	=> {cmd2=>"0A091A", cmd3=>"0A091B",	type =>"1clean", unit =>" Wh"},
  "sElectrDHWTotal" 	=> {cmd2=>"0A091C", cmd3=>"0A091D",	type =>"1clean", unit =>" kWh"},
  "sElectrHCDay" 	=> {cmd2=>"0A091E", cmd3=>"0A091F",	type =>"1clean", unit =>" Wh"},
  "sElectrHCTotal"	=> {cmd2=>"0A0920", cmd3=>"0A0921",	type =>"1clean", unit =>" kWh"},
  "party-time"		=> {cmd2=>"0A05D1", argMin =>"00:00", argMax =>"23:59", type =>"8party", unit =>""} # value 1Ch 28dec is 7 ; value 1Eh 30dec is 7:30
  );


my %getsonly539 = (  #info from belu and godmorgon
  "sFlowRate"		=> {cmd2=>"0A033B", type =>"1clean", unit =>" cl/min"},
  "sHumMaskingTime"	=> {cmd2=>"0A064F", type =>"1clean", unit =>" min"},
  "sHumThreshold"	=> {cmd2=>"0A0650", type =>"1clean", unit =>" %"},
  "sHeatingRelPower"	=> {cmd2=>"0A069A", type =>"1clean", unit =>" %"},
  "sComprRelPower"	=> {cmd2=>"0A069B", type =>"1clean", unit =>" %"},
  "sComprRotUnlimit"	=> {cmd2=>"0A069C", type =>"1clean", unit =>" Hz"},
  "sComprRotLimit"	=> {cmd2=>"0A069D", type =>"1clean", unit =>" Hz"},
  "sOutputReduction"	=> {cmd2=>"0A06A4", type =>"1clean", unit =>" %"},
  "sOutputIncrease"	=> {cmd2=>"0A06A5", type =>"1clean", unit =>" %"},
  "sHumProtection"	=> {cmd2=>"0A09D1", type =>"1clean", unit =>""},
  "sSetHumidityMin"	=> {cmd2=>"0A09D2", type =>"1clean", unit =>" %"},
  "sSetHumidityMax"	=> {cmd2=>"0A09D3", type =>"1clean", unit =>" %"},
  "sCoolHCTotal"	=> {cmd2=>"0A0648", cmd3 =>"0A0649", type =>"1clean", unit =>" kWh"},
  "sDewPointHC1"	=> {cmd2=>"0B0264", type =>"5temp",  unit =>" °C"}
 );
%getsonly539=(%getsonly539, %getsonly439);

my %getsonly2xx = (
  "pDefrostEva"		=> {cmd2=>"03", type =>"03pxx206", unit =>""},
  "pDefrostAA"		=> {cmd2=>"04", type =>"04pxx206", unit =>""},
  "pHeat1"		=> {cmd2=>"05", type =>"05pxx206", unit =>""},
  "pHeat2"		=> {cmd2=>"06", type =>"06pxx206", unit =>""},
  "pDHW"		=> {cmd2=>"07", type =>"07pxx206", unit =>""},
  "pSolar"		=> {cmd2=>"08", type =>"08pxx206", unit =>""},
  "sHistory"		=> {cmd2=>"09", type =>"09his206", unit =>""},
  "pCircPump"		=> {cmd2=>"0A", type =>"0Apxx206", unit =>""},
  "pHeatProg"		=> {cmd2=>"0B", type =>"0Bpxx206", unit =>""},
  "pDHWProg"		=> {cmd2=>"0C", type =>"0Cpxx206", unit =>""},
  "pFanProg"   		=> {cmd2=>"0D", type =>"0Dpxx206", unit =>""},
  "pRestart"		=> {cmd2=>"0E", type =>"0Epxx206", unit =>""},
  "pAbsence"		=> {cmd2=>"0F", type =>"0Fpxx206", unit =>""},
  "pDryHeat"		=> {cmd2=>"10", type =>"10pxx206", unit =>""},
  "sSol"		=> {cmd2=>"16", type =>"16sol",    unit =>""},
  "p01-p12"		=> {cmd2=>"17", type =>"17pxx206", unit =>""},
  "sProgram"  		=> {cmd2=>"EE", type =>"EEprg206", unit =>""},
  "sFan"  		=> {cmd2=>"E8", type =>"E8fan206", unit =>""},
  "sControl"  		=> {cmd2=>"F2", type =>"F2ctrl",   unit =>""},
  "sDHW"		=> {cmd2=>"F3", type =>"F3dhw",    unit =>""},
  "sHC2"		=> {cmd2=>"F5", type =>"F5hc2",    unit =>""},
  "sSystem"		=> {cmd2=>"F6", type =>"F6sys206", unit =>""},
  "sTimedate" 		=> {cmd2=>"FC", type =>"FCtime206",unit =>""},
  "inputVentilatorSpeed"=> {parent=>"sGlobal",		unit =>" %"},
  "outputVentilatorSpeed"=> {parent=>"sGlobal",		unit =>" %"},
  "mainVentilatorSpeed"	=> {parent=>"sGlobal",		unit =>" %"},
  "inputVentilatorPower"=> {parent=>"sGlobal",		unit =>" %"},
  "outputVentilatorPower"=> {parent=>"sGlobal",		unit =>" %"},
  "mainVentilatorPower"	=> {parent=>"sGlobal",		unit =>" %"}, 
 );
my %getsonly206 = (
  "sHC1"		=> {cmd2=>"F4", type =>"F4hc1",    unit =>""},
  "pFan"		=> {cmd2=>"01", type =>"01pxx206", unit =>""},
  "sLast10errors"	=> {cmd2=>"D1", type =>"D1last206",unit =>""},
  "sFirmware"		=> {cmd2=>"FD", type =>"FDfirm",   unit =>""},
  "sGlobal"		=> {cmd2=>"FB", type =>"FBglob206",unit =>""}
 );

my %getsonly214 = (
  "pFan"		=> {cmd2=>"01", type =>"01pxx214", unit =>""},
  "pExpert"		=> {cmd2=>"02", type =>"02pxx206", unit =>""},
  "sControl"  		=> {cmd2=>"F2", type =>"F2type",   unit =>""},
  "sHC1"		=> {cmd2=>"F4", type =>"F4hc1214", unit =>""},
  #"sLVR"  		=> {cmd2=>"E8", type =>"E8tyype",  unit =>""},
  #"sF0"  		=> {cmd2=>"F0", type =>"F0type",   unit =>""},
  #"sF1"  		=> {cmd2=>"F1", type =>"F1type",   unit =>""},
  #"sEF"  		=> {cmd2=>"EF", type =>"EFtype",   unit =>""},
  "sGlobal"	     	=> {cmd2=>"FB", type =>"FBglob214",unit =>""}  
 ); 

 my %getsonly214j = (  # contribution from joerg  hellijo  Antwort #1085  Juni 2022
  "pFan"		=> {cmd2=>"01", type =>"01pxx214", unit =>""},
  "pExpert"		=> {cmd2=>"02", type =>"02pxx206", unit =>""},
  "sControl"  		=> {cmd2=>"F2", type =>"F2type",   unit =>""},
  "sHC1"		=> {cmd2=>"F4", type =>"F4hc1214j", unit =>""},
  #"sLVR"  		=> {cmd2=>"E8", type =>"E8tyype",  unit =>""},
  #"sF0"  		=> {cmd2=>"F0", type =>"F0type",   unit =>""},
  #"sF1"  		=> {cmd2=>"F1", type =>"F1type",   unit =>""},
  #"sEF"  		=> {cmd2=>"EF", type =>"EFtype",   unit =>""},
  "sGlobal"	     	=> {cmd2=>"FB", type =>"FBglob214",unit =>""}  
 );

########################################################################################
# FHEM until here, conversion script follows
########################################################################################

sub convertRecords {
	my ($channelTypesPerGroup, $channelTypes, $name, $version, $copyFromVersion) = @_;
	my %sets;
	my %gets;

	if (!defined $copyFromVersion) {
		$copyFromVersion = $version;
	}

	if ($copyFromVersion eq "2.06") {
		%sets = %sets206;
		%gets = (%getsonly2xx, %getsonly206, %sets);
	}
	elsif ($copyFromVersion eq "2.14") {
		%sets = (%sets206, %setsonly214);
		%gets = (%getsonly2xx, %getsonly214, %sets206);
	}
	elsif ($copyFromVersion eq "2.14j") {
		%sets = (%sets206, %setsonly214);
		%gets = (%getsonly2xx, %getsonly214j, %sets206);
	}
	elsif ($copyFromVersion eq "5.39") {
		%sets=(%sets439539common, %sets539only);
		%gets=(%getsonly539, %sets);
	}
	elsif ($copyFromVersion eq "4.39technician") {
		%sets=(%sets439539common, %sets439only, %sets439technician);
		%gets=(%getsonly439, %sets);
	}
	else { #in all other cases I assume $copyFromVersion eq "4.39" cambiato nella v0140
		%sets=(%sets439539common, %sets439only);
		%gets=(%getsonly439, %sets);
	}

	my $RECORDS_FILE = "src/main/resources/HeatpumpConfig/${name}_" . uc($version =~ s/\./_/r) . ".xml";
	print "Saving to $RECORDS_FILE\n";
	open(RECORDS, ">", $RECORDS_FILE) or die $!;
	print RECORDS qq(<?xml version="1.0" ?>\n<records>\n);

	my @DERIVED_NAME = ("Month", "Day");
	my $derivedNameIndex;

	# First pass for 2.06 where the definitions are somewhat different ("parent" property).
	# We gather the information from the entries where type is "" or "pclean" or "ptime" and use
	# them to augment the real entries.
	my %metadata;
	foreach my $property (sort keys %gets) {
		my $definition = $gets{$property};
		my %definition = %$definition;
		my $type = $definition{"type"} // "";

		my $parent = $definition->{"parent"};
		if (!defined $parent) {
			next;
		}

		my $channelId = $parent . "_" . $property;
		$metadata{$channelId} = $definition;
	}

	# Second pass for the real channel definitions.
	foreach my $property (sort keys %gets) {
		my $definition = $gets{$property};
		my %definition = %$definition;
		my $cmd = $definition{"cmd2"} // "";
		my $cmd1000 = $definition{"cmd3"};
		my $type = $definition{"type"} // "";
		my $argMin = $definition{"argMin"};
		my $argMax = $definition{"argMax"};
		my $propertyUnit = $definition{"unit"} // "";
		my $parent = $definition->{"parent"};

		if (defined $parent) {
			# These are the metadata entries that we stored to %metadata above.
			next;
		}

		print RECORDS "\t<!-- property $property, $cmd $type $propertyUnit -->\n";
		my $parse = $parsinghash{$type};
		my $previousChannelId;

		if (!defined($parse)) {
			print RECORDS "\t<!-- Unknown type: $type -->\n";
			next;
		}

		foreach (@$parse) {
			my ($name, $offset, $size, $format, $inverseScale) = @$_;
			$name =~ s/[ \:]*//g;

			my $requestByte = $cmd;
			my $requestByte1000 = $cmd1000;
			my $position = int($offset / 2 + 2); # for cmd 2, maybe more (len($cmd)*2)
			my $length = $size > 1 ? $size / 2 : 1;
			my $scale = 1 / $inverseScale;
			my $bitPosition = 0;
			my $min = $argMin;
			my $max = $argMax;
			my $step = $scale;
			my $odd = $offset % 2 == 1;
			my $itemType = "Number";
			my $channelId = $name || $property;

			if ($channelId eq "Date") {
				$channelId = "Year";
				$derivedNameIndex = 0;
			} elsif ($channelId eq "/") {
				$channelId = $DERIVED_NAME[$derivedNameIndex++];
			}

			if (rindex($channelId, $property, 0) < 0 && $channelId ne "version") {
				$channelId = "${property}_$channelId";
			}

			my $unit = $propertyUnit;
			if (defined $metadata{$channelId}) {
				if (!(defined $unit && length $unit)) {
					$unit = $metadata{$channelId}->{"unit"};
				}
				if (!(defined $min || length $min)) {
					$min = $metadata{$channelId}->{"argMin"};
				}
				if (!(defined $max || length $max)) {
					$max = $metadata{$channelId}->{"argMax"};
				}
			}
			$unit =~ s/\s//g;
			$unit =~ s/2/²/g;
			$unit =~ s/3/³/g;

			if ($format eq "hex2int") {
				# nothing special
			} elsif ($format eq "hex") {
				# nothing special
			} elsif ($format eq "swver") {
				# nothing special
			} elsif ($format eq "hex2time") {
				$min = 0;
				$max = 2359;
			} elsif ($format eq "hexdate") {
			# unsupported (or the one above)
	#		} elsif ($format eq "turnhexdate") {
	#			$max = 3112;
			} elsif ($format eq "weekday") {
				$min = 1;
				$max = 7;
			} elsif ($format eq "year") {
				# from 0 to 99, +2000 is assumed.
				$min = 0;
				$max = 99;
			} elsif ($format eq "quater") {
				# time in quarter hours since midnight
				$min = 0;
				$max = 95;
				if ($name eq "") {
					$previousChannelId = $channelId;
					$channelId .= "_Begin";
				} elsif ($name eq "--") {
					$channelId = $previousChannelId . "_End";
					$name = "- -"; # -- is not allowed in XML comments
				}
			} elsif ($format eq "opmode") {
				$min = 0;
				$max = 14; # todo right?
			} elsif ($format eq "opmode2") {
				$min = 0;
				$max = 1;
			} elsif ($format eq "opmodehc") {
				$min = 1;
				$max = 5;
			} elsif ($format eq "somwinmode") {
				$min = 1;
				$max = 2;
			} elsif ($format eq "faultmap") {
				# nothing special
			} elsif ($format eq "raw") {
				# have only either 1 or 2 bytes with type raw, so representing it
				# as a number is still adequate
			} elsif ($format eq "bit0") {
				$bitPosition = $odd ? 7 : 3;
				$itemType = "Switch";
			} elsif ($format eq "bit1") {
				$bitPosition = $odd ? 6 : 2;
				$itemType = "Switch";
			} elsif ($format eq "bit2") {
				$bitPosition = $odd ? 5 : 1;
				$itemType = "Switch";
			} elsif ($format eq "bit3") {
				$bitPosition = $odd ? 4 : 0;
				$itemType = "Switch";
			} elsif ($format eq "nbit0") {
				$bitPosition = $odd ? 7 : 3;
				$channelId .= "Inverted";
				$itemType = "Switch";
			} elsif ($format eq "nbit1") {
				$bitPosition = $odd ? 6 : 2;
				$channelId .= "Inverted";
				$itemType = "Switch";
			} else {
				print RECORDS "\t<!-- Unknown format: $format -->";
				print RECORDS "<!-- detail $name, $offset, $size, $format, $inverseScale -->\n";
				next;
			}

			# Min and max
			if ($channelId eq "sTimedate_Weekday") {
				$min = 0;
				$max = 6;
			} elsif ($channelId eq "sTimedate_Hour") {
				$min = 0;
				$max = 23;
			} elsif ($channelId eq "sTimedate_Min") {
				$min = 0;
				$max = 59;
			} elsif ($channelId eq "sTimedate_Sec") {
				$min = 0;
				$max = 59;
			} elsif ($channelId eq "sTimedate_Year") {
				$min = 0;
				$max = 99;
			} elsif ($channelId eq "sTimedate_Month") {
				$min = 1;
				$max = 12;
			} elsif ($channelId eq "sTimedate_Day") {
				$min = 1;
				$max = 31;
			} elsif ($channelId eq "version") {
				$scale = 0.01;
			}

			$min //= "0";
			$max //= "0";
			my $minMax = qq( min="$min" max="$max");

			# Units
			my $category;
			my $pattern = "";
			if ($unit =~ /°C/ || $unit eq "" && $channelId =~ /temp/i) {
				$category = "Temperature";
				$itemType .= ":" . $category;
				$unit = "°C";
			} elsif ($unit =~ /m³\/h/ || $unit =~/cl\/min/) {
				$category = "VolumetricFlowRate";
				$itemType .= ":" . $category;
			} elsif ($unit =~ /^days?$/i) {
				$category = "Time";
				$itemType .= ":" . $category;
				$unit = "d";
			} elsif ($unit =~ /^\/Day/) {
				$category = "Time";
				$itemType .= ":" . $category;
				$unit = "1/d";
			} elsif ($unit =~ /^hour$|^h$/) {
				$category = "Time";
				$itemType .= ":" . $category;
				$unit = "h";
			} elsif ($unit =~ /^min$|^s$/) {
				$category = "Time";
				$itemType .= ":" . $category;
			} elsif ($unit =~ /Wh$/) {
				$category = "Energy";
				$itemType .= ":" . $category;
			} elsif ($unit =~ /^%$/) {
				$category = "Dimensionless";
				$itemType .= ":" . $category;
			} elsif ($channelId =~ /P_/) {
				$category = "Pressure";
				$itemType .= ":" . $category;
				$unit = "bar";
			} elsif ($unit eq "Kmin") {
				# only used once, weird unit as it's a parameter for the PID control
			} elsif ($unit eq "K")  {
				$category = "Temperature";
				$itemType .= ":" . $category;
				# note: these are used for temperature differences, so they should not be converted to
				#       °C or °F for display!
			} elsif ($unit eq "Hz") {
				$category = "Time";
				$itemType .= ":" . $category;
			} elsif ($unit ne "") {
				print "UNKNOWN UNIT: $unit\n";
			}

			# Format for showing the value in OpenHAB
			if ($inverseScale == 10) {
				$pattern = ' pattern="%.1f %unit%"';
			} elsif ($inverseScale == 100) {
				$pattern = ' pattern="%.2f %unit%"';
			} elsif ($unit ne "") {
				# Includes inverseScales 1, 55 and 60 (2.06)
				$pattern = ' pattern="%.0f %unit%"';
			}

			# Determine the data type and if read-only
			my $dataType;
			my $readOnly = "";
			if ($max eq "0" || $channelId =~ /^s/) {
				$readOnly = qq( readOnly="true");
				$dataType = "Sensor";	# "Sensor" and "Status" seem to be the same
			} else {
				$dataType = "Settings";
			}

			# Write record to Thing XML
			print RECORDS qq(\t<record channelid="$channelId" requestByte="$requestByte" ) .
				(defined($requestByte1000) ? qq(requestByte1000="$requestByte1000" ) : "") .
				qq(dataType="$dataType" position="$position" length="$length" scale="$scale" ) .
				qq(bitPosition="$bitPosition"$minMax step="$step" unit="$unit"></record>);
			print RECORDS "<!-- detail $name, $offset, $size, $format, $inverseScale -->\n";

			# Channel type for XML
			my $label = $channelId;
			my $channelType = qq(\t<channel-type id="$channelId">\n) .
				qq(\t\t<item-type>$itemType</item-type>\n) .
				qq(\t\t<label>$label</label>\n) .
				(defined($category) ? qq(\t\t<category>$category</category>\n) : "") .
#				qq(\t\t<description></description>\n) .
				qq(\t\t<state$minMax$pattern$readOnly/>\n) .
				qq(\t</channel-type>\n);

			# Channel type might have been defined already for another firmware version.
			# We prefer the longer declaration, because these include the formatting pattern.
			my $useChannelType = 1;
			if (defined($channelTypes->{$channelId}) && $channelTypes->{$channelId} ne $channelType) {
				my $lenExisting = length($channelTypes->{$channelId});
				my $lenNew = length($channelType);

				if ($lenNew < $lenExisting) {
					$useChannelType = 0;
				} elsif ($lenNew == $lenExisting) {
					# There is a single case for this, which is p75passiveCooling, which has a
					# max value of 2 on 4.39 and 4 on 5.39.
					if ($channelType =~ /p75passiveCooling/) {
						if ($channelType =~ /max="2"/) {
							$useChannelType = 0;
						}
					} else {
						print "CHANNEL ALREADY EXISTS WITH CONFLICTING DETAILS: $channelId ($lenExisting vs. $lenNew)\n";
						print "  $channelTypes->{$channelId}";
						print "vs.\n";
						print "  $channelType";
					}
				}
			}
			if ($useChannelType) {
				$channelTypes->{$channelId} = $channelType;
			}

			if ($channelId eq "version") {
				next;
			}

			$channelId =~ /^([^_]*)/;
			my $group = $1;
			my $channelGroupType;
			if ($group =~ /^sTimedate|^party|^pClock/) {
				$channelGroupType = "channelGroupTypeTime";
			} elsif ($group =~ /^p0|^p1[012]/) {
				$channelGroupType = "channelGroupTypeNominalValues";
			} elsif ($group =~ /^p5[45678]|^pCircPump$/) {
				$channelGroupType = "channelGroupTypeCirculationPump";
			} elsif ($group =~ /^p1[3-9]|^p2|^p(30|31|49|50|76|77|78|79|80|86|99HC|99CoolingHC)|^sHC|^pHeat[12]$|^pSolar$|^zPumpHC$|^p99PumpRateHC$|^sDewPointHC|^sCoolHC/) {
				$channelGroupType = "channelGroupTypeHeating";
			} elsif ($group =~ /^p3[2-6]|^p8[39]|^p99DHW|^pDHW|^sDHW$|^zPumpDHW$|^p99PumpRateDHW$/) {
				$channelGroupType = "channelGroupTypeDomesticHotWater";
			} elsif ($group =~ /^p3[7-9]|^p4[0-6]|^p75|^p99Fan|^p99startUnschedVent|^pOvenFireplace|^pFan$|^sFan|^sSystem$/) {
				$channelGroupType = "channelGroupTypeVentilation";
			} elsif ($group =~ /^p85|^p99Frost|^pDefrostEva$/) {
				$channelGroupType = "channelGroupTypeEvaporator";
			} elsif ($group =~ /^sGlobal|^sSol|^sProgram/) {
				$channelGroupType = "channelGroupTypeCurrentValues";
			} elsif ($group =~ /^sControl/) {
				$channelGroupType = "channelGroupTypeControlValues";
			} elsif ($group =~ /^sDisplay/) {
				$channelGroupType = "channelGroupTypeDisplayValues";
			} elsif ($group =~ /^programHC|^pHeatProg/) {
				$channelGroupType = "channelGroupTypeHeatingProgram";
			} elsif ($group =~ /^programFan|^pFanProg/) {
				$channelGroupType = "channelGroupTypeVentilationProgram";
			} elsif ($group =~ /^programDHW|^pDHWProg$/) {
				$channelGroupType = "channelGroupTypeDomesticHotWaterProgram";
			} elsif ($group =~ /^sFirmware/) {
				$channelGroupType = "channelGroupTypeVersion";
			} elsif ($group =~ /^sLast10errors|zResetLast/) {
				$channelGroupType = "channelGroupTypeLastErrors";
			} elsif ($group =~ /^pOpMode/) {
				$channelGroupType = "channelGroupTypeOperationMode";
			} elsif ($group =~ /^pHoliday|^pAbsence$/) {
				$channelGroupType = "channelGroupTypeAbsenceProgram";
			} elsif ($group =~ /^sBoost|^sElectr|^sHeat|sHistory/) {
				$channelGroupType = "channelGroupTypeOperationCounters";
			} else {
				$channelGroupType = "channelGroupTypeMisc";
				print "MISSING (for $version), will sort into MISC: $channelId => $group\n";
			}

			my %emptyHash = ();
			$channelTypesPerGroup->{$channelGroupType} ||= \%emptyHash;
			$channelTypesPerGroup->{$channelGroupType}->{$channelId} = 1;
		}
	}

	print RECORDS "</records>\n";
	close(RECORDS);
}

sub saveChannelTypes {
	my $channelTypes = shift;
	my %channelTypes = %$channelTypes;

	my $CHANNEL_TYPES_FILE = "src/main/resources/OH-INF/thing/channel-types.xml";
	print "Saving to $CHANNEL_TYPES_FILE\n";
	open(CHANNEL_TYPES, ">", $CHANNEL_TYPES_FILE) or die $!;
	print CHANNEL_TYPES qq(<?xml version="1.0" encoding="UTF-8"?>
<thing:thing-descriptions bindingId="stiebelheatpump"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:thing="https://openhab.org/schemas/thing-description/v1.0.0"
	xsi:schemaLocation="https://openhab.org/schemas/thing-description/v1.0.0 https://openhab.org/schemas/thing-description-1.0.0.xsd">

	<channel-type id="requestBytes" advanced="true">
		<item-type>String</item-type>
		<label>Dump heat pump response for request byte </label>
		<description>Takes this request bytes and asks for a heat pump response, which is logged in the log.</description>
		<state pattern="%s"/>
	</channel-type>
	<channel-type id="respondBytes" advanced="true">
		<item-type>String</item-type>
		<label>Response form heat pump</label>
		<description>Represents the heat pump responds of the dump request.</description>
		<state pattern="%s" readOnly="true"/>
	</channel-type>
	<channel-type id="dumpResponse" advanced="true">
		<item-type>Switch</item-type>
		<label>Dump heat pump response </label>
		<description>Takes standard request bytes and asks for a heat pump response, which is logged in the log.</description>
	</channel-type>
	<channel-type id="dateTime">
		<item-type>String</item-type>
		<label>Last refresh</label>
		<description>Heatpump refreshed date time</description>
		<category>Date</category>
		<state readOnly="true"/>
	</channel-type>
	<channel-type id="setTime" advanced="true">
		<item-type>Switch</item-type>
		<label>Set time</label>
		<description>Set the time of the heat pump to system time of openHAB.</description>
	</channel-type>
);

	foreach my $channelType (sort keys %channelTypes) {
		print CHANNEL_TYPES $channelTypes{$channelType};
	}

	print CHANNEL_TYPES "</thing:thing-descriptions>\n";
	close(CHANNEL_TYPES);
}

sub saveChannelGroupTypes {
	my $channelTypesPerGroup = shift;
	my %channelTypesPerGroup = %$channelTypesPerGroup;
	my $CHANNEL_GROUP_TYPES_FILE = "src/main/resources/OH-INF/thing/channelgroup-types.xml";
	my $CHANNEL_GROUP_TYPES_TEMPLATE_FILE = "lwz-fhem-to-openhab/channelgroup-types.template.xml";

	print "Saving to $CHANNEL_GROUP_TYPES_FILE using template $CHANNEL_GROUP_TYPES_TEMPLATE_FILE\n";
	open(CHANNEL_GROUP_TYPES_TEMPLATE, "<", $CHANNEL_GROUP_TYPES_TEMPLATE_FILE) or die $!;
	my $channelGroupTypesDocument = do { local $/; <CHANNEL_GROUP_TYPES_TEMPLATE> };

	foreach my $channelType (keys %channelTypesPerGroup) {
		my $channels = "";
		my @channelsArray = sort keys %{$channelTypesPerGroup{$channelType}};
		foreach my $channel (@channelsArray) {
			$channels .= qq(\t\t\t<channel id="$channel" typeId="$channel"/>\n);
		}
		chop $channels;
		if ($channelGroupTypesDocument !~ s/\Q%$channelType%/$channels/gs) {
			print "LEFT-OVER: $channelType\n";
		}
	}
	$channelGroupTypesDocument =~ s/%.*?%//gs;

	open(CHANNEL_GROUP_TYPES, ">", $CHANNEL_GROUP_TYPES_FILE) or die $!;
	print CHANNEL_GROUP_TYPES $channelGroupTypesDocument;
	close(CHANNEL_GROUP_TYPES);
}

my %channelTypes = ();
my %channelTypesPerGroup = ();

#   Version  |  FHEM   |  openHAB (old)  |  openHAB (new)
# -----------+---------+-----------------+----------------
#   2.06     |  x (1)  |  x              |  x (1)
#   2.14     |  x (2)  |                 |  x (2)
#   2.14j    |  x      |                 |  x
#   2.36     |         |  x              |  x (2)
#   4.19     |         |  x (= 2.36)     |  x (2)
#   4.39     |  x      |                 |  x
#   4.39t    |  x      |                 |  -
#   5.09     |         |  x              |  x (3)
#   5.39     |  x (3)  |  x (= 5.09)     |  x (3)
#   7.39     |         |  x (= 5.09)     |  x (3)
#   7.59     |         |  x              |  x (3)
#   7.62     |         |  x              |  x (3)
# -----------+---------+-----------------+----------------

convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "2.06");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "2.14");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "2.14j");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "2.36", "2.14");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "4.19", "2.14");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "4.39");
#convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "4.39technician"); # not too important I suppose
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "5.09", "5.39");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "5.39");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ303", "7.39", "5.39");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "LWZ_THZ504", "7.59", "5.39");
convertRecords(\%channelTypesPerGroup, \%channelTypes, "Tecalor_THZ55", "7.62", "5.39");

saveChannelTypes(\%channelTypes);
saveChannelGroupTypes(\%channelTypesPerGroup);
