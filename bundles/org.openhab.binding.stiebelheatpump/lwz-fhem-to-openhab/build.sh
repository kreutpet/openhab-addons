#!/bin/bash
set -e

# Run the conversion script, the code formatter, the build, and copy it to the Raspberry PI.
# Run this from the directory openhab-addons/bundles/org.openhab.binding.stiebelheatpump.

./lwz-fhem-to-openhab/fhem2openhab.pl
mvn spotless:apply -pl :org.openhab.binding.stiebelheatpump
mvn clean install -pl :org.openhab.binding.stiebelheatpump
scp target/org.openhab.binding.stiebelheatpump-*-SNAPSHOT.jar root@raspberrypi:/opt/openhab/addons/
