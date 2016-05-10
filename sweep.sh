#!/bin/bash
export DYLD_FALLBACK_LIBRARY_PATH=/Library/PostgreSQL/9.5/lib:$DYLD_LIBRARY_PATH

for a in timestamp area_id building_id interiorequipment_electricity_j interiorequipment_gas_kw fans_electricity_j fans_electricity_kw heating_electricity_kw electricity_facility_kw interiorequipment_gas_j interiorlights_electricity_kw electricity_facility_j water_heater_watersystems_gas_kw heating_gas_j cooling_electricity_j water_heater_watersystems_gas_j interiorlights_electricity_j heating_electricity_j gas_facility_j cooling_electricity_kw heating_gas_kw gas_facility_kw interiorequipment_electricity_kw building_name
do
	for b in timestamp area_id building_id interiorequipment_electricity_j interiorequipment_gas_kw fans_electricity_j fans_electricity_kw heating_electricity_kw electricity_facility_kw interiorequipment_gas_j interiorlights_electricity_kw electricity_facility_j water_heater_watersystems_gas_kw heating_gas_j cooling_electricity_j water_heater_watersystems_gas_j interiorlights_electricity_j heating_electricity_j gas_facility_j cooling_electricity_kw heating_gas_kw gas_facility_kw interiorequipment_electricity_kw building_name
	do
		if [[ "$a" < "$b" ]]; then
			python script/py_analysis/plot_distribution.py --csv src/test/resources/data/us_energy_0p1.csv.gz --hist2d $a $b --savefig target/plots/$a-$b.png
		fi
	done
done
