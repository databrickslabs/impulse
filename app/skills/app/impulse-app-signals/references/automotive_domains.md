# Automotive Signal Domain Reference

Bilingual (EN/DE) mapping of problem domains to signal families. Use this to identify relevant channels when an engineer describes a problem or analysis goal in natural language.

## How to Use This Reference

1. **Match the user's description** to one or more domains below
2. **Call `search_channels`** with the patterns listed and pick channels from its results only
3. **Use units and the min/max range from the search results** as confirmation clues
4. **Suggest aggregations** from the recommendations at the end of each domain section

---

## Domain 1: Engine / Powertrain (Motor / Antriebsstrang)

**Typical questions:**
- EN: "engine speed distribution", "torque map", "throttle usage", "fuel consumption"
- DE: "Drehzahlverteilung", "Momentenkennfeld", "Gaspedalnutzung", "Kraftstoffverbrauch"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Engine speed | Motordrehzahl | `nmot`, `n_mot`, `Nmot`, `eng_spd`, `EngSpd`, `nMotor` | rpm, 1/min | 0–8000 |
| Engine torque (actual) | Motormoment (Ist) | `Mist`, `M_mot`, `tq_act`, `EngTrq`, `Md_ist` | Nm | -50–600 |
| Engine torque (target) | Motormoment (Soll) | `Msoll`, `M_soll`, `tq_req`, `TqDes`, `Md_soll` | Nm | -50–600 |
| Throttle position | Drosselklappenstellung | `wdks`, `thr_pos`, `ThrottlePos`, `DK_Pos`, `tps` | % | 0–100 |
| Accelerator pedal | Fahrpedalstellung | `wped`, `AccPed`, `APP_Pos`, `FahrPed`, `PedalPos` | % | 0–100 |
| Boost pressure | Ladedruck | `pLade`, `p_boost`, `BoostPres`, `pTurbo` | kPa, bar | 0–3.5 |
| Intake manifold pressure | Saugrohrdruck | `ps`, `MAP`, `p_saug`, `pSaug` | kPa, mbar | 20–250 |
| Intake air temperature | Ansauglufttemperatur | `tans`, `T_intake`, `IAT`, `tLuft` | °C | -40–80 |
| Fuel pressure (rail) | Raildruck | `pKrst`, `p_fuel`, `pRail`, `RailPres` | bar, MPa | 0–2500 |
| Injection quantity | Einspritzmenge | `qMI`, `InjQty`, `q_inj`, `mKrst` | mg/hub | 0–100 |
| Lambda | Lambda | `lam`, `Lambda`, `LAM`, `lambda_act`, `AFR` | - | 0.7–1.3 |
| Engine load | Motorlast | `rl`, `EngLoad`, `load_eng`, `RL` | % | 0–100 |
| Ignition angle | Zuendwinkel | `zwout`, `SpkAdv`, `IgnAngle`, `ZW` | °KW | -10–50 |
| Camshaft position | Nockenwellenposition | `NW_pos`, `CamPos`, `VVT_in`, `VVT_ex` | °KW | -50–50 |
| Turbo speed | Turboladerdrehzahl | `nTurbo`, `TurboSpd`, `n_turbo` | rpm | 0–300000 |

**Suggested aggregations:**
- Engine speed → duration histogram (0, 500, 1000, ..., 7000 rpm)
- Engine speed vs. torque → 2D histogram (operating point map)
- Pedal position → duration histogram (0, 10, 20, ..., 100 %)
- Boost pressure → duration histogram

---

## Domain 2: Thermal Management (Thermomanagement)

**Typical questions:**
- EN: "coolant temperature under load", "oil temp distribution", "overheating analysis"
- DE: "Kuehlmitteltemperatur unter Last", "Oeltemperaturverteilung", "Ueberhitzungsanalyse"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Coolant temperature | Kuehlmitteltemperatur | `tKueMi`, `T_cool`, `CoolantTemp`, `tCool`, `ECT`, `TWasser` | °C | -40–130 |
| Engine oil temperature | Motoroeltemperatur | `tOel`, `T_oil`, `EngOilTemp`, `toil` | °C | -40–160 |
| Exhaust gas temperature | Abgastemperatur | `tAbg`, `T_exh`, `EGT`, `ExhTemp`, `tAbgas` | °C | 0–1100 |
| Exhaust pre/post-cat | Abgastemp vor/nach Kat | `tAbgVorKat`, `tAbgNachKat`, `EGT_preCAT`, `EGT_postCAT` | °C | 0–1100 |
| Intercooler outlet temp | LLK-Austrittstemperatur | `T_LLK_out`, `tLLKAus`, `CAC_out` | °C | -40–80 |
| Ambient temperature | Umgebungstemperatur | `tUmg`, `T_amb`, `AmbTemp`, `tAussen` | °C | -40–55 |
| Transmission oil temp | Getriebeoel-Temperatur | `tGetriebe`, `T_trans`, `TransOilTemp`, `ATF_temp` | °C | -40–160 |
| Brake disc temperature | Bremsscheibentemperatur | `tBrSch`, `T_brake_disc`, `BrkDiscTemp` | °C | -40–800 |
| Fan speed | Luefterdrehzahl | `nLuefter`, `FanSpd`, `nFan` | rpm, % | 0–5000 |

**Suggested aggregations:**
- Coolant temp → duration histogram (-40, -20, 0, 20, 40, 60, 80, 100, 120, 140 °C)
- Oil temp → duration histogram
- Exhaust temp vs. RPM → 2D histogram
- Fan activation events → condition event; results show instance count and total time
- Statistics (min, max, mean) across all temp signals

---

## Domain 3: Chassis / Vehicle Dynamics (Fahrwerk / Fahrdynamik)

**Typical questions:**
- EN: "lateral acceleration distribution", "brake usage", "ESP interventions", "speed profile"
- DE: "Querbeschleunigungsverteilung", "Bremsnutzung", "ESP-Eingriffe", "Geschwindigkeitsprofil"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Vehicle speed | Fahrzeuggeschwindigkeit | `vfzg`, `V_Fzg`, `VehSpd`, `VSS`, `pveh`, `v_veh` | km/h | 0–300 |
| Wheel speed FL/FR/RL/RR | Raddrehzahl VL/VR/HL/HR | `vRadVL`, `vRadVR`, `vRadHL`, `vRadHR`, `WhlSpd_FL`..`RR` | km/h | 0–300 |
| Lateral acceleration | Querbeschleunigung | `aq`, `AY`, `ay`, `LatAcc`, `aQuer`, `a_lat` | m/s², g | -15–15 |
| Longitudinal acceleration | Laengsbeschleunigung | `ax`, `AX`, `LonAcc`, `aLaengs`, `a_lon` | m/s², g | -15–15 |
| Vertical acceleration | Vertikalbeschleunigung | `az`, `AZ`, `VertAcc`, `aVert`, `a_vert` | m/s², g | -20–20 |
| Yaw rate | Gierrate | `YawRate`, `psi_dot`, `dPsi`, `GierRate`, `YRS` | °/s | -100–100 |
| Steering angle | Lenkwinkel | `LW`, `SteerAng`, `delta_H`, `SWA`, `LenkWinkel` | ° | -720–720 |
| Steering torque | Lenkmoment | `MLenk`, `SteerTrq`, `M_steer` | Nm | -20–20 |
| Brake pressure | Bremsdruck | `pBrake`, `BrkPres`, `p_brake`, `pHZ` | bar | 0–200 |
| ABS/ESP/TCS active | ABS/ESP/ASR aktiv | `ABS_act`, `ESP_act`, `ESC_active`, `TCS_act`, `ASR_act` | bool | 0/1 |
| Suspension travel | Federweg | `sFedVL`..`HR`, `SusTravel_FL`..`RR`, `RideHt_FL`..`RR` | mm | -120–120 |
| Tire pressure | Reifendruck | `pReifVL`..`HR`, `TirePres_FL`..`RR`, `TPMS_FL`..`RR` | bar | 1.5–4.0 |

**Suggested aggregations:**
- Vehicle speed → duration + distance histogram (0, 10, 20, ..., 200 km/h)
- Lateral acceleration → duration histogram (-10, -8, ..., 8, 10 m/s²)
- Lat-acc vs. speed → 2D histogram
- ESP events → condition event; results show instance count and total time
- Suspension travel → duration histogram

---

## Domain 4: Transmission (Getriebe)

**Typical questions:**
- EN: "time in each gear", "shift frequency", "clutch usage"
- DE: "Gangverteilung", "Schaltfrequenz", "Kupplungsnutzung"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Gear (current) | Gang (aktuell) | `Gang`, `gear`, `GearAct`, `gear_act`, `CurrGear`, `GangIst` | - | 0–10 |
| Gear (target) | Gang (Soll) | `GangSoll`, `gear_tgt`, `GearTgt` | - | 0–10 |
| Transmission input speed | Getriebeeingangsdrehzahl | `nGE`, `n_trans_in`, `TransInSpd` | rpm | 0–8000 |
| Transmission output speed | Getriebeausgangsdrehzahl | `nGA`, `n_trans_out`, `TransOutSpd` | rpm | 0–5000 |
| Clutch position | Kupplungsposition | `KupplPos`, `ClutchPos`, `clutch_pos` | %, mm | 0–100 |
| Torque converter slip | Wandlerschlupf | `TC_slip`, `WandlerSchlupf`, `TqConvSlip` | rpm, % | varies |

**Suggested aggregations:**
- Gear → duration histogram (1 bin per gear)
- Shift events → points-in-time event on gear changes, for example gear.rising_edges() | gear.falling_edges()
- Clutch engagement → condition event; results show total time

---

## Domain 5: Electric / Hybrid Powertrain (Elektro / Hybrid)

**Typical questions:**
- EN: "SOC distribution", "e-motor operating points", "regen braking energy"
- DE: "Ladezustandsverteilung", "E-Motor Betriebspunkte", "Rekuperationsenergie"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| HV battery voltage | HV-Batteriespannung | `U_HV`, `HVBattVolt`, `uBattHV` | V | 200–800 |
| HV battery current | HV-Batteriestrom | `I_HV`, `HVBattCurr`, `iBattHV` | A | -500–500 |
| State of charge | Ladezustand (SOC) | `SOC`, `BattSOC`, `soc_batt`, `StateOfCharge` | % | 0–100 |
| HV battery power | HV-Batterieleistung | `P_HV`, `HVBattPow`, `pBattHV` | kW | -300–300 |
| E-motor speed | E-Motor Drehzahl | `n_EM`, `EMotSpd`, `nEMot`, `EM_Speed` | rpm | -20000–20000 |
| E-motor torque | E-Motor Moment | `M_EM`, `EMotTrq`, `mEMot`, `EM_Torque` | Nm | -400–400 |
| E-motor temperature | E-Motor Temperatur | `T_EM`, `EMotTemp`, `tEMot` | °C | -40–180 |
| Inverter temperature | Invertertemperatur | `T_inv`, `InvTemp`, `tInverter` | °C | -40–120 |
| Cell voltage min/max | Zellspannung min/max | `U_cell_min`, `U_cell_max`, `CellVoltMin`, `CellVoltMax` | V | 2.5–4.2 |
| Battery temperature | Batterietemperatur | `tBatt`, `T_batt`, `BattTemp`, `HVBattTemp`, `T_cell_max` | °C | -40–60 |

**Suggested aggregations:**
- SOC → duration histogram (0, 10, 20, ..., 100 %)
- E-motor torque vs. speed → 2D histogram (operating point map)
- Battery power → duration histogram
- Battery temp → duration histogram

---

## Domain 6: Emissions / Aftertreatment (Abgas / Emissionen)

**Typical questions:**
- EN: "NOx during cold start", "DPF regen frequency", "catalyst light-off"
- DE: "NOx im Kaltstart", "DPF-Regenerationshaeufigkeit", "Katalysator-Anspringverhalten"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| NOx pre/post-cat | NOx vor/nach Kat | `NOx_pre`, `NOx_post`, `cNOx_vor`, `cNOx_nach` | ppm | 0–3000 |
| Soot / DPF load | Russmasse / DPF-Beladung | `soot_mass`, `mRuss`, `SootLoad`, `DPF_load` | g, g/l | 0–50 |
| DPF differential pressure | DPF-Differenzdruck | `dpDPF`, `DPF_dP`, `dP_DPF` | mbar | 0–200 |
| DPF regen active | DPF-Regeneration aktiv | `DPF_regen`, `flgDPFRegen` | bool | 0/1 |
| Catalyst temperature | Katalysatortemperatur | `T_cat`, `tKat`, `CatTemp` | °C | 0–1100 |
| EGR valve / rate | AGR-Ventil / Rate | `AGR_pos`, `EGR_pos`, `AGR_rate`, `EGR_rate` | %, mm | 0–60 |
| Mass air flow | Luftmassenstrom | `LMS`, `MAF`, `dm_air`, `AirMassFlow`, `mLuft` | kg/h, g/s | 0–1500 |
| AdBlue dosing | AdBlue-Dosierung | `AdBlueDose`, `SCR_dose`, `urea_rate`, `DEF_rate` | ml/h | 0–5000 |

**Suggested aggregations:**
- NOx → duration histogram
- Catalyst temp → duration histogram
- DPF regen events → condition event; results show instance count and total time
- Soot vs. speed → 2D histogram
- EGR rate → duration histogram

---

## Domain 7: NVH (Akustik / Schwingungen)

**Typical questions:**
- EN: "steering wheel vibration", "interior noise at highway speed"
- DE: "Lenkradvibration", "Innengeraeusch bei Autobahngeschwindigkeit"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Accelerometer X/Y/Z | Beschleunigungssensor | `Acc_X`, `Acc_Y`, `Acc_Z`, `a_x`, `a_y`, `a_z` | m/s², g | varies |
| Steering wheel vibration | Lenkradvibration | `Acc_SW`, `SteerVib`, `a_Lenkrad` | m/s², g | 0–20 |
| Interior microphone | Innenraum-Mikrofon | `Mic_int`, `IntNoise`, `SPL_int`, `dB_innen` | dB(A), Pa | 30–100 |
| Exterior microphone | Aussen-Mikrofon | `Mic_ext`, `ExtNoise`, `SPL_ext`, `dB_aussen` | dB(A), Pa | 40–120 |
| Strain gauge | Dehnmessstreifen (DMS) | `DMS_`, `strain_`, `eps_`, `SG_`, `Dehnung` | um/m | varies |

**Suggested aggregations:**
- Interior noise vs. speed → 2D histogram
- Steering vibration → duration histogram
- Statistics (min, max, mean) for accelerometer signals

---

## Domain 8: ADAS / Driver Assistance (Fahrerassistenz)

**Typical questions:**
- EN: "following distance distribution", "AEB trigger frequency", "ACC usage"
- DE: "Folgeabstandsverteilung", "AEB-Ausloesehaeufigkeit", "ACC-Nutzung"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Radar distance | Radarabstand | `RadarDist`, `dRadar`, `FollowDist`, `dist_obj1` | m | 0–250 |
| Time to collision | Zeit bis Kollision | `TTC`, `TimeToCollision`, `ttc_obj1` | s | 0–10 |
| Time headway | Zeitluecke | `THW`, `TimeHeadway`, `thw` | s | 0–10 |
| ACC active / set speed | ACC aktiv / Sollgeschw. | `ACC_act`, `ACC_set`, `SetSpd`, `CruiseSetSpd` | bool, km/h | varies |
| Lane offset | Spurabweichung | `LaneOff`, `LnOffset`, `lat_dev` | m | -2–2 |
| AEB triggered | AEB ausgeloest | `AEB_trig`, `AEB_active`, `FCW_active` | bool | 0/1 |
| GPS lat/lon | GPS Breite/Laenge | `GPS_lat`, `GPS_lon`, `Latitude`, `Longitude` | ° | varies |

**Suggested aggregations:**
- Following distance → duration histogram
- Headway vs. speed → 2D histogram
- AEB events → condition event; results show instance count and total time
- Lane offset → duration histogram

---

## Domain 9: Durability / Fatigue (Dauerhaltbarkeit / Ermuedung)

**Typical questions:**
- EN: "load spectrum at rear axle", "spring travel distribution", "road impact events"
- DE: "Lastspektrum Hinterachse", "Federwegverteilung", "Strassenschlaege"

| Signal (EN) | Signal (DE) | Channel patterns | Unit | Range |
|---|---|---|---|---|
| Wheel force (vertical) | Radkraft (vertikal) | `F_wheel_z`, `WhlForceZ`, `Fz_FL`..`RR`, `RadkraftZ` | N, kN | 0–30000 |
| Spring travel | Federweg | `sFed`, `SprTravel`, `s_spring`, `Federweg` | mm | -150–150 |
| Shock absorber velocity | Daempfergeschwindigkeit | `vDmpf`, `DmpVel`, `v_damp` | m/s | varies |
| Cumulative distance | Kumulative Strecke | `s_cum`, `CumDist`, `odo`, `Odometer` | km | monotonic |
| Strain gauge | DMS | `DMS_`, `strain_`, `eps_`, `Dehnung` | um/m | varies |

**Suggested aggregations:**
- Wheel force → duration histogram (load spectrum)
- Spring travel → duration histogram
- Impact events → condition event; results show instance count and total time
- Strain → duration histogram
- Statistics (min, max, mean) for force signals

---

## German Abbreviation Decoder

Common prefixes and abbreviations in German OEM channel names:

| Prefix | German | English |
|---|---|---|
| `n` / `Drz` | Drehzahl | Rotational speed |
| `M` / `Md` | Moment / Drehmoment | Torque |
| `t` / `T` | Temperatur | Temperature |
| `p` | Druck | Pressure |
| `v` | Geschwindigkeit | Velocity |
| `a` | Beschleunigung | Acceleration |
| `F` | Kraft | Force |
| `s` | Weg / Strecke | Distance / travel |
| `U` / `u` | Spannung | Voltage |
| `I` / `i` | Strom | Current |
| `P` | Leistung | Power |
| `q` / `Q` | Menge / Durchfluss | Quantity / flow |
| `m` / `dm` | Masse / Massenstrom | Mass / mass flow |
| `mot` | Motor | Engine |
| `Fzg` | Fahrzeug | Vehicle |
| `Rad` | Rad | Wheel |
| `Oel` | Oel | Oil |
| `KueMi` | Kuehlmittel | Coolant |
| `Abg` | Abgas | Exhaust |
| `Krst` | Kraftstoff | Fuel |
| `Getr` | Getriebe | Transmission |
| `Brm` / `Br` | Bremse | Brake |
| `Lenk` | Lenkung | Steering |
| `Kuppl` | Kupplung | Clutch |
| `Lade` | Ladedruck | Boost pressure |
| `Kat` | Katalysator | Catalyst |
| `LLK` | Ladeluftkuehler | Intercooler |
| `AGR` | Abgasrueckfuehrung | EGR |
| `DPF` / `OPF` | Partikelfilter | Particulate filter |
| `VL` / `VR` / `HL` / `HR` | vorne links/rechts, hinten links/rechts | FL / FR / RL / RR |
| `ist` | Istwert | Actual value |
| `soll` | Sollwert | Setpoint / target |

## Unit Strings in MDF/MF4 Files

| Quantity | Common unit strings |
|---|---|
| Rotational speed | `rpm`, `1/min`, `U/min`, `rev/min` |
| Temperature | `degC`, `°C`, `C`, `K` |
| Pressure | `bar`, `mbar`, `kPa`, `Pa`, `MPa` |
| Speed | `km/h`, `m/s`, `mph` |
| Acceleration | `m/s2`, `m/s^2`, `g`, `mg` |
| Force | `N`, `kN`, `daN` |
| Torque | `Nm`, `N.m`, `kNm` |
| Voltage | `V`, `mV` |
| Current | `A`, `mA` |
| Power | `W`, `kW` |
| Flow | `l/h`, `l/min`, `g/s`, `kg/h` |
| Concentration | `ppm`, `%`, `mg/m3`, `g/km` |
| Angle | `deg`, `°`, `rad`, `°KW` |
| Sound | `dB`, `dB(A)`, `dBA`, `Pa` |

## Quick Natural-Language Lookup

| Engineer says... | Domain | Key signals |
|---|---|---|
| "engine speed", "RPM", "Drehzahl" | Powertrain | `nmot`, `eng_spd` |
| "torque", "Moment" | Powertrain | `Mist`, `M_mot` |
| "throttle", "gas pedal", "Fahrpedal" | Powertrain | `wdks`, `wped` |
| "vehicle speed", "Geschwindigkeit" | Chassis | `vfzg`, `VehSpd` |
| "lateral acceleration", "Querbeschleunigung" | Chassis | `aq`, `AY` |
| "brake", "Bremse" | Chassis | `pBrake`, `BrkPres` |
| "steering", "Lenkung" | Chassis | `LW`, `SteerAng` |
| "temperature", "hot", "cold", "heiss", "kalt" | Thermal | `tKueMi`, `tOel`, `tAbg` |
| "coolant", "Kuehlmittel" | Thermal | `tKueMi`, `T_cool` |
| "exhaust", "Abgas", "EGT" | Thermal + Emissions | `tAbg`, `EGT` |
| "battery", "SOC", "charge", "Ladezustand" | EV/HEV | `SOC`, `U_HV` |
| "e-motor", "E-Motor" | EV/HEV | `n_EM`, `M_EM` |
| "NOx", "emissions", "Emissionen" | Emissions | `NOx_pre`, `NOx_post` |
| "DPF", "soot", "Russ", "Partikelfilter" | Emissions | `DPF_dP`, `soot_mass` |
| "vibration", "NVH", "Schwingung" | NVH | `Acc_X`, `Acc_Y`, `Acc_Z` |
| "noise", "Geraeusch", "Lautstaerke" | NVH | `Mic_int`, `SPL` |
| "gear", "Gang", "shift", "Schaltung" | Transmission | `Gang`, `GearAct` |
| "radar", "distance", "Abstand" | ADAS | `RadarDist`, `TTC` |
| "ACC", "cruise", "Tempomat" | ADAS | `ACC_act`, `ACC_set` |
| "ABS", "ESP", "stability", "Stabilitaet" | Chassis | `ABS_act`, `ESP_act` |
| "fuel", "Kraftstoff", "Verbrauch" | Powertrain | `fc_inst`, `FuelCons` |
| "strain", "DMS", "fatigue", "Ermuedung" | Durability | `DMS_`, `strain_` |
| "wheel force", "Radkraft" | Durability | `F_wheel_z` |
| "suspension", "Fahrwerk", "Feder" | Chassis / Durability | `sFed`, `SusTravel` |
| "oil", "Oel" | Thermal | `tOel`, `pOel` |
| "tire", "Reifen", "TPMS" | Chassis | `pReifVL`, `TPMS_FL` |
