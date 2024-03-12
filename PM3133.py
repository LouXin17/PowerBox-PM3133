import serial, subprocess, time, json, re
import modbus_tk
import modbus_tk.defines as cst
import modbus_tk.modbus_rtu as modbus_rtu
import paho.mqtt.client as mqtt

from struct import *

 #/--------------Config MQTT-------------------/
broker_address = {MQTT Boker}
ports = {MQTT Port}
mqtt_topic = {MQTT Topic}

 #/--------------Config Modbus-------------------/
mbComPort = "/dev/ttyUSB0"
baudrate = 19200
databit = 8 #7, 8
parity = "N" #N, O, E
stopbit = 1 #1, 2
mbTimeout = 1000 # ms
mbId = 1


try:
    mb_port = serial.Serial(port=mbComPort, baudrate=baudrate, bytesize=databit, parity=parity, stopbits=stopbit)
    master = modbus_rtu.RtuMaster(mb_port)
    master.set_timeout(mbTimeout/1000.0)
except:
    print("[WARING] Serial Get Data ERROR ")
    pass


while True:
    try:
        #/--------------Relay Value-------------------/

        # DO_0_value =  master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1000, 1)
        DO_1_value =  master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1001, 1)
        DO_0_Power_On_Value =  master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1010, 1)
        DO_1_Power_On_Value =  master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1011, 1)

        RelayValueData = {
            # "DO_0_value": DO_0_value[0],
            "DO_1_value": DO_1_value[0],
            "DO_0_Power_On_Value": DO_0_Power_On_Value[0],
            "DO_1_Power_On_Value": DO_1_Power_On_Value[0]
        }
        RelayValuePayload = json.dumps(RelayValueData)

        #/-------System Parameter Setting--------------/

        PT_Ratio, CT_Ratio = master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1003, 2)
        Wiring_Mode = master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x100A, 1)
        Default_Frequency = master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x100D, 1)
        Energy_Absolute_Accumulated_Mode = master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1010, 1)
        Harmonic_Phase_Select, Display_Voltage = master.execute(mbId, cst.READ_HOLDING_REGISTERS, 0x1011, 2)

        SystemParameterSettingData = {
            "PT_Ratio": PT_Ratio,
            "CT_Ratio": CT_Ratio,
            "Wiring_Mode": Wiring_Mode[0],
            "Default_Frequency": hex(Default_Frequency[0]),
            "Energy_Absolute_Accumulated_Mode": Energy_Absolute_Accumulated_Mode[0],
            "Harmonic_Phase_Select": Harmonic_Phase_Select,
            "Display_Voltage": Display_Voltage
        }
        SystemParameterSettingPayload = json.dumps(SystemParameterSettingData)

        #/-----------System Information---------------/

        System_Information_Data = master.execute(mbId, cst.READ_INPUT_REGISTERS, 0x0200, 5)
        Wiring_Type = System_Information_Data[0]
        Phase_Sequence = System_Information_Data[1]
        Model_Name = System_Information_Data[2]
        Model_Type = System_Information_Data[3]
        Firmware_Version = System_Information_Data[4]

        SystemInformationData = {
            "Wiring_Type": Wiring_Type,
            "Phase_Sequence": Phase_Sequence,
            "Model_Name": Model_Name,
            "Model_Type": Model_Type,
            "Firmware_Version": Firmware_Version
        }
        SystemInformationPayload = json.dumps(SystemInformationData)

        #/--------------Power Value A------------------/
        
        Power_A_Data= master.execute(mbId, cst.READ_INPUT_REGISTERS, 0x1100, 18)
        V_a_Float_Data = [Power_A_Data[0], Power_A_Data[1]]
        I_a_Float_Data = [Power_A_Data[2], Power_A_Data[3]]
        kW_a_Float_Data = [Power_A_Data[4], Power_A_Data[5]]
        kvar_a_Float_Data = [Power_A_Data[6], Power_A_Data[7]]
        kVA_a_Float_Data = [Power_A_Data[8], Power_A_Data[9]]
        PF_a_Float_Data = [Power_A_Data[10], Power_A_Data[11]]
        kWh_a_Float_Data = [Power_A_Data[12], Power_A_Data[13]]
        kvarh_a_Float_Data = [Power_A_Data[14], Power_A_Data[15]]
        kVAh_a_Float_Data = [Power_A_Data[16], Power_A_Data[17]]


        # convert to float:
        # IEEE754 ==> (Hi word Hi Bite, Hi word Lo Byte, Lo word Hi Byte, Lo word Lo Byte)
        V_a_Float = unpack('>f', pack('>HH', Power_A_Data[1], Power_A_Data[0]))
        I_a_Float = unpack('>f', pack('>HH', I_a_Float_Data[1], I_a_Float_Data[0]))
        kW_a_Float = unpack('>f', pack('>HH', kW_a_Float_Data[1], kW_a_Float_Data[0]))
        kvar_a_Float = unpack('>f', pack('>HH', kvar_a_Float_Data[1], kvar_a_Float_Data[0]))
        kVA_a_Float = unpack('>f', pack('>HH', kVA_a_Float_Data[1], kVA_a_Float_Data[0]))
        PF_a_Float = unpack('>f', pack('>HH', PF_a_Float_Data[1], PF_a_Float_Data[0]))
        kWh_a_Float = unpack('>f', pack('>HH', kWh_a_Float_Data[1], kWh_a_Float_Data[0]))
        kvarh_a_Float = unpack('>f', pack('>HH', kvarh_a_Float_Data[1], kvarh_a_Float_Data[0]))
        kVAh_a_Float = unpack('>f', pack('>HH', kVAh_a_Float_Data[1], kVAh_a_Float_Data[0]))

        PowerAData = {
            "V_a_Float": V_a_Float[0],
            "I_a_Float": I_a_Float[0],
            "kW_a_Float": kW_a_Float[0],
            "kvar_a_Float": kvar_a_Float[0],
            "kVA_a_Float": kVA_a_Float[0],
            "PF_a_Float": PF_a_Float[0],
            "kWh_a_Float": kWh_a_Float[0],
            "kvarh_a_Float": kvarh_a_Float[0],
            "kVAh_a_Float": kVAh_a_Float[0]
        }
        PowerAPayload = json.dumps(PowerAData)


        #/--------------Power Value B------------------/

        Power_B_Data= master.execute(mbId, cst.READ_INPUT_REGISTERS, 0x1112, 18)
        V_b_Float_Data = [Power_B_Data[0], Power_B_Data[1]]
        I_b_Float_Data = [Power_B_Data[2], Power_B_Data[3]]
        kW_b_Float_Data = [Power_B_Data[4], Power_B_Data[5]]
        kvar_b_Float_Data = [Power_B_Data[6], Power_B_Data[7]]
        kVA_b_Float_Data = [Power_B_Data[8], Power_B_Data[9]]
        PF_b_Float_Data = [Power_B_Data[10], Power_B_Data[11]]
        kWh_b_Float_Data = [Power_B_Data[12], Power_B_Data[13]]
        kvarh_b_Float_Data = [Power_B_Data[14], Power_B_Data[15]]
        kVAh_b_Float_Data = [Power_B_Data[16], Power_B_Data[17]]

        # convert to float:
        # IEEE754 ==> (Hi word Hi Bite, Hi word Lo Byte, Lo word Hi Byte, Lo word Lo Byte)
        V_b_Float = unpack('>f', pack('>HH', V_b_Float_Data[1], V_b_Float_Data[0]))
        I_b_Float = unpack('>f', pack('>HH', I_b_Float_Data[1], I_b_Float_Data[0]))
        kW_b_Float = unpack('>f', pack('>HH', kW_b_Float_Data[1], kW_b_Float_Data[0]))
        kvar_b_Float = unpack('>f', pack('>HH', kvar_b_Float_Data[1], kvar_b_Float_Data[0]))
        kVA_b_Float = unpack('>f', pack('>HH', kVA_b_Float_Data[1], kVA_b_Float_Data[0]))
        PF_b_Float = unpack('>f', pack('>HH', PF_b_Float_Data[1], PF_b_Float_Data[0]))
        kWh_b_Float = unpack('>f', pack('>HH', kWh_b_Float_Data[1], kWh_b_Float_Data[0]))
        kvarh_b_Float = unpack('>f', pack('>HH', kvarh_b_Float_Data[1], kvarh_b_Float_Data[0]))
        kVAh_b_Float = unpack('>f', pack('>HH', kVAh_b_Float_Data[1], kVAh_b_Float_Data[0]))

        PowerBData = {
            "V_b_Float": V_b_Float[0],
            "I_b_Float": I_b_Float[0],
            "kW_b_Float": kW_b_Float[0],
            "kvar_b_Float": kvar_b_Float[0],
            "kVA_b_Float": kVA_b_Float[0],
            "PF_b_Float": PF_b_Float[0],
            "kWh_b_Float": kWh_b_Float[0],
            "kvarh_b_Float": kvarh_b_Float[0],
            "kVAh_b_Float": kVAh_b_Float[0]
        }
        PowerBPayload = json.dumps(PowerBData)

        #/--------------Power Value c------------------/

        Power_C_Data= master.execute(mbId, cst.READ_INPUT_REGISTERS, 0x1124, 18)
        V_c_Float_Data = [Power_C_Data[0], Power_C_Data[1]]
        I_c_Float_Data = [Power_C_Data[2], Power_C_Data[3]]
        kW_c_Float_Data = [Power_C_Data[4], Power_C_Data[5]]
        kvar_c_Float_Data = [Power_C_Data[6], Power_C_Data[7]]
        kVA_c_Float_Data = [Power_C_Data[8], Power_C_Data[9]]
        PF_c_Float_Data = [Power_C_Data[10], Power_C_Data[11]]
        kWh_c_Float_Data = [Power_C_Data[12], Power_C_Data[13]]
        kvarh_c_Float_Data = [Power_C_Data[14], Power_C_Data[15]]
        kVAh_c_Float_Data = [Power_C_Data[16], Power_C_Data[17]]

        # convert to float:
        # IEEE754 ==> (Hi word Hi Bite, Hi word Lo Byte, Lo word Hi Byte, Lo word Lo Byte)
        V_c_Float = unpack('>f', pack('>HH', V_c_Float_Data[1], V_c_Float_Data[0]))
        I_c_Float = unpack('>f', pack('>HH', I_c_Float_Data[1], I_c_Float_Data[0]))
        kW_c_Float = unpack('>f', pack('>HH', kW_c_Float_Data[1], kW_c_Float_Data[0]))
        kvar_c_Float = unpack('>f', pack('>HH', kvar_c_Float_Data[1], kvar_c_Float_Data[0]))
        kVA_c_Float = unpack('>f', pack('>HH', kVA_c_Float_Data[1], kVA_c_Float_Data[0]))
        PF_c_Float = unpack('>f', pack('>HH', PF_c_Float_Data[1], PF_c_Float_Data[0]))
        kWh_c_Float = unpack('>f', pack('>HH', kWh_c_Float_Data[1], kWh_c_Float_Data[0]))
        kvarh_c_Float = unpack('>f', pack('>HH', kvarh_c_Float_Data[1], kvarh_c_Float_Data[0]))
        kVAh_c_Float = unpack('>f', pack('>HH', kVAh_c_Float_Data[1], kVAh_c_Float_Data[0]))

        PowerCData = {
            "V_c_Float": V_c_Float[0],
            "I_c_Float": I_c_Float[0],
            "kW_c_Float": kW_c_Float[0],
            "kvar_c_Float": kvar_c_Float[0],
            "kVA_c_Float": kVA_c_Float[0],
            "PF_c_Float": PF_c_Float[0],
            "kWh_c_Float": kWh_c_Float[0],
            "kvarh_c_Float": kvarh_c_Float[0],
            "kVAh_c_Float": kVAh_c_Float[0]
        }
        PowerCPayload = json.dumps(PowerCData)

        #/----------------Power_Total--------------------/

        Power_Total_Data =  master.execute(mbId, cst.READ_INPUT_REGISTERS, 0x1136, 30)
        V_avg_Float_Data = [Power_Total_Data[0], Power_Total_Data[1]]
        I_avg_Float_Data = [Power_Total_Data[2], Power_Total_Data[3]]
        kW_tot_Float_Data = [Power_Total_Data[4], Power_Total_Data[5]]
        kvar_tot_Float_Data = [Power_Total_Data[6], Power_Total_Data[7]]
        kVA_tot_Float_Data = [Power_Total_Data[8], Power_Total_Data[9]]
        PF_tot_Float_Data = [Power_Total_Data[10], Power_Total_Data[11]]
        kWh_tot_Float_Data = [Power_Total_Data[12], Power_Total_Data[13]]
        kvarh_tot_Float_Data = [Power_Total_Data[14], Power_Total_Data[15]]
        kVAh_tot_Float_Data = [Power_Total_Data[16], Power_Total_Data[17]]
        Freq_a_Float_Data = [Power_Total_Data[18], Power_Total_Data[19]]
        Freq_b_Float_Data  = [Power_Total_Data[20], Power_Total_Data[21]]
        Freq_c_Float_Data = [Power_Total_Data[22], Power_Total_Data[23]]
        Freq_max_Float_Data = [Power_Total_Data[24], Power_Total_Data[25]]
        VTHD_value_Float_Data = [Power_Total_Data[26], Power_Total_Data[27]]
        ITHD_value_Float_Data = [Power_Total_Data[28], Power_Total_Data[29]]

        # convert to float:
        # IEEE754 ==> (Hi word Hi Bite, Hi word Lo Byte, Lo word Hi Byte, Lo word Lo Byte)
        V_avg_Float = unpack('>f', pack('>HH', V_avg_Float_Data[1], V_avg_Float_Data[0]))
        I_avg_Float = unpack('>f', pack('>HH', I_avg_Float_Data[1], I_avg_Float_Data[0]))
        kW_tot_Float = unpack('>f', pack('>HH', kW_tot_Float_Data[1], kW_tot_Float_Data[0]))
        kvar_tot_Float = unpack('>f', pack('>HH', kvar_tot_Float_Data[1], kvar_tot_Float_Data[0]))
        kVA_tot_Float = unpack('>f', pack('>HH', kVA_tot_Float_Data[1], kVA_tot_Float_Data[0]))
        PF_tot_Float = unpack('>f', pack('>HH', PF_tot_Float_Data[1], PF_tot_Float_Data[0]))
        kWh_tot_Float = unpack('>f', pack('>HH', kWh_tot_Float_Data[1], kWh_tot_Float_Data[0]))
        kvarh_tot_Float = unpack('>f', pack('>HH', kvarh_tot_Float_Data[1], kvarh_tot_Float_Data[0]))
        kVAh_tot_Float = unpack('>f', pack('>HH', kVAh_tot_Float_Data[1], kVAh_tot_Float_Data[0]))
        Freq_a_Float = unpack('>f', pack('>HH', Freq_a_Float_Data[1], Freq_a_Float_Data[0]))
        Freq_b_Float  = unpack('>f', pack('>HH', Freq_b_Float_Data[1], Freq_b_Float_Data[0]))
        Freq_c_Float = unpack('>f', pack('>HH', Freq_c_Float_Data[1], Freq_c_Float_Data[0]))
        Freq_max_Float = unpack('>f', pack('>HH', Freq_max_Float_Data[1], Freq_max_Float_Data[0]))
        VTHD_value_Float = unpack('>f', pack('>HH', VTHD_value_Float_Data[1], VTHD_value_Float_Data[0]))
        ITHD_value_Float = unpack('>f', pack('>HH', ITHD_value_Float_Data[1], ITHD_value_Float_Data[0]))

        PowerTotalData = {
            "V_avg_Float": V_avg_Float[0],
            "I_avg_Float": I_avg_Float[0],
            "kW_tot_Float": kW_tot_Float[0],
            "kvar_tot_Float": kvar_tot_Float[0],
            "kVA_tot_Float": kVA_tot_Float[0],
            "PF_tot_Float": PF_tot_Float[0],
            "kWh_tot_Float": kWh_tot_Float[0],
            "kvarh_tot_Float": kvarh_tot_Float[0],
            "kVAh_tot_Float": kVAh_tot_Float[0],
            "Freq_a_Float": Freq_a_Float[0],
            "Freq_b_Float": Freq_b_Float[0],
            "Freq_c_Float": Freq_c_Float[0],
            "VTHD_value_Float": VTHD_value_Float[0],
            "ITHD_value_Float": ITHD_value_Float[0],
        }
        PowerTotalPayload = json.dumps(PowerTotalData)

        PowerBoxGetData = {
            "IN_A": I_a_Float[0],
            "IN_B": I_b_Float[0],
            "IN_C": I_c_Float[0],
            "IN_Avg": I_avg_Float[0]
        }

        PowerBoxGetPayload = json.dumps(PowerBoxGetData)
    except Exception as e:
        print("[WARING] modbus test Error: " + str(e))



    def is_network_alive():
        global broker_address
        try:
            output = subprocess.check_output(["ping", "-c", "1", broker_address])
            match = re.search(r"rtt min/avg/max/mdev = (\d+\.\d+)/(\d+\.\d+)/(\d+\.\d+)/(\d+\.\d+) ms", output.decode("utf-8"))
            if match:
                min_rtt = match.group(1)
                avg_rtt = match.group(2)
                max_rtt = match.group(3)
                mdev_rtt = match.group(4)
                print(f"[INFO] Network Status: True (Ping {max_rtt} ms)")
            return True
        except:
            return False


    def is_mqtt_alive():
        if client.is_connected():
            return True
        else:
            return False


    def Publish_MQTT():
        time.sleep(1)
        client.publish("2706/PM3133/RelayValueData", RelayValuePayload)
        client.publish("2706/PM3133/SystemParameterSettingData", SystemParameterSettingPayload)
        client.publish("2706/PM3133/SystemInformationData", SystemInformationPayload)
        client.publish("2706/PM3133/PowerAData", PowerAPayload)
        client.publish("2706/PM3133/PowerBData", PowerBPayload)
        client.publish("2706/PM3133/PowerCData", PowerCPayload) 
        client.publish("2706/PM3133/PowerTotalData", PowerTotalPayload)
        client.publish("2706/PowerBox", PowerBoxGetPayload)


    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            pass
        else:
            print(f"[ERROR] Connection mqtt failed code: {rc}")

    client = mqtt.Client()
    client.connect(broker_address , ports, 300)  
    client.on_connect = on_connect
    client.loop_start()

    if is_network_alive() and is_mqtt_alive():
        print("[INFO] mqtt status: True")
        Publish_MQTT()
        print("[INFO] pubed_time: ", time.strftime("%H:%M:%S"))
    elif is_network_alive() and not is_mqtt_alive():
        print("[INFO] network status: True")
        print("[INFO] mqtt status: False")
        time.sleep(3)
    else:
        print("[INFO] network status: False")
        print("[INFO] mqtt status: False")
        time.sleep(3)
    client.loop_stop()
