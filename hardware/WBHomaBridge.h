#ifndef WBHOMABRIDGE_H
#define WBHOMABRIDGE_H

#include "stdafx.h"
#include <map>
#include <mosquittopp.h>
#include "DomoticzHardware.h"

struct MQTTAddress
{
    MQTTAddress(const std::string& _system_id = "",
                const std::string& _device_control_id = ""):
        system_id(_system_id), device_control_id(_device_control_id) {}
    bool operator<(const MQTTAddress& other) const
    {
        return system_id < other.system_id ||
            (system_id == other.system_id &&
             device_control_id < other.device_control_id);
    }
    std::string GetDeviceID() const {
        return system_id + "/" + device_control_id;
    }

    std::string system_id;
    std::string device_control_id;
};

struct MQTTValue
{
    MQTTValue(const std::string& _value = "", const std::string& _type = ""):
        value(_value), type(_type) {}
    bool ready() const {
        return !value.empty() && !type.empty();
    }

    std::string value;
    std::string type;
};

typedef std::map<MQTTAddress, MQTTValue> ValueMap;

class WBHomaBridge: public CDomoticzHardwareBase
{
public:
    WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort);
    ~WBHomaBridge();
    void WriteToHardware(const char *pdata, const unsigned char length);
    void HandleMQTTMessage(const MQTTAddress& address, bool is_type, const std::string& payload);
private:
    bool StartHardware();
    bool StopHardware();
    void WriteValueToDB(const MQTTAddress& address, const MQTTValue& value);
    void Do_Work();

    std::string m_szIPAddress;
    unsigned short m_usIPPort;
    bool m_stoprequested;
    boost::shared_ptr<boost::thread> m_thread;
    ValueMap m_values;
    static bool s_libInitialized;
};

#endif

