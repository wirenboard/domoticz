#include <ctime>
#include <vector>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include "stdafx.h"
#include "DomoticzHardware.h"
#include "WBHomaBridge.h"
#include "../main/Logger.h"
#include "../main/SQLHelper.h"
#include "../main/Helper.h"
#include "../main/RFXtrx.h" // more hw types
#include "hardwaretypes.h"

class MQTTTypeHandler
{
public:
    MQTTTypeHandler(const char* type): m_type(type) {}
    virtual ~MQTTTypeHandler();
    virtual bool HandleValue(const std::string& inputValue,
                             int* out_type, int* out_subtype,
                             int* out_nValue, std::string* out_sValue) = 0;
    bool Match(const std::string& type) const;
private:
    const char* m_type;
};

MQTTTypeHandler::~MQTTTypeHandler()
{
    // NOOP
}

bool MQTTTypeHandler::Match(const std::string& type) const
{
    return m_type  == type;
}

class MQTTTemperatureHandler: public MQTTTypeHandler
{
public:
    MQTTTemperatureHandler(): MQTTTypeHandler("temperature") {}
    bool HandleValue(const std::string& inputValue,
                     int* out_type, int* out_subtype,
                     int* out_nValue, std::string* out_sValue);
};

bool MQTTTemperatureHandler::HandleValue(const std::string& inputValue,
                                         int* out_type, int* out_subtype,
                                         int* out_nValue, std::string* out_sValue)
{
    *out_type = pTypeTEMP;
    *out_subtype = sTypeTEMP1;
    *out_nValue = 0;
    *out_sValue = inputValue;
    return true;
}

namespace {
    MQTTTypeHandler* type_handlers[] = {
        new MQTTTemperatureHandler(),
        0
    };
}

class MQTTHandler: public mosqpp::mosquittopp
{
public:
    MQTTHandler(WBHomaBridge* bridge, std::string host, int port);
    void on_connect(int rc);
    void on_message(const struct mosquitto_message *message);
    void on_subscribe(int mid, int qos_count, const int *granted_qos);
private:
    WBHomaBridge* m_bridge;
    const int keepalive;
};

MQTTHandler::MQTTHandler(WBHomaBridge* bridge, std::string host, int port)
    : m_bridge(bridge), keepalive(60)
{
    connect(host.c_str(), port, keepalive);
}

void MQTTHandler::on_connect(int rc)
{
	_log.Log(LOG_STATUS, "WBHomaBridge: connected to MQTT broker with code %d", rc);
    if (rc == 0)
        subscribe(0, "#");
        // subscribe(0, "/devices/notebook/controls/Core0");
        // subscribe(0, "/devices/msu34tlp/controls/Pressure");
}

void MQTTHandler::on_message(const struct mosquitto_message *message)
{
    _log.Log(LOG_STATUS, "message topic: %s payload: %s", message->topic, message->payload);

    bool is_type = false;
    std::vector<std::string> parts;
    StringSplit(message->topic, "/", parts);
    if ((parts.size() != 4 && parts.size() != 6) ||
        parts[0] != "devices" ||
        parts[2] != "controls") {
        _log.Log(LOG_STATUS, "ignoring message with topic: %s", message->topic);
        return;
    }
    if (parts.size() == 6) {
        if (parts[4] != "meta" || parts[5] != "type") {
            _log.Log(LOG_STATUS, "ignoring message with topic: %s", message->topic);
            return;
        }
        is_type = true;
    }
    MQTTAddress address(parts[1], parts[3]);
    m_bridge->HandleMQTTMessage(address, is_type, (const char*)message->payload);

    // if (strcmp(message->topic, "/devices/msu34tlp/controls/Pressure"))
    //     return;

	// char buf[51];
    // memset(buf, 0, 51);
    // memcpy(buf, message->payload, 50);
    // double v = atof(buf);

    // _tGeneralDevice gDevice;
    // gDevice.subtype = sTypePressure;
    // gDevice.id = 1;
    // gDevice.floatval1 = v;
    // gDevice.intval1 = 1; // device index
    // m_bridge->sDecodeRXMessage(m_bridge, (const unsigned char *)&gDevice);
}

void MQTTHandler::on_subscribe(int, int, const int*)
{
    _log.Log(LOG_STATUS,"WBHomaBridge: MQTT subscription succeeded");
}

bool WBHomaBridge::s_libInitialized = false;

WBHomaBridge::WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort)
{
	m_HwdID = ID;
	m_stoprequested = false;
	m_szIPAddress = IPAddress;
	m_usIPPort = usIPPort;
}

WBHomaBridge::~WBHomaBridge()
{
    // NOOP
}

void WBHomaBridge::WriteToHardware(const char *, const unsigned char)
{
    // TBD
}

void WBHomaBridge::HandleMQTTMessage(const MQTTAddress& address, bool is_type, const std::string& payload)
{
    ValueMap::iterator it = m_values.find(address);
    if (it == m_values.end()) {
        m_values[address] = is_type ? MQTTValue("", payload) : MQTTValue(payload, "");
        return;
    }
    if (is_type)
        it->second.type = payload;
    else
        it->second.value = payload;
    _log.Log(LOG_NORM, "WBHomaBridge: got message: sys %s ctrl %s payload %s is_type %s ready %s",
             address.system_id.c_str(), address.device_control_id.c_str(),
             payload.c_str(), is_type ? "true" : "false",
             it->second.ready() ? "true" : "false");
    if (it->second.ready())
        WriteValueToDB(it->first, it->second);
}

bool WBHomaBridge::StartHardware()
{
    m_bIsStarted = true;
    if (!s_libInitialized) {
        mosqpp::lib_init();
        s_libInitialized = true;
    }
    m_thread = boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&WBHomaBridge::Do_Work, this)));
    return true;
}

bool WBHomaBridge::StopHardware()
{
	if (m_thread!=NULL)
	{
		m_stoprequested = true;
		m_thread->join();
	}

    m_bIsStarted = false;
    return true;
}

void WBHomaBridge::WriteValueToDB(const MQTTAddress& address, const MQTTValue& value)
{
    bool found = false;
    int devType, subType, nValue = 0;
    std::string sValue = "";
    for (MQTTTypeHandler** h = type_handlers; *h; ++h) {
        if ((*h)->Match(value.type)) {
            found = true;
            (*h)->HandleValue(value.value, &devType, &subType, &nValue, &sValue);
            break;
        }
    }
    if (!found) {
        _log.Log(LOG_ERROR, "no matching type for MQTT/homA type %s", value.type.c_str());
        return;
    }

    _log.Log(LOG_NORM, "WBHomaBridge: writing param: hw id %d, devid %s, nValue %d, sValue %s",
             m_HwdID, address.GetDeviceID().c_str(), nValue, sValue.c_str());
    std::string devname;
    m_sql.UpdateValue(m_HwdID, address.GetDeviceID().c_str(),
                      1, devType, subType, 12, 255, nValue, sValue.c_str(),
                      devname);
    int r = m_sql.execute("UPDATE DeviceStatus SET Name = ? "
                          "WHERE HardwareID = ? AND DeviceID = ? AND Name <> ?",
                          SQLParamList() << address.device_control_id << m_HwdID <<
                          address.GetDeviceID() << address.device_control_id);
    _log.Log(LOG_NORM, "WBHomaBridge: name update result: %d", r);
    return;
}

void WBHomaBridge::Do_Work()
{
	m_stoprequested=false;
    MQTTHandler handler(this, this->m_szIPAddress, this->m_usIPPort);
	while (!m_stoprequested)
	{
        int rc = handler.loop(100);
        if (rc)
            handler.reconnect();
	}

	_log.Log(LOG_STATUS,"WBHomaBridge: Stopped...");
}

// TBD: uninit lib
// TBD: make keepalive configurable
// TBD: move mqtt loop timeout to a constant
// TBD: obtain version from git repository, too (during build)
// TBD: make sure notifications work
// TBD: Mon Aug 25 00:44:30 2014 Error: zzz hardware (3) thread seems to have ended unexpectedly
//      (though the thread still works)
// TBD: rm SQL injection (DeviceStatus column Name, StrParam1/StrParam2, etc)
// TBD: receive types, rm DeviceStatus entries with mismatching types,
//      ignore params with unknown type values
// TBD: handle ranges
// TBD: validate numbers in value handlers
