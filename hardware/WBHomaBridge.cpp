#include <map>
#include <ctime>
#include <vector>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <mosquittopp.h>
#include "stdafx.h"
#include "DomoticzHardware.h"
#include "WBHomaBridge.h"
#include "../main/Logger.h"
#include "../main/SQLHelper.h"
#include "../main/Helper.h"
#include "../main/RFXtrx.h" // more hw types
#include "../main/localtime_r.h"
#include "hardwaretypes.h"

#ifdef WIN32
	#include <comdef.h>
#elif defined __linux__
	#include <sys/time.h>
#endif

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

    std::string topic() const
    {
        return "/devices/" + system_id + "/controls/" + device_control_id;
    }

    std::string system_id;
    std::string device_control_id;
};

struct MQTTValue
{
    MQTTValue(const std::string devID = "",
              const std::string& _value = "",
              const std::string& _type = ""):
        devID(devID), value(_value), type(_type) {}
    bool ready() const {
        return !value.empty() && !type.empty();
    }

    std::string devID;
    std::string value;
    std::string type;
};

typedef std::map<MQTTAddress, MQTTValue> ValueMap;
typedef std::map<std::string, MQTTAddress> AddressMap;

namespace {
    const int HighIDStart = 420000;

    class MQTTTypeHandler
    {
    public:
        MQTTTypeHandler(const char* type): m_type(type) {}
        virtual ~MQTTTypeHandler();
        virtual int devType() const = 0;
        virtual int subType() const = 0;
        virtual bool HandleValue(const std::string& inputValue,
                                 int* out_nValue, std::string* out_sValue) const = 0;
        virtual std::string GenerateDeviceID(int hardwareID) const;
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

    std::string MQTTTypeHandler::GenerateDeviceID(int hardwareID) const
    {
        std::vector< std::vector<std::string> > result =
            m_sql.query("SELECT MAX(CAST(DeviceID AS INT)) + 1 FROM DeviceStatus "
                        "WHERE HardwareID = ?", SQLParamList() << hardwareID);

        if (result.empty()) {
            std::stringstream s;
            s << HighIDStart;
            return s.str();
        }

        return result[0][0];
    }

    class MQTTTemperatureHandler: public MQTTTypeHandler
    {
    public:
        MQTTTemperatureHandler(): MQTTTypeHandler("temperature") {}

        int devType() const { return pTypeTEMP; }
        int subType() const { return sTypeTEMP1; }

        bool HandleValue(const std::string& inputValue,
                         int* out_nValue, std::string* out_sValue) const;
    };

    bool MQTTTemperatureHandler::HandleValue(const std::string& inputValue,
                         int* out_nValue, std::string* out_sValue) const {
        *out_nValue = 0;
        *out_sValue = inputValue;
        return true;
    }

    class MQTTSwitchHandler: public MQTTTypeHandler
    {
    public:
        MQTTSwitchHandler(): MQTTTypeHandler("switch") {}

        int devType() const { return pTypeLighting1; }
        int subType() const { return sTypeX10; }

        bool HandleValue(const std::string& inputValue,
                         int* out_nValue, std::string* out_sValue) const;
        virtual std::string GenerateDeviceID(int hardwareID) const;
    };

    bool MQTTSwitchHandler::HandleValue(const std::string& inputValue,
                     int* out_nValue, std::string* out_sValue) const
    {
        *out_nValue = inputValue == "1" ? light1_sOn : light1_sOff;
        *out_sValue = "";
        return true;
    }

    std::string MQTTSwitchHandler::GenerateDeviceID(int hardwareID) const
    {
        // must use id < 255 for the switch to work
        std::vector< std::vector<std::string> > result =
            m_sql.query("SELECT MAX(CAST(DeviceID AS INT)) + 1 FROM DeviceStatus "
                        "WHERE HardwareID = ? AND CAST(DeviceID AS INT) < ?",
                        SQLParamList() << hardwareID << HighIDStart);
        return result.empty() ? "1" : result[0][0];
    }

    MQTTTypeHandler* type_handlers[] = {
        new MQTTTemperatureHandler(),
        new MQTTSwitchHandler(),
        0
    };

    class MQTTHandler: public mosqpp::mosquittopp
    {
    public:
        enum PayloadType {
            Type,
            Value,
            DeviceID
        };
        MQTTHandler(WBHomaBridge* bridge, std::string host, int port);
        virtual ~MQTTHandler();
        void on_connect(int rc);
        void on_message(const struct mosquitto_message *message);
        void on_subscribe(int mid, int qos_count, const int *granted_qos);
        void StoreDeviceID(const MQTTAddress& address, const std::string& device_id);
        void ToggleSwitch(const MQTTAddress& address, bool on);

    private:
        WBHomaBridge* m_bridge;
        std::string m_readyTopic;
        int m_keepalive;
    };

    MQTTHandler::MQTTHandler(WBHomaBridge* bridge, std::string host, int port)
        : m_bridge(bridge), m_keepalive(60)
    {
        char buf[64];
        sprintf(buf, "/tmp/%ld-%d-%d/retain_hack", time(NULL), rand(), rand());
        m_readyTopic = buf;
        connect(host.c_str(), port, m_keepalive);
    }

    MQTTHandler::~MQTTHandler()
    {
        // NOOP
    }

    void MQTTHandler::on_connect(int rc)
    {
        _log.Log(LOG_STATUS, "WBHomaBridge: connected to MQTT broker with code %d", rc);
        if (rc == 0)
            subscribe(0, "#");
        subscribe(0, m_readyTopic.c_str());
        publish(0, m_readyTopic.c_str(), 1, "1");
    }

    void MQTTHandler::on_message(const struct mosquitto_message *message)
    {
        _log.Log(LOG_STATUS, "message topic: %s payload: %s", message->topic, message->payload);

#if 0
        // that's how to do it via DecodeRXMessage(),
        // but that would require more elaborate DeviceID generation
        if (strcmp(message->topic, "/devices/notebook/controls/Core0"))
            return;

        char buf[51];
        memset(buf, 0, 51);
        memcpy(buf, message->payload, 50);
        double v = atof(buf);

        _tGeneralDevice gDevice;
        gDevice.subtype = sTypePressure;
        gDevice.id = 1;
        gDevice.floatval1 = v;
        gDevice.intval1 = 1; // device index
        m_bridge->sDecodeRXMessage(m_bridge, (const unsigned char *)&gDevice);

        return;
#endif
        if (message->topic == m_readyTopic) {
            // at that point, retained control -> domoticz DeviceID
            // mappings are recieved
            m_bridge->Ready();
            return;
        }

        int payload_type = Value;
        std::vector<std::string> parts;
        StringSplit(message->topic, "/", parts);

        if ((parts.size() != 4 && parts.size() != 6) ||
            parts[0] != "devices" ||
            parts[2] != "controls") {
            _log.Log(LOG_STATUS, "ignoring message with topic: %s", message->topic);
            return;
        }

        if (parts.size() == 6) {
            if (parts[4] != "meta") {
                _log.Log(LOG_STATUS, "ignoring message with topic: %s", message->topic);
                return;
            }
            if (parts[5] == "type")
                payload_type = Type;
            else if (parts[5] == "domoticz_device_id")
                payload_type = DeviceID;
            else {
                _log.Log(LOG_STATUS, "ignoring message with topic: %s", message->topic);
                return;
            }
        }

        MQTTAddress address(parts[1], parts[3]);
        m_bridge->HandleMQTTMessage(address, payload_type, (const char*)message->payload);

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

    void MQTTHandler::StoreDeviceID(const MQTTAddress& address, const std::string& device_id)
    {
        std::string topic = address.topic() + "/meta/domoticz_device_id";
        publish(0, topic.c_str(), device_id.size(), device_id.c_str(), 0, true);
    }

    void MQTTHandler::ToggleSwitch(const MQTTAddress& address, bool on)
    {
        std::string topic = address.topic() + "/on";
        publish(0, topic.c_str(), 1, on ? "1" : "0");
    }
}

struct WBHomaBridgePrivate
{
    bool m_ready;
    MQTTHandler* m_handler;
    std::string m_szIPAddress;
    unsigned short m_usIPPort;
    bool m_stoprequested;
    boost::shared_ptr<boost::thread> m_thread;
    ValueMap m_values;
    AddressMap m_addresses;
    static bool s_libInitialized;
};

bool WBHomaBridgePrivate::s_libInitialized = false;

WBHomaBridge::WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort)
    : d(new WBHomaBridgePrivate)
{
	m_HwdID = ID;
    d->m_ready = false;
    d->m_handler = 0;
	d->m_stoprequested = false;
	d->m_szIPAddress = IPAddress;
	d->m_usIPPort = usIPPort;
}

WBHomaBridge::~WBHomaBridge()
{
    if (d->m_handler)
        delete d->m_handler;
    delete d;
}

void WBHomaBridge::WriteToHardware(const char *pdata, const unsigned char length)
{
    if (length < 2 || pdata[1] != pTypeLighting1) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): skipping bad packet");
        return;
    }

    tRBUF* rbuf = (tRBUF*)pdata;
    _log.Log(LOG_NORM, "WBHomaBridge::WriteToHardware(): subType 0x%02x, housecode %d, "
             "unitcode %d, cmnd 0x%02x",
             rbuf->LIGHTING1.subtype,
             rbuf->LIGHTING1.housecode,
             rbuf->LIGHTING1.unitcode,
             rbuf->LIGHTING1.cmnd);

    std::stringstream s;
    s << (int)rbuf->LIGHTING1.housecode;
    AddressMap::iterator it = d->m_addresses.find(s.str());
    if (it == d->m_addresses.end()) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): unknown device id %s",
                 s.str().c_str());
        return;
    }
    bool on = rbuf->LIGHTING1.cmnd == light1_sOn;
    _log.Log(LOG_NORM, "ToggleSwitch: %s %s", it->second.topic().c_str(), on ? "on" : "off");
    d->m_handler->ToggleSwitch(it->second, on);
}

void WBHomaBridge::HandleMQTTMessage(const MQTTAddress& address, int payload_type,
                                     const std::string& payload)
{
    MQTTValue* value;
    ValueMap::iterator it = d->m_values.find(address);
    if (it == d->m_values.end())
        d->m_values[address] = MQTTValue();
    // it = d->m_values.insert(ValueMap::value_type(address, MQTTValue())).first;

    value = &d->m_values[address];
    switch (payload_type) {
    case MQTTHandler::Type:
        value->type = payload;
        break;
    case MQTTHandler::Value:
        value->value = payload;
        break;
    case MQTTHandler::DeviceID:
        value->devID = payload;
        d->m_addresses[payload] = address;
        break;
    }

    _log.Log(LOG_NORM, "WBHomaBridge: got message: "
             "sys %s ctrl %s payload %s payload_type %d ready %s",
             address.system_id.c_str(), address.device_control_id.c_str(),
             payload.c_str(), payload_type,
             value->ready() ? "true" : "false");

    if (d->m_ready && value->ready())
        WriteValueToDB(address, value);
}

void WBHomaBridge::Ready()
{
    _log.Log(LOG_NORM, "WBHomaBridge::Ready()");
    if (d->m_ready)
        return;
    d->m_ready = true;
    for (ValueMap::iterator it = d->m_values.begin(); it != d->m_values.end(); ++it) {
        if (it->second.ready()) {
            _log.Log(LOG_NORM, "ready0");
            WriteValueToDB(it->first, &it->second);
            _log.Log(LOG_NORM, "ready1");
        }
    }
}

bool WBHomaBridge::StartHardware()
{
    m_bIsStarted = true;
    if (!d->s_libInitialized) {
        mosqpp::lib_init();
        d->s_libInitialized = true;
    }
    d->m_thread = boost::shared_ptr<boost::thread>(
        new boost::thread(boost::bind(&WBHomaBridge::Do_Work, this)));
    return true;
}

bool WBHomaBridge::StopHardware()
{
	if (d->m_thread != NULL)
	{
		d->m_stoprequested = true;
		d->m_thread->join();
	}

    m_bIsStarted = false;
    return true;
}

void WBHomaBridge::WriteValueToDB(const MQTTAddress& address, MQTTValue* value)
{
    MQTTTypeHandler* typeHandler = 0;
    int nValue = 0;
    std::string sValue = "";
    for (MQTTTypeHandler** h = type_handlers; *h; ++h) {
        if ((*h)->Match(value->type)) {
            typeHandler = *h;
            break;
        }
    }
    if (!typeHandler) {
        _log.Log(LOG_ERROR, "no matching type for MQTT/homA type %s", value->type.c_str());
        return;
    }
    typeHandler->HandleValue(value->value, &nValue, &sValue);

    if (value->devID.empty()) {
        value->devID = typeHandler->GenerateDeviceID(m_HwdID);
        d->m_handler->StoreDeviceID(address, value->devID);
        d->m_addresses[value->devID] = address;
    }

    _log.Log(LOG_NORM, "WBHomaBridge: writing param: hw id %d, devid '%s', nValue %d, sValue '%s'",
             m_HwdID, value->devID.c_str(), nValue, sValue.c_str());

    std::string devname = "";
    m_sql.UpdateValue(m_HwdID, value->devID.c_str(),
                      1, typeHandler->devType(), typeHandler->subType(),
                      12, 255, nValue, sValue.c_str(),
                      devname);
    int r = m_sql.execute("UPDATE DeviceStatus SET Name = ? "
                          "WHERE HardwareID = ? AND DeviceID = ?",
                          SQLParamList() << address.device_control_id <<
                          m_HwdID << value->devID);
    _log.Log(LOG_NORM, "WBHomaBridge: name update result: %d", r);
    if (typeHandler->devType() == pTypeLighting1)
        m_sql.execute("UPDATE DeviceStatus SET SwitchType = 0 "
                      "WHERE HardwareID = ? AND DeviceID = ?",
                      SQLParamList() << m_HwdID << value->devID);
    return;
}

void WBHomaBridge::Do_Work()
{
	d->m_stoprequested = false;
    d->m_handler = new MQTTHandler(this, d->m_szIPAddress, d->m_usIPPort);
	while (!d->m_stoprequested)
	{
        int rc = d->m_handler->loop(100);
        if (rc)
            d->m_handler->reconnect();
        mytime(&m_LastHeartbeat);
	}

	_log.Log(LOG_STATUS,"WBHomaBridge: Stopped...");
}

// TBD: uninit lib
// TBD: make keepalive configurable
// TBD: make mqtt loop timeout a constant
// TBD: obtain version from git repository, too (during build)
// TBD: make sure notifications work
// TBD: receive types, rm DeviceStatus entries with mismatching types,
//      ignore params with unknown type values
// TBD: validate numbers in value handlers
// TBD: light color / level
// TBD: custom lighting subtype
// TBD: make mosquitto an optional dependency
//      (don't build WBHomaBridge if it's not found)
// TBD: handle deletion of controls
//      (when type becomes empty or unrecognized, remote DeviceStatus entry)
// TBD: use type other than pTypeLighting1 in order to avoid 255 switch limit
// TBD: show proper ID colum in 'Devices' table in the browser
