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

    std::string Topic() const
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
    class MQTTTypeHandler
    {
    public:
        MQTTTypeHandler(const char* type): m_type(type) {}
        virtual ~MQTTTypeHandler();
        bool Match(const std::string& type) const;
        bool Match(RBUF* rbuf) const;
        std::string AddressMapKey(const std::string& deviceID) const;
        virtual int DevType() const = 0;
        virtual int SubType() const = 0;
        virtual std::string GenerateDeviceID(int hardwareID) const;
        virtual int Encode(const std::string& value, RBUF* buf, const std::string& deviceID) const = 0;
        virtual bool Decode(RBUF* buf, std::string* deviceID, std::string* value) const;
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

    bool MQTTTypeHandler::Match(RBUF* buf) const
    {
        return buf->RAW.packettype == DevType() && buf->RAW.subtype == SubType();
    }

    std::string MQTTTypeHandler::AddressMapKey(const std::string& deviceID) const
    {
        std::stringstream s;
        s << deviceID << "/" << DevType() << "/" << SubType();
        return s.str();
    }

    std::string MQTTTypeHandler::GenerateDeviceID(int hardwareID) const
    {
        std::vector< std::vector<std::string> > result =
            m_sql.query("SELECT MAX(CAST(DeviceID AS INT)) + 1 FROM DeviceStatus "
                        "WHERE HardwareID = ? AND Type = ? AND SubType = ?",
                        SQLParamList() << hardwareID << DevType() << SubType());
        return result.empty() ? "1" : result[0][0];
    }

    bool MQTTTypeHandler::Decode(RBUF*, std::string*, std::string*) const
    {
        return false;
    }

    class MQTTTemperatureHandler: public MQTTTypeHandler
    {
    public:
        MQTTTemperatureHandler(): MQTTTypeHandler("temperature") {}

        int DevType() const { return pTypeTEMP; }
        int SubType() const { return sTypeTEMP1; }

        int Encode(const std::string& value, RBUF* buf, const std::string& deviceID) const;
    };

    int MQTTTemperatureHandler::Encode(const std::string& value, RBUF* buf,
                                       const std::string& deviceID) const
    {
        std::stringstream s(deviceID);
        int id;
        s >> id;

        std::stringstream s1(value);
        float v;
        s1 >> v;

        buf->TEMP.packetlength = sizeof(buf->TEMP) - 1;
        buf->TEMP.packettype = pTypeTEMP;
        buf->TEMP.subtype = SubType();
        buf->TEMP.battery_level = 9;
        buf->TEMP.rssi = 12;
        buf->TEMP.id1 = id >> 8;
        buf->TEMP.id2 = id & 255;
        buf->TEMP.tempsign = (v >= 0) ? 0 : 1;
        int vInt = round(abs(v * 10.0f));
        buf->TEMP.temperatureh = vInt >> 8;
        buf->TEMP.temperaturel = vInt & 255;

        return buf->TEMP.id2; // unit id
    }

    class MQTTSwitchHandler: public MQTTTypeHandler
    {
    public:
        MQTTSwitchHandler(): MQTTTypeHandler("switch") {}

        int DevType() const { return pTypeLighting5; }
        int SubType() const { return sTypeWBRelay; }

        std::string GenerateDeviceID(int hardwareID) const;
        int Encode(const std::string& value, RBUF* buf, const std::string& deviceID) const;
        bool Decode(RBUF* buf, std::string* deviceID, std::string* value) const;
    };

    std::string MQTTSwitchHandler::GenerateDeviceID(int hardwareID) const
    {
        std::vector< std::vector<std::string> > result =
            m_sql.query("SELECT MAX(DeviceID) FROM DeviceStatus "
                        "WHERE HardwareID = ? AND Type = ? AND SubType = ?",
                        SQLParamList() << hardwareID << DevType() << SubType());
        int id = 1;
        std::stringstream s;
        if (!result.empty()) {
            s << std::hex << result[0][0];
            s >> id;
            ++id;
        }

        s.clear();
        s.str("");
        s << std::uppercase << std::hex << std::setw(6) << std::setfill('0') << id;
        return s.str();
    }

    int MQTTSwitchHandler::Encode(const std::string& value, RBUF* buf,
                                  const std::string& deviceID) const
    {
        int id;
        std::stringstream s;
        s << std::hex << deviceID;
        s >> id;

        buf->LIGHTING5.id1 = (id >> 16) & 255;
        buf->LIGHTING5.id2 = (id >> 8) & 255;
        buf->LIGHTING5.id3 = id & 255;
        buf->LIGHTING5.packetlength = sizeof(buf->LIGHTING5) -1;
        buf->LIGHTING5.packettype = pTypeLighting5;
        buf->LIGHTING5.subtype = sTypeWBRelay;
        buf->LIGHTING5.seqnbr = 0; // FIXME

        buf->LIGHTING5.rssi = 7;
        buf->LIGHTING5.cmnd = (value == "1" ? light5_sOn : light5_sOff);
        buf->LIGHTING5.unitcode = (BYTE)id;

        return buf->LIGHTING5.unitcode;
    }

    bool MQTTSwitchHandler::Decode(RBUF* buf, std::string* deviceID, std::string* value) const
    {
        _log.Log(LOG_NORM,
                 "MQTTSwitchHandler(): id %02X%02X%02X, subType 0x%02x, unitcode %d, cmnd 0x%02x",
                 buf->LIGHTING5.id1,
                 buf->LIGHTING5.id2,
                 buf->LIGHTING5.id3,
                 buf->LIGHTING5.subtype,
                 buf->LIGHTING5.unitcode,
                 buf->LIGHTING5.cmnd);
        std::stringstream s;
        s << std::uppercase << std::hex << std::setw(6) << std::setfill('0') <<
            (((int)buf->LIGHTING5.id1 << 16) + ((int)buf->LIGHTING5.id2 << 8) + (int)buf->LIGHTING5.id3);
        // s << std::uppercase << std::hex << std::setw(2) << std::setfill('0') <<
        //     (int)buf->LIGHTING5.id1 << (int)buf->LIGHTING5.id2 << (int)buf->LIGHTING5.id3;
        _log.Log(LOG_NORM, "(1)id: %s", s.str().c_str());
        *deviceID = s.str();
        _log.Log(LOG_NORM, "(2)id: %s", deviceID->c_str());
        *value = buf->LIGHTING5.cmnd == light5_sOn ? "1" : "0";
        return true;
    }

    MQTTTypeHandler* type_handlers[] = {
        new MQTTTemperatureHandler(),
        new MQTTSwitchHandler(),
        0
    };

    template<class T> MQTTTypeHandler* GetMQTTTypeHandler(T thing)
    {
        for (MQTTTypeHandler** h = type_handlers; *h; ++h) {
            if ((*h)->Match(thing))
                return *h;
        }

        return 0;
    }

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
        void WriteControlValue(const MQTTAddress& address, const std::string& value);

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
        std::string topic = address.Topic() + "/meta/domoticz_device_id";
        publish(0, topic.c_str(), device_id.size(), device_id.c_str(), 0, true);
    }

    void MQTTHandler::WriteControlValue(const MQTTAddress& address, const std::string& value)
    {
        _log.Log(LOG_NORM, "WBHomaBridge::WriteControlValue(): %s <-- %s",
                 address.Topic().c_str(), value.c_str());
        std::string topic = address.Topic() + "/on";
        publish(0, topic.c_str(), value.size(), value.c_str());
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

void WBHomaBridge::WriteToHardware(const char *pdata, const unsigned char)
{
    RBUF* buf = (tRBUF*)pdata;
    _log.Log(LOG_NORM, "WBHomaBridge::WriteToHardware(): type 0x%02x, subType 0x%02x",
             buf->RAW.packettype, buf->RAW.subtype);

    MQTTTypeHandler* typeHandler = GetMQTTTypeHandler(buf);
    if (!typeHandler) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): couldn't decode the packet");
        return;
    }

    std::string deviceID, value;
    typeHandler->Decode(buf, &deviceID, &value);

    AddressMap::iterator it = d->m_addresses.find(typeHandler->AddressMapKey(deviceID));
    if (it == d->m_addresses.end()) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): unknown device id %s",
                 deviceID.c_str());
        return;
    }
    d->m_handler->WriteControlValue(it->second, value);
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
        break;
    }

    _log.Log(LOG_NORM, "WBHomaBridge: got message: "
             "sys %s ctrl %s payload %s payload_type %d ready %s",
             address.system_id.c_str(), address.device_control_id.c_str(),
             payload.c_str(), payload_type,
             value->ready() ? "true" : "false");

    if (d->m_ready && value->ready())
        SendValueToDomoticz(address, value);
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
            SendValueToDomoticz(it->first, &it->second);
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

void WBHomaBridge::SendValueToDomoticz(const MQTTAddress& address, MQTTValue* value)
{
    MQTTTypeHandler* typeHandler = GetMQTTTypeHandler(value->type);
    if (!typeHandler) {
        _log.Log(LOG_ERROR, "no matching type for MQTT/homA type %s", value->type.c_str());
        return;
    }

    if (value->devID.empty()) {
        value->devID = typeHandler->GenerateDeviceID(m_HwdID);
        d->m_handler->StoreDeviceID(address, value->devID);
    }
    d->m_addresses[typeHandler->AddressMapKey(value->devID)] = address;

    RBUF buf;
    memset(&buf, 0, sizeof(buf));
    typeHandler->Encode(value->value, &buf, value->devID);

    _log.Log(LOG_NORM, "WBHomaBridge: value to domoticz: hw id %d, devid '%s', value '%s'",
             m_HwdID, value->devID.c_str(), value->value.c_str());

    sDecodeRXMessage(this, (const unsigned char*)&buf);

    int r = m_sql.execute("UPDATE DeviceStatus SET Name = ? "
                          "WHERE HardwareID = ? AND DeviceID = ? AND "
                          "Type = ? AND SubType = ?",
                          SQLParamList() << address.device_control_id <<
                          m_HwdID << value->devID <<
                          typeHandler->DevType() << typeHandler->SubType());

    if (r != 1)
        _log.Log(LOG_ERROR, "WBHomaBridge: bad name update result for %s: %d",
                 address.device_control_id.c_str(), r);

    if (typeHandler->DevType() == pTypeLighting5)
        m_sql.execute("UPDATE DeviceStatus SET SwitchType = 0 "
                      "WHERE HardwareID = ? AND DeviceID = ? AND "
                      "Type = ? AND SubType = ?",
                      SQLParamList() << m_HwdID << value->devID <<
                      typeHandler->DevType() << typeHandler->SubType());
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
// TBD: show proper ID colum in 'Devices' table in the browser
// TBD: generate unit ids
// TBD: check packet length in Decode() methods (need to add arg)
// TBD: getlightswitches cmd: IsDimmer is important (depends on SwitchType field of DeviceStatus)
