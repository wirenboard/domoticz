#include <map>
#include <list>
#include <ctime>
#include <vector>
#include <memory>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/enable_shared_from_this.hpp>
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
    MQTTAddress(const std::string& system_id = "",
                const std::string& device_control_id = ""):
        SystemID(system_id), DeviceControlID(device_control_id) {}
    bool operator<(const MQTTAddress& other) const
    {
        return SystemID < other.SystemID ||
                          (SystemID == other.SystemID &&
                           DeviceControlID < other.DeviceControlID);
    }

    std::string Topic() const
    {
        return "/devices/" + SystemID + "/controls/" + DeviceControlID;
    }

    std::string SystemID;
    std::string DeviceControlID;
};

struct MQTTTemporaryValue
{
    MQTTTemporaryValue(const std::string device_id = "",
                       const std::string& value = "",
                       const std::string& type = ""):
        DeviceID(device_id), Value(value), Type(type) {}

    bool Ready() const {
        return !Value.empty() && !Type.empty();
    }

    std::string DeviceID;
    std::string Value;
    std::string Type;
};

namespace {
    class IDomoticzType
    {
    public:
        virtual ~IDomoticzType() {}
        virtual int DevType() const = 0;
        virtual int SubType() const = 0;
    };

    template<int dev_type, int subtype>
    class DomoticzType: virtual public IDomoticzType
    {
    public:
        static const int DEV_TYPE = dev_type;
        static const int SUB_TYPE = subtype;
        int DevType() const { return DEV_TYPE; }
        int SubType() const { return SUB_TYPE; }
    };

    class IMQTTValueWriter
    {
    public:
        virtual ~IMQTTValueWriter() {}
        virtual void PublishControlValue(const MQTTAddress& address, const std::string& value) = 0;
        virtual void StoreDeviceID(const MQTTAddress& address, const std::string& device_id) = 0;
    };

    class MQTTControl;

    class MQTTControlTypeBase: virtual public IDomoticzType
    {
    public:
        MQTTControlTypeBase(const std::string& type): m_type(type) {}
        bool Match(const std::string& type) const
        {
            return m_type  == type;
        }
        bool Match(RBUF* buf) const
        {
            return buf->RAW.packettype == DevType() && buf->RAW.subtype == SubType();
        }
        virtual MQTTControl* CreateControl() = 0;
        virtual std::string DomoticzAddressMapKey(RBUF*) { return std::string(); }
    private:
        std::string m_type;
    };

    template<typename T>
    class MQTTControlType: public MQTTControlTypeBase,
                           public DomoticzType<T::DEV_TYPE, T::SUB_TYPE>
    {
    public:
        MQTTControlType(const std::string& type): MQTTControlTypeBase(type) {}
        MQTTControl* CreateControl()
        {
            return new T();
        }
    };

    template<typename T>
    class TemperatureControlType: public MQTTControlType<T>
    {
    public:
        TemperatureControlType(const std::string& type): MQTTControlType<T>(type) {}
        std::string DomoticzAddressMapKey(RBUF* buf)
        {
            int id = (buf->TEMP.id1 << 8) + buf->TEMP.id2;
            std::stringstream s;
            s << id << "/" << (int)buf->LIGHTING5.packettype << "/" << (int)buf->LIGHTING5.subtype;
            return s.str();
        }
    };

    template<typename T>
    class Lighting5ControlType: public MQTTControlType<T>
    {
    public:
        Lighting5ControlType(const std::string& type): MQTTControlType<T>(type) {}
        std::string DomoticzAddressMapKey(RBUF* buf)
        {
            std::stringstream id;
            id << std::uppercase << std::hex << std::setw(6) << std::setfill('0') <<
                (((int)buf->LIGHTING5.id1 << 16) +
                 ((int)buf->LIGHTING5.id2 << 8) +
                 (int)buf->LIGHTING5.id3);
            std::stringstream s;
            s << id.str() << "/" << (int)buf->LIGHTING5.packettype << "/" <<
                (int)buf->LIGHTING5.subtype;
            return s.str();
        }
    };

    template<typename T>
    class PressureControlType: public MQTTControlType<T>
    {
    public:
        PressureControlType(const std::string& type): MQTTControlType<T>(type) {}
        std::string DomoticzAddressMapKey(RBUF* buf)
        {
            _tGeneralDevice* dev = reinterpret_cast<_tGeneralDevice*>(buf);
            std::stringstream id;
            id << std::uppercase << std::hex << std::setw(8) << std::setfill('0') <<
                dev->intval1;

            std::stringstream s;
            s << id.str() << "/" << (int)dev->type << "/" << (int)dev->subtype;
            return s.str();
        }
    };

    template<typename T>
    class LuxControlType: public MQTTControlType<T>
    {
    public:
        LuxControlType(const std::string& type): MQTTControlType<T>(type) {}
        std::string DomoticzAddressMapKey(RBUF* buf)
        {
            _tLightMeter* dev = reinterpret_cast<_tLightMeter*>(buf);
            std::stringstream id;
            id << std::uppercase << std::hex << 
                (int)dev->id1 <<
                std::setfill('0') << std::setw(2) <<
                (int)dev->id2 << (int)dev->id3 << (int)dev->id4;

            std::stringstream s;
            s << id.str() << "/" << (int)dev->type << "/" << (int)dev->subtype;
            return s.str();
        }
    };

    class MQTTControl: virtual public IDomoticzType,
                       public boost::enable_shared_from_this<MQTTControl>
    {
    public:
        void Setup(const MQTTAddress& address,
                   const std::string& device_id,
                   int hardwareID,
                   boost::shared_ptr<IMQTTValueWriter> writer);
        virtual ~MQTTControl() {}
        std::string DomoticzAddressMapKey() const;
        void EnsureDeviceID();
        const std::string& DeviceID() const { return m_DeviceID; }
        const MQTTAddress& Address() const { return m_Address; }
        virtual int SwitchType() const { return -1; }
        virtual int Priority() const { return 10; }
        virtual void MQTTToDomoticz(const std::string&, RBUF*) {}
        virtual void SlaveMQTTToDomoticz(boost::shared_ptr<MQTTControl>,
                                         const std::string&,
                                         RBUF*) {}
        virtual void DomoticzToMQTT(RBUF*) {}
        virtual void AddSlave(boost::shared_ptr<MQTTControl>) {}
        virtual bool HasDomoticzControl() const { return true; }
        void SetMaster(boost::shared_ptr<MQTTControl> master) { m_Master = master; }
        boost::shared_ptr<MQTTControl> Master() const { return m_Master; }
        void PublishValue(const std::string& value);
        boost::shared_ptr<MQTTControl> AcceptValue(const std::string& payload, RBUF* buf);

    protected:
        virtual std::string GenerateDeviceID(int hardwareID) const;

    private:
        MQTTAddress m_Address;
        std::string m_DeviceID;
        int m_HardwareID;
        boost::shared_ptr<IMQTTValueWriter> m_Writer;
        boost::shared_ptr<MQTTControl> m_Master;
    };

    void MQTTControl::Setup(const MQTTAddress& address,
                            const std::string& device_id,
                            int hardwareID,
                            boost::shared_ptr<IMQTTValueWriter> writer)
    {
        m_Address = address;
        m_DeviceID = device_id;
        m_HardwareID = hardwareID;
        m_Writer = writer;
    }

    std::string MQTTControl::DomoticzAddressMapKey() const
    {
        std::stringstream s;
        s << m_DeviceID << "/" << DevType() << "/" << SubType();
        return s.str();
    }

    void MQTTControl::EnsureDeviceID()
    {
        if (!m_DeviceID.empty())
            return;
        m_DeviceID = GenerateDeviceID(m_HardwareID);
        m_Writer->StoreDeviceID(m_Address, m_DeviceID);
    }

    std::string MQTTControl::GenerateDeviceID(int hardwareID) const
    {
        std::vector< std::vector<std::string> > result =
            m_sql.query("SELECT MAX(CAST(DeviceID AS INT)) + 1 FROM DeviceStatus "
                        "WHERE HardwareID = ? AND Type = ? AND SubType = ?",
                        SQLParamList() << hardwareID << DevType() << SubType());
        return result.empty() ? "1" : result[0][0];
    }

    void MQTTControl::PublishValue(const std::string& value)
    {
        m_Writer->PublishControlValue(m_Address, value);
    }

    boost::shared_ptr<MQTTControl> MQTTControl::AcceptValue(const std::string& payload, RBUF* buf)
    {
        if (Master()) {
            Master()->EnsureDeviceID();
            Master()->SlaveMQTTToDomoticz(shared_from_this(), payload, buf);
            return Master();
        } else if (HasDomoticzControl()) {
            EnsureDeviceID();
            MQTTToDomoticz(payload, buf);
        } else
            return boost::shared_ptr<MQTTControl>();
        return shared_from_this();
    }

    class MQTTHexIDControl: public MQTTControl {
    public:
        std::string GenerateDeviceID(int hardwareID) const;
    protected:
        virtual int IdDigits() const { return 8; };
    };

    std::string MQTTHexIDControl::GenerateDeviceID(int hardwareID) const
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
        s << std::uppercase << std::hex << std::setw(IdDigits()) << std::setfill('0') << id;
        _log.Log(LOG_NORM,
                 "GenerateDeviceID: %s for %s (IdDigits() %d)",
                 s.str().c_str(),
                 Address().Topic().c_str(),
                 IdDigits());
        return s.str();
    }

    class MQTTTemperatureControl: public MQTTControl,
                                  public DomoticzType<pTypeTEMP, sTypeTEMP1>
    {
    public:
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
    };

    void MQTTTemperatureControl::MQTTToDomoticz(const std::string& value, RBUF* buf)
    {
        std::stringstream s(DeviceID());
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
    }

    class MQTTPressureControl: public MQTTHexIDControl,
                               public DomoticzType<pTypeGeneral, sTypePressure>
    {
    public:
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
    };

    void MQTTPressureControl::MQTTToDomoticz(const std::string& value, RBUF* buf)
    {
        std::stringstream s(DeviceID());
        int id;
        s << std::hex;
        s >> id;

        std::stringstream s1(value);
        float v;
        s1 >> v;

        _tGeneralDevice *dev = new (buf) _tGeneralDevice();
        dev->subtype = SubType();
        dev->id = 0; // not actually used for sTypePressure
        dev->floatval1 = v * 0.00133322368; // mmhg -> bar
        dev->intval1 = id;
    }

    class MQTTLuxControl: public MQTTHexIDControl,
                          public DomoticzType<pTypeLux, sTypeLux>
    {
    public:
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
    protected:
        int IdDigits() const { return 7; }
    };

    void MQTTLuxControl::MQTTToDomoticz(const std::string& value, RBUF* buf)
    {
        std::stringstream s(DeviceID());
        int id;
        s << std::hex;
        s >> id;

        std::stringstream s1(value);
        float v;
        s1 >> v;

        _tLightMeter *dev = new (buf) _tLightMeter();
        dev->id1 = id >> 24;
        dev->id2 = (id >> 16) & 255;
        dev->id3 = (id >> 8) & 255;
        dev->id4 = id & 255;
        dev->dunit = (BYTE)id;
        dev->fLux = v;

        assert(buf->RAW.packettype == pTypeLux);
        assert(buf->RAW.subtype == sTypeLux);
    }

    class MQTTLightControlBase: public MQTTHexIDControl
    {
    public:
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
    protected:
        int IdDigits() const { return 6; }
    };

    void MQTTLightControlBase::MQTTToDomoticz(const std::string&, RBUF* buf)
    {
        int id;
        std::stringstream s;
        s << std::hex << DeviceID();
        s >> id;

        buf->LIGHTING5.id1 = (id >> 16) & 255;
        buf->LIGHTING5.id2 = (id >> 8) & 255;
        buf->LIGHTING5.id3 = id & 255;
        buf->LIGHTING5.packetlength = sizeof(buf->LIGHTING5) - 1;
        buf->LIGHTING5.packettype = pTypeLighting5;
        buf->LIGHTING5.subtype = SubType();
        buf->LIGHTING5.seqnbr = 0; // FIXME

        buf->LIGHTING5.rssi = 7;
        buf->LIGHTING5.unitcode = (BYTE)id;
    }

    class MQTTSwitchControl: public MQTTLightControlBase,
                             public DomoticzType<pTypeLighting5, sTypeWBRelay>
    {
    public:
        int SwitchType() const { return STYPE_OnOff; }
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
        void DomoticzToMQTT(RBUF* buf);
    };

    void MQTTSwitchControl::MQTTToDomoticz(const std::string& value, RBUF* buf)
    {
        MQTTLightControlBase::MQTTToDomoticz(value, buf);
        buf->LIGHTING5.cmnd = (value == "1" ? light5_sOn : light5_sOff);
    }

    void MQTTSwitchControl::DomoticzToMQTT(RBUF* buf)
    {
        _log.Log(LOG_NORM,
                 "MQTTSwitchControl(): id %02X%02X%02X, subType 0x%02x, unitcode %d, cmnd 0x%02x",
                 buf->LIGHTING5.id1,
                 buf->LIGHTING5.id2,
                 buf->LIGHTING5.id3,
                 buf->LIGHTING5.subtype,
                 buf->LIGHTING5.unitcode,
                 buf->LIGHTING5.cmnd);
        switch(buf->LIGHTING5.cmnd) {
        case light5_sOff:
            PublishValue("0");
            break;
        case light5_sOn:
            PublishValue("1");
            break;
        default:
            _log.Log(LOG_ERROR, "MQTTSwitchControl(): bad lighting5 command 0x%02x",
                     buf->LIGHTING5.cmnd);
        }
    }

    class MQTTRGBControl: public MQTTLightControlBase,
                          public DomoticzType<pTypeLighting5, sTypeWBRGB>
    {
    public:
        int SwitchType() const { return STYPE_Dimmer; }
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
        void SlaveMQTTToDomoticz(boost::shared_ptr<MQTTControl> slave,
                                 const std::string& value,
                                 RBUF* buf);
        void DomoticzToMQTT(RBUF* buf);
        void AddSlave(boost::shared_ptr<MQTTControl> slave)
        {
            m_Slave = slave;
            m_Slave->SetMaster(shared_from_this());
        }

    private:
        std::string GetRGB(int hue) const;
        boost::shared_ptr<MQTTControl> m_Slave;
        int m_Level;
    };
    
    void MQTTRGBControl::MQTTToDomoticz(const std::string& value, RBUF* buf)
    {
        MQTTLightControlBase::MQTTToDomoticz(value, buf);
        _log.Log(LOG_NORM, "MQTTRGBControl::MQTTToDomoticz(): '%s': value '%s' (TBD)",
                 Address().Topic().c_str(), value.c_str());
        buf->LIGHTING5.cmnd = m_Level ? light5_sSetLevel : light5_sRGBoff;
        buf->LIGHTING5.level = m_Level;
    }

    void MQTTRGBControl::SlaveMQTTToDomoticz(boost::shared_ptr<MQTTControl> slave,
                                             const std::string& value,
                                             RBUF* buf)
    {
        _log.Log(LOG_NORM, "MQTTRGBControl::SlaveMQTTToDomoticz(): '%s': value '%s'",
                 Address().Topic().c_str(), value.c_str());
        assert(slave == m_Slave);
        MQTTLightControlBase::MQTTToDomoticz(value, buf);
        std::stringstream s(value);
        s >> m_Level;
        buf->LIGHTING5.cmnd = m_Level ? light5_sSetLevel : light5_sRGBoff;
        buf->LIGHTING5.level = m_Level;
    }

    void MQTTRGBControl::DomoticzToMQTT(RBUF* buf)
    {
        // Handles light5_sRGBcolormin+1+x, light5_sRGBoff, light5_sSetLevel
        // hue: when (light5_sRGBcolormin+1) is subtracted, the remaining value
        // is in range 0 <= x <= 78, corresponding to 0 <= x <= 360 value of the
        // original control.
        std::stringstream s;
        switch (buf->LIGHTING5.cmnd) {
        case light5_sRGBoff:
            if (m_Slave)
                m_Slave->PublishValue("0");
            break;
        case light5_sSetLevel:
            _log.Log(LOG_NORM, "MQTTRGBControl::DomoticzToMQTT(): set level to %d",
                     (int)buf->LIGHTING5.level);
            if (m_Slave) {
                s << (int)buf->LIGHTING5.level;
                m_Slave->PublishValue(s.str());
            }
            break;
        default:
            if (buf->LIGHTING5.cmnd < light5_sRGBcolormin) {
                _log.Log(LOG_ERROR, "MQTTRGBControl::DomoticzToMQTT(): unknown command 0x%02x",
                         buf->LIGHTING5.cmnd, s.str().c_str());
                return;
            }
            int hue = round((buf->LIGHTING5.cmnd - light5_sRGBcolormin - 1.) * 360 / 78.);
            PublishValue(GetRGB(hue));
        }
    }
    
    std::string MQTTRGBControl::GetRGB(int hue) const
    {
        int x = round(255. * (double)(hue % 60) / 60.);
        if(hue == 360)
            hue = 0;
        int r, g, b;
        if (hue < 60) {
            r = 255;
            g = x;
            b = 0;
        } else if (hue < 120) {
            r = 255 - x;
            g = 255;
            b = 0;
        } else if (hue < 180) {
            r = 0;
            g = 255;
            b = x;
        } else if (hue < 240) {
            r = 0;
            g = 255 - x;
            b = 255;
        } else if (hue < 300) {
            r = x;
            g = 0;
            b = 255;
        } else if (hue < 360) {
            r = 255;
            g = 0;
            b = 255 - x;
        } else {
            r = 0;
            g = 0;
            b = 0;
        }

        std::stringstream s;
        s << r << ";" << g << ";" << b;
        return s.str();
    }

    class MQTTDimmerControl: public MQTTLightControlBase,
                             public DomoticzType<pTypeLighting5, sTypeWBDimmer>
    {
    public:
        int SwitchType() const { return STYPE_Dimmer; }
        void MQTTToDomoticz(const std::string& value, RBUF* buf);
        void DomoticzToMQTT(RBUF* buf);
    };

    void MQTTDimmerControl::MQTTToDomoticz(const std::string& value, RBUF* buf)
    {
        MQTTLightControlBase::MQTTToDomoticz(value, buf);
        _log.Log(LOG_NORM, "MQTTRGBControl::MQTTToDomoticz(): '%s': value '%s'",
                 Address().Topic().c_str(), value.c_str());
        std::stringstream s(value);
        int level;
        s >> level;
        // FIXME: don't hardcode max value (255), use max from mqtt
        level = (int)round((double)level * 100. / 255.);
        buf->LIGHTING5.cmnd = level ? light5_sSetLevel : light5_sRGBoff;
        buf->LIGHTING5.level = level;
    }

    void MQTTDimmerControl::DomoticzToMQTT(RBUF* buf)
    {
        std::stringstream s;
        switch (buf->LIGHTING5.cmnd) {
        case light5_sRGBoff:
            PublishValue("0");
            break;
        case light5_sSetLevel:
            _log.Log(LOG_NORM, "MQTTDimmerControl::DomoticzToMQTT(): set level to %d",
                     (int)buf->LIGHTING5.level);
            // FIXME: don't hardcode max value (255), use max from mqtt
            s << (int)round((double)buf->LIGHTING5.level * 255. / 100.);
            PublishValue(s.str());
            break;
        default:
            _log.Log(LOG_ERROR, "MQTTDimmerControl::DomoticzToMQTT(): unknown command 0x%02x",
                     buf->LIGHTING5.cmnd, s.str().c_str());
        }
    }

    // slave-only, used for RGB dimmer 'RGB_All' value
    class MQTTRangeControl: public MQTTControl,
                            public DomoticzType<-1, -1>
    {
    public:
        bool HasDomoticzControl() const { return false; }
    };

    MQTTControlTypeBase* control_types[] = {
        new TemperatureControlType<MQTTTemperatureControl>("temperature"),
        new PressureControlType<MQTTPressureControl>("pressure"),
        new LuxControlType<MQTTLuxControl>("lux"),
        new Lighting5ControlType<MQTTSwitchControl>("switch"),
        new Lighting5ControlType<MQTTRGBControl>("rgb"),
        new Lighting5ControlType<MQTTDimmerControl>("dimmer"),
        new MQTTControlType<MQTTRangeControl>("range"), // needed for RGB_All
        0
    };

    struct MQTTControlLink
    {
        MQTTControlLink(const std::string& master_name, const std::string& slave_name)
            : MasterName(master_name), SlaveName(slave_name) {}
        std::string MasterName;
        std::string SlaveName;
    };

    MQTTControlLink* control_links[] = {
        new MQTTControlLink("RGB", "RGB_All"),
        0
    };

    template<class T> MQTTControlTypeBase* GetMQTTControlType(T thing)
    {
        for (MQTTControlTypeBase** ct = control_types; *ct; ++ct) {
            if ((*ct)->Match(thing))
                return *ct;
        }

        return 0;
    }

    class MQTTHandler: public mosqpp::mosquittopp, public IMQTTValueWriter
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
        void PublishControlValue(const MQTTAddress& address, const std::string& value);

    private:
        WBHomaBridge* m_bridge;
        std::string m_ReadyTopic;
        int m_keepalive;
    };

    MQTTHandler::MQTTHandler(WBHomaBridge* bridge, std::string host, int port)
        : m_bridge(bridge), m_keepalive(60)
    {
        char buf[64];
        sprintf(buf, "/tmp/%ld-%d-%d/retain_hack", time(NULL), rand(), rand());
        m_ReadyTopic = buf;
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
        subscribe(0, m_ReadyTopic.c_str());
        publish(0, m_ReadyTopic.c_str(), 1, "1");
    }

    void MQTTHandler::on_message(const struct mosquitto_message *message)
    {
        _log.Log(LOG_STATUS, "message topic: %s payload: %s", message->topic, message->payload);

        if (message->topic == m_ReadyTopic) {
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

    void MQTTHandler::PublishControlValue(const MQTTAddress& address, const std::string& value)
    {
        std::string topic = address.Topic() + "/on";
        _log.Log(LOG_NORM, "MQTTHandler::PublishControlValue(): %s <-- %s",
                 topic.c_str(), value.c_str());
        publish(0, topic.c_str(), value.size(), value.c_str());
    }

    typedef std::map<MQTTAddress, MQTTTemporaryValue> TemporaryValueMap;
    typedef std::map<MQTTAddress, boost::shared_ptr<MQTTControl> > MQTTAddressMap;
    typedef std::map<std::string, boost::shared_ptr<MQTTControl> > DomoticzAddressMap;
}

struct WBHomaBridgePrivate
{
    CDomoticzHardwareBase* m_Hardware;
    bool m_Ready;
    boost::shared_ptr<MQTTHandler> m_Handler;
    std::string m_IPAddress;
    unsigned short m_Port;
    bool m_StopRequested;
    boost::shared_ptr<boost::thread> m_Thread;
    TemporaryValueMap m_TemporaryValueMap;
    MQTTAddressMap m_MQTTAddressMap;
    DomoticzAddressMap m_DomoticzAddressMap;
    static bool s_LibInitialized;

    boost::shared_ptr<MQTTControl> AddControl(
        const MQTTAddress& address, const MQTTTemporaryValue& value)
    {
        _log.Log(LOG_NORM, "WBHomaBridgePrivate::AddControl(): '%s'",
                 address.Topic().c_str());
        MQTTControlTypeBase* type = GetMQTTControlType(value.Type);
        if (!type) {
            _log.Log(LOG_NORM, "NOTE: skipping control of type '%s' at topic '%s'",
                     value.Type.c_str(), address.Topic().c_str());
            return boost::shared_ptr<MQTTControl>();
        }

        assert(!m_MQTTAddressMap.count(address));
        boost::shared_ptr<MQTTControl> control(type->CreateControl());
        control->Setup(address, value.DeviceID, m_Hardware->m_HwdID, m_Handler);
        m_MQTTAddressMap[address] = control;
        MaybeLinkControl(control);

        return control;
    }

    void MaybeLinkControl(boost::shared_ptr<MQTTControl> control)
    {
        boost::shared_ptr<MQTTControl> master, slave;   
        std::string name = control->Address().DeviceControlID;
        std::string other_name;
        for (MQTTControlLink** l = control_links; *l; ++l) {
            if ((*l)->MasterName == name) {
                master = control;
                other_name = (*l)->SlaveName;
                break;
            } else if ((*l)->SlaveName == name) {
                slave = control;
                other_name = (*l)->MasterName;
                break;
            }
        }

        if (other_name.empty())
            return;

        MQTTAddress other_address(control->Address().SystemID, other_name);
        MQTTAddressMap::iterator it = m_MQTTAddressMap.find(other_address);
        if (it == m_MQTTAddressMap.end()) {
            _log.Log(LOG_NORM,
                     "WBHomaBridge: '%s' is a pending %s",
                     (master ? master : slave)->Address().Topic().c_str(),
                     (master ? "master" : "slave"));
            return;
        }

        assert(master || slave);
        if (master)
            slave = it->second;
        else
            master = it->second;

        _log.Log(LOG_NORM,
                 "WBHomaBridge: adding '%s' as a slave for '%s'",
                 slave->Address().Topic().c_str(),
                 master->Address().Topic().c_str());

        master->AddSlave(slave);
        // Slave controls have direct MQTT mapping but don't have
        // any direct Domoticz mapping. From Domoticz point of view,
        // they're part of their master control.
        if (!slave->HasDomoticzControl() && !slave->DeviceID().empty())
            m_DomoticzAddressMap.erase(slave->DomoticzAddressMapKey());
    }

    void SendValueToDomoticz(boost::shared_ptr<MQTTControl> control,
                             const std::string& payload)
    {
        RBUF buf;
        memset(&buf, 0, sizeof(buf));
        control = control->AcceptValue(payload, &buf);
        if (!control)
            return;

        m_DomoticzAddressMap[control->DomoticzAddressMapKey()] = control;

        m_Hardware->sDecodeRXMessage(m_Hardware, (const unsigned char*)&buf);
        int r = m_sql.execute("UPDATE DeviceStatus SET Name = ? "
                "WHERE HardwareID = ? AND DeviceID = ? AND "
                "Type = ? AND SubType = ?",
                SQLParamList() << control->Address().DeviceControlID <<
                m_Hardware->m_HwdID << control->DeviceID() <<
                control->DevType() << control->SubType());

        if (r != 1) {
            _log.Log(LOG_ERROR, "WBHomaBridge: bad name update result for %s: %d",
                     control->Address().DeviceControlID.c_str(), r);
            abort();
        }

        if (control->SwitchType() >= 0) {
            r = m_sql.execute("UPDATE DeviceStatus SET SwitchType = ? "
                    "WHERE HardwareID = ? AND DeviceID = ? AND "
                    "Type = ? AND SubType = ?",
                    SQLParamList() << control->SwitchType() <<
                    m_Hardware->m_HwdID << control->DeviceID() <<
                    control->DevType() << control->SubType());
            if (r != 1) {
                _log.Log(LOG_ERROR, "WBHomaBridge: bad switch type update result for %s: %d",
                         control->Address().DeviceControlID.c_str(), r);
                abort();
            }
        }
    }

};

bool WBHomaBridgePrivate::s_LibInitialized = false;

WBHomaBridge::WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort)
    : d(new WBHomaBridgePrivate)
{
	m_HwdID = ID;
    d->m_Hardware = this;
    d->m_Ready = false;
	d->m_StopRequested = false;
	d->m_IPAddress = IPAddress;
	d->m_Port = usIPPort;
}

void WBHomaBridge::WriteToHardware(const char *pdata, const unsigned char)
{
    RBUF* buf = (tRBUF*)pdata;
    _log.Log(LOG_NORM, "WBHomaBridge::WriteToHardware(): type 0x%02x, subType 0x%02x",
             buf->RAW.packettype, buf->RAW.subtype);
    MQTTControlTypeBase* type = GetMQTTControlType(buf);
    if (!type) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): couldn't decode the packet");
        return;
    }

    std::string domoticz_address_map_key = type->DomoticzAddressMapKey(buf);
    if (domoticz_address_map_key.empty()) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): couldn't locate the control");
        return;
    }

    DomoticzAddressMap::iterator it = d->m_DomoticzAddressMap.find(domoticz_address_map_key);
    if (it == d->m_DomoticzAddressMap.end()) {
        _log.Log(LOG_ERROR, "WBHomaBridge::WriteToHardware(): unknown address map key '%s'",
                 domoticz_address_map_key.c_str());
        return;
    }

    it->second->DomoticzToMQTT(buf);
}

void WBHomaBridge::HandleMQTTMessage(const MQTTAddress& address, int payload_type,
                                     const std::string& payload)
{
    if (d->m_Ready) {
        MQTTAddressMap::iterator it = d->m_MQTTAddressMap.find(address);
        if (it != d->m_MQTTAddressMap.end()) {
            if (payload_type == MQTTHandler::Value)
                d->SendValueToDomoticz(it->second, payload);
            return;
        }
    }

    if (!d->m_TemporaryValueMap.count(address))
        d->m_TemporaryValueMap[address] = MQTTTemporaryValue();

    MQTTTemporaryValue& value = d->m_TemporaryValueMap[address];
    switch (payload_type) {
    case MQTTHandler::Type:
        value.Type = payload;
        break;
    case MQTTHandler::Value:
        value.Value = payload;
        break;
    case MQTTHandler::DeviceID:
        value.DeviceID = payload;
        break;
    }

    _log.Log(LOG_NORM, "WBHomaBridge: got message: "
             "sys %s ctrl %s payload %s payload_type %d ready %s",
             address.SystemID.c_str(), address.DeviceControlID.c_str(),
             payload.c_str(), payload_type,
             value.Ready() ? "true" : "false");

    if (d->m_Ready && value.Ready()) {
        boost::shared_ptr<MQTTControl> control = d->AddControl(address, value);
        if (!control) {
            _log.Log(LOG_NORM, "WBHomaBridge::Ready(): value ready, but no control for type '%s' -- '%s'",
                     value.Type.c_str(), address.Topic().c_str());
            return;
        }
        _log.Log(LOG_NORM, "WBHomaBridge::HandleMQTTMessage(): value ready -- '%s'",
                 address.Topic().c_str());
        d->SendValueToDomoticz(control, value.Value);
        d->m_TemporaryValueMap.erase(address);
    }

    _log.Log(LOG_NORM, "WBHomaBridge::HandleMQTTMessage(): number of temp values: %d",
             d->m_TemporaryValueMap.size());
}

void WBHomaBridge::Ready()
{
    _log.Log(LOG_NORM, "WBHomaBridge::Ready()");
    if (d->m_Ready)
        return;

    _log.Log(LOG_NORM, "WBHomaBridge::Ready(): number of temp values: %d",
             d->m_TemporaryValueMap.size());

    d->m_Ready = true;
    std::list<MQTTAddress> to_erase;
    for (TemporaryValueMap::iterator it = d->m_TemporaryValueMap.begin(); it != d->m_TemporaryValueMap.end(); ++it) {
        if (it->second.Ready()) {
            boost::shared_ptr<MQTTControl> control = d->AddControl(it->first, it->second);
            if (!control) {
                _log.Log(LOG_NORM, "WBHomaBridge::Ready(): value ready, but no control for type '%s' -- '%s'",
                         it->second.Type.c_str(), it->first.Topic().c_str());
                continue;
            }
            _log.Log(LOG_NORM, "WBHomaBridge::Ready(): value ready -- '%s'",
                     it->first.Topic().c_str());
            d->SendValueToDomoticz(control, it->second.Value);
            to_erase.push_back(it->first);
        } else
            _log.Log(LOG_NORM, "WBHomaBridge::Ready(): value not ready yet -- '%s'",
                     it->first.Topic().c_str());
    }

    for (std::list<MQTTAddress>::iterator it = to_erase.begin(); it != to_erase.end(); ++it)
        d->m_TemporaryValueMap.erase(*it);
}

bool WBHomaBridge::StartHardware()
{
    m_bIsStarted = true;
    if (!d->s_LibInitialized) {
        mosqpp::lib_init();
        d->s_LibInitialized = true;
    }
    d->m_Thread = boost::shared_ptr<boost::thread>(
        new boost::thread(boost::bind(&WBHomaBridge::Do_Work, this)));
    return true;
}

bool WBHomaBridge::StopHardware()
{
	if (d->m_Thread != NULL)
	{
		d->m_StopRequested = true;
		d->m_Thread->join();
	}

    m_bIsStarted = false;
    return true;
}

void WBHomaBridge::Do_Work()
{
	d->m_StopRequested = false;
    d->m_Handler = boost::shared_ptr<MQTTHandler>(
        new MQTTHandler(this, d->m_IPAddress, d->m_Port));

	while (!d->m_StopRequested)
	{
        int rc = d->m_Handler->loop(100);
        if (rc)
            d->m_Handler->reconnect();
        mytime(&m_LastHeartbeat);
	}

	_log.Log(LOG_STATUS,"WBHomaBridge: Stopped...");
}

// TBD: make keepalive configurable
// TBD: make mqtt loop timeout a constant
// TBD: obtain version from git repository, too (during build)
// TBD: make sure notifications work
// TBD: handle deletion of controls
//      (when type becomes empty or unrecognized, remote DeviceStatus entry)
// TBD: check packet length in DomoticzToMQTT methods
// TBD: don't use hardcoded min/max values for dimmer/rgb_dimmer types
