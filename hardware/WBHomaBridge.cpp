// TBD: uninit lib
// TBD: make keepalive configurable
// TBD: move mqtt loop timeout to a constant
// TBD: obtain version from git repository, too (during build)
// TBD: Mon Aug 25 00:44:30 2014 Error: zzz hardware (3) thread seems to have ended unexpectedly
//      (though the thread still works)
#include <ctime>
#include "stdafx.h"   
#include "DomoticzHardware.h"
#include "WBHomaBridge.h"
#include "../main/Logger.h"
#include "../main/SQLHelper.h"
#include "hardwaretypes.h"

class MQTTHandler: public mosqpp::mosquittopp
{
public:
    MQTTHandler(WBHomaBridge* bridge, std::string host, int port);
    void on_connect(int rc);
    void on_message(const struct mosquitto_message *message);
    void on_subscribe(int mid, int qos_count, const int *granted_qos);
private:
    WBHomaBridge* m_bridge;
    const int keepalive = 60;
};

MQTTHandler::MQTTHandler(WBHomaBridge* bridge, std::string host, int port)
    : m_bridge(bridge)
{
    connect(host.c_str(), port, keepalive);
}

void MQTTHandler::on_connect(int rc)
{
	_log.Log(LOG_STATUS, "WBHomaBridge: connected to MQTT broker with code %d", rc);
    if (rc == 0)
        // subscribe(0, "#");
        subscribe(0, "/devices/msu34tlp/controls/Pressure");
}

void MQTTHandler::on_message(const struct mosquitto_message *message)
{
    _log.Log(LOG_STATUS, "message topic: %s payload: %s", message->topic, message->payload);
    if (strcmp(message->topic, "/devices/msu34tlp/controls/Pressure"))
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

void WBHomaBridge::WriteToHardware(const char *pdata, const unsigned char length)
{
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

bool WBHomaBridge::DeviceExists()
{
	std::stringstream szQuery;
	std::vector<std::vector<std::string> > result;
	szQuery << "SELECT Name FROM DeviceStatus WHERE (HardwareID==" << m_HwdID << ") AND (DeviceID=='" << 1 << "') AND (Type==" << int(pTypeGeneral) << ") AND (Subtype==" << int(sTypePressure) << ")";
	result = m_sql.query(szQuery.str());
	return result.size() > 0;
}

void WBHomaBridge::UpdateDeviceName()
{
	std::stringstream szQuery;
    szQuery.clear();
    szQuery.str("");
    szQuery << "UPDATE DeviceStatus SET Name='" << "WBHomaBridgeValue" << "' WHERE (HardwareID==" << m_HwdID << ") AND (DeviceID=='" << 1 << "') AND (Type==" << int(pTypeGeneral) << ") AND (Subtype==" << int(sTypePressure) << ")";
    m_sql.query(szQuery.str());
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
		// sleep(1);
		// time_t t = time(0);

        // bool bDeviceExists = DeviceExists();

        // _tGeneralDevice gDevice;
        // gDevice.subtype = sTypePercentage;
        // gDevice.id = 1;
        // gDevice.floatval1 = t % 101;
        // gDevice.intval1 = 1; // device index
        // sDecodeRXMessage(this, (const unsigned char *)&gDevice);

        // if (!bDeviceExists)
        //     UpdateDeviceName();

		// if (ltime.tm_sec%POLL_INTERVAL==0)
		// {
		// 	FetchData();
		// }
		// if (ltime.tm_sec % 12 == 0) {
		// 	mytime(&m_LastHeartbeat);
		// }

	}

	_log.Log(LOG_STATUS,"WBHomaBridge: Stopped...");			
}

