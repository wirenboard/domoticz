#pragma once

#include <mosquittopp.h>
#include "DomoticzHardware.h"

class WBHomaBridge: public CDomoticzHardwareBase
{
public:
    WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort);
    ~WBHomaBridge();
	void WriteToHardware(const char *pdata, const unsigned char length);
private:
	bool StartHardware();
	bool StopHardware();
    bool DeviceExists();
    void UpdateDeviceName();
    void Do_Work();

	std::string m_szIPAddress;
	unsigned short m_usIPPort;
    bool m_stoprequested;
	boost::shared_ptr<boost::thread> m_thread;
    static bool s_libInitialized;
    friend class MQTTHandler;
};
