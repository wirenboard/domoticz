#ifndef WBHOMABRIDGE_H
#define WBHOMABRIDGE_H

#include "stdafx.h"
#include "DomoticzHardware.h"
#include "../main/RFXtrx.h" // more hw types

struct MQTTAddress;
struct MQTTTemporaryValue;
struct WBHomaBridgePrivate;

class WBHomaBridge: public CDomoticzHardwareBase
{
public:
    WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort);
    void WriteToHardware(const char *pdata, const unsigned char length);
    void HandleMQTTMessage(const MQTTAddress& address, int payload_type,
                           const std::string& payload);
    void Ready();
    void SendValueToDomoticz(RBUF* rbuf,
                             const std::string& device_id,
                             const std::string& name,
                             int switch_type);
private:
    bool StartHardware();
    bool StopHardware();
    void Do_Work();

    boost::shared_ptr<WBHomaBridgePrivate> d;
};

#endif

