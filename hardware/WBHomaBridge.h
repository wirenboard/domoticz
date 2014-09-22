#ifndef WBHOMABRIDGE_H
#define WBHOMABRIDGE_H

#include "stdafx.h"
#include "DomoticzHardware.h"

struct MQTTAddress;
struct MQTTValue;
struct WBHomaBridgePrivate;

class WBHomaBridge: public CDomoticzHardwareBase
{
public:
    WBHomaBridge(const int ID, const std::string IPAddress, const unsigned short usIPPort);
    ~WBHomaBridge();
    void WriteToHardware(const char *pdata, const unsigned char length);
    void HandleMQTTMessage(const MQTTAddress& address, int payload_type,
                           const std::string& payload);
    void Ready();
private:
    bool StartHardware();
    bool StopHardware();
    std::string GenerateDeviceID() const;
    void WriteValueToDB(const MQTTAddress& address, MQTTValue* value);
    void Do_Work();

    WBHomaBridgePrivate* d;
};

#endif

