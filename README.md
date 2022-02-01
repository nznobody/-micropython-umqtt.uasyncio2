# micropython-umqtt.uasyncio2
umqtt.uasyncio2 is an asyncio MQTT client for MicroPython. It is derived from [micropython-umqtt.simple2](https://github.com/fizista/micropython-umqtt.simple2) (and in future micropython-umqtt.robust2) and a lot of credit goes to the original author.

## Requirements
* Base asyncio library: https://github.com/micropython/micropython/tree/master/extmod/uasyncio

## Usage Example
```python
import uasyncio as asyncio

def on_message(*args, **kwargs):
    log.info("Async MQTT client received a message")
    ...

async def run():
    logging.info("Starting async MQTT client")
    ssl_params = ssl_params if ssl_params else {}
    client = MQTTClient("uasync2-client-id", "test.mosquitto.org", port=1883, user=user, password=password, keepalive=keepalive, ssl=bool(ssl_params), ssl_params=ssl_params)
    client.set_callback(on_message)
    try:
        await client.connect()
        log.info("Async MQTT client connected")
        await c.subscribe(b"test/qos1", qos=1)
        await c.subscribe(b"test/qos0", qos=0)
        log.info("Async MQTT client subscribed to topics")
        while True:
            await client.check_msg()
    except Exception as exc:
        log.error("MQTT exc: {}".format(str(exc)))
        raise exc
    finally:
        log.info("Shutting down async MQTT client")
        try:
            # This may fail due, best effort shutdown
            await client.disconnect()
        finally:
            del client

asyncio.run(run())
```

## Development State
This repository is a work-in-progress (WIP). As such there are still outstanding known and unknown bugs.

### Check issues and PRs on micropython-umqtt.simple2
As this repository is directly derived from micropython-umqtt.simple2 (referred to as *upstream*), it is worth checking their issues or PRs as this repo may diverge.

To assist with this, the `src` folder is initially `git subtree`'d from *upstream*.

## Micropython Hardware
This code has only been tested on Pycom hardware ([Fipy](https://docs.pycom.io/datasheets/development/fipy/)). Some specific fixes for this platform have been applied with regard to use of SSL.