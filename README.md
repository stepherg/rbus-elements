# rbus-elements

Lightweight helper service that registers RBUS data model elements, tables, events and methods from a JSON description plus a small set of built‑in DeviceInfo properties.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

## Run

```bash
./build/rbus_elements            # uses elements.json in source dir
./build/rbus_elements custom.json
```

## JSON Schema (informal)

Array of objects:

- name: Fully qualified RBUS element path (properties, tables, events, methods)
- elementType: property|table|event|method (default property)
- type: numeric ValueType enum (properties only)
- value: initial value (properties only; row properties become initial row values if name contains concrete table instances)

Tables are inferred from property names containing concrete indices; wildcard table/property definitions with `{i}` are synthesized automatically.

## Methods

- Device.Reboot(Delay) -> Status
- Device.GetSystemInfo() -> SerialNumber,SystemTime,UpTime
- Device.Telemetry.Collect(msg_type,source,dest) -> status
- SetPSMRecordValue() stores each input property by its property name
- GetPSMRecordValue() returns stored values using the requested property names

PSM records are held for the lifetime of the process. The WebConfig RFC enable
record is initialized to `true` for simulated-device startup.

## Notes

The service exits cleanly on SIGINT/SIGTERM/SIGHUP/SIGQUIT.

## License

See `LICENSE`.
