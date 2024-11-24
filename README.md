# twc-poller
Tesla Wall Connecter Poller. Stores readings in redis stream

create a config.yml file in the root of the project with the following content:

```
Global: 
  api_prefix: /api/1/
  poll_interval: 1
  random_offset_max: 0.1
  default_endpoints:
    - vitals
    - lifetime
    - version
    - wifi_status

Redis:
  host: localhost
  port: 6379
  db: 0
  stream: twc_stream

WallConnectors: 
  - address: hostname.com:80
  - address: ip:80
```
