| Scenario | Receiver fail | Worker fail | Events | Lost | Crashed | Recovered | Unrecoverable | Duplicates | Recovery rate | Median recovery | Max recovery |
|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 0% | 0% | 151 | 0 | 0 | 0 | 0 | 0 | n/a | - | - |
| outage | 20% | 0% | 151 | 28 | 0 | 28 | 0 | 0 | 100.0% | 213s | 382s |
| crashes | 0% | 30% | 151 | 0 | 43 | 43 | 0 | 0 | 100.0% | 180s | 360s |
| combined | 20% | 30% | 151 | 33 | 51 | 74 | 0 | 0 | 100.0% | 180s | 477s |
