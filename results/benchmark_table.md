| Framework | UDF mode | Runs | Min (s) | Median (s) | Max (s) | CPU peak % | CPU mean % | System memory peak (MB) |
|---|---|---|---|---|---|---|---|---|
| spark | python | 3 | 110.9 | 126.5 | 128.2 | 99 | 55 | 5128 |
| spark | pandas | 3 | 98.8 | 108.8 | 214.1 | 96 | 55 | 5340 |
| spark | native | 3 | 93.6 | 98.6 | 147.4 | 100 | 55 | 4938 |
| ray | python | 3 | 341.6 | 381.5 | 396.4 | 99 | 57 | 5664 |
| ray | pandas | 3 | 205.8 | 246.9 | 965.3 | 100 | 52 | 5705 |
| ray | native | 3 | 151.8 | 381.3 | 390.2 | 97 | 49 | 5660 |
