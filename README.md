# nginx-chash-statistic
```
Simulation of nginx chash,
get Upstreams and hashkeys from file,
then get every upstream hit count and hit rate.
```

# Usage
```
Usage of ./nginx-chash-statistic:
  -hashkeys-file string
        File format: per hashKey per line (default "hashkeys.list")
  -output-sort-key string
        Output Sort Key: ["server"|"hitCount"] (default "server")
  -upstreams-file string
        File format: per line per upstream, Line Format: "server_name,weight" (default "upstreams.list")
  -verbose
        Verbose output
  -version
        Show version then exit
```
