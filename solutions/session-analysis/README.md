There are two function in this project.

1. Sessionize log
2. Determine the average session time and top-k longest session


# Sessionize log

This phase solve:
1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics)
2. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

I apply Flink `Session window` to implement `time-based sliding window` for sessionizing input logs.
> The session windows assigner groups elements by sessions of activity. Session windows do not overlap and do not have a fixed start and end time, in contrast to tumbling windows and sliding windows. Instead a session window closes when it does not receive elements for a certain period of time, i.e., when a gap of inactivity occurred. A session window assigner can be configured with either a static session gap or with a session gap extractor function which defines how long the period of inactivity is. When this period expires, the current session closes and subsequent elements are assigned to a new session window.


Refer to: https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#session-windows

Type command as below
```
$ sbt "run --input LOG_PATH --output OUTPUT_DIRECTORY"
```

Example:
```
$ sbt "run --input ../../data/2015_07_22_mktplace_shop_web_log_sample.log --output ../../sessions"
```

Output Format: csv files

|  Column  |  Definition  |
| ---- | ---- |
| sessionID | ID of session, format=ip-start-end |
| IP | IP of session |
| transitionLength | Length of unique URL visits per session |
| DuringTime | Millionseconds time of session during |
| sessionStart | Timestamp of session start |
| sessionEnd | Timestamp of session end |

# 2. Determine the average session time and top-k longest session

This phase solve:
1. Determine the average session time
2. Find the most engaged users, ie the IPs with the longest session times

I use `Pandas` to calculate `average session time` and `top-k session time`

Type command as below
```aidl
# Find out average session time
$ python sbin/cal.py avg --input SESSION_DATA_PATH

# Example
$ python sbin/cal.py avg --input ../../sessions
Average session time: 673.96 sec.
```

```aidl
# Find out top-k session time
$ python sbin/cal.py top-k -k NUM --input SESSION_DATA_PATH

# Example
$ python sbin/cal.py top-k -k 10 --input ../../sessions
Top-k sessions:
                                              id              ip  transition   during          start            end
7329    125.19.44.66-1437528628000-1437530392000    125.19.44.66         321  1764000  1437528628000  1437530392000
7581   119.81.61.166-1437528628000-1437530392000   119.81.61.166         967  1764000  1437528628000  1437530392000
7663    52.74.219.71-1437528628000-1437530392000    52.74.219.71        5676  1764000  1437528628000  1437530392000
7254   106.186.23.95-1437528628000-1437530392000   106.186.23.95        1720  1764000  1437528628000  1437530392000
7538    59.144.58.37-1437528628000-1437530391000    59.144.58.37         127  1763000  1437528628000  1437530391000
7314   54.251.151.39-1437528630000-1437530392000   54.251.151.39           4  1762000  1437528630000  1437530392000
7488  180.211.69.209-1437528628000-1437530390000  180.211.69.209          44  1762000  1437528628000  1437530390000
7489    182.66.36.72-1437528628000-1437530390000    182.66.36.72           7  1762000  1437528628000  1437530390000
7548  103.29.159.186-1437528629000-1437530391000  103.29.159.186           2  1762000  1437528629000  1437530391000
7549   196.15.16.102-1437528629000-1437530391000   196.15.16.102          46  1762000  1437528629000  1437530391000
```