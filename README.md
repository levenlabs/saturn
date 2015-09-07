# Saturn

Saturn is a tiny go client that uses UDP to watch time across servers. A single
saturn instance should be the master instance and all other instances should
be passed a `--master-addr` with the address for the master. All communication
is done over UDP on the `--listen-addr`. BOTH masters and slaves need to have
the UDP port open. The default port is `4123`.

Since each instance operates over UDP a shared key is used to "verify" traffic
the secret should be the same across all nodes and is passed via `--hmac-key`.

Time offset is determined using a multi-round system. The slave sends its
current time to the master. The master replies with its time and the offset.
The slave then respondes one more time with its time and the original offset.
This process can be repeated n times to achieve greater accuracy. The master
takes the 80th percentile of the RTTs when calculating the offset of the slave
in order to account for anomalies in Internet networks.

## Rounds

The number of back-and-forth offset calculations can be passed with `--rounds`
and defaults to 5. With 5 rounds, I've noticed less than 100 millisecond
variance. If you're just trying to make sure a server isn't 3 seconds off, this
is perfectly fine.

Note: The `--rounds` count only needs to be configured on the master.

## Threshold

When a slave is over the threshold passed with `--threshold` a warning will
be printed to stdout:

    ~ WARN -- slave offset is over threshold -- ip="10.42.0.9" offset="-4.984391125000001"

This follows the format of [llog](https://github.com/LevenLabs/go-llog)
messages. There will be more configurable options in the future.

## ProtoBuf

The communication is done with protobufs and you can find the protobuf files
in the `proto` file.

To generate new go files based on the proto files run:
```
protoc --go_out=. proto/report.proto proto/response.proto
```

## Example Offset Calculation

```
- Slave is 4 seconds behind Master -
- n = 2 -

Slave       Packet Transit Time         Master
----------------------------------------------
Time 20   -------- 2 seconds -------->
                                       Time 26
          <-------- 1 second --------- Offset -6
Time 23
Offset -3 -------- 3 seconds -------->
                                       Time 30
                                       Offset -7
First RTT = |-6 - -3| = 3
Second RTT = |30 - 26| = 4
Total RTTs = 7
Total Offsets = (-6 + -7) = -13

Slave offset = (-13 + 7) / 3 = -5

* You'll notice this is 1 second off, but keep in mind this was done
 with seconds and not with milliseconds. *
```

## Todo

* Instead of comparing every server to the master we should compare the servers
to the other servers
* POST a message when over threshold instead of just logging
