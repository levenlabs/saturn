# Saturn

Saturn is a tiny go client that uses UDP to watch time across servers. A single
saturn instance should be the master instance and all other instances should
be passed a `--master-addr` with the address for the master. All communication
is done over UDP on the `--listen-addr`. BOTH masters and slaves need to have
the UDP port open.

Since each instance operates over UDP a shared key is used to "verify" traffic
the secret should be the same across all nodes and is passed via `--hmac-key`.

Time offset is determined using a transactional system. The slave sends its
current time to the master. The master replies with its time and the offset.
The slave then respondes one more time with its time and the original offset.
This process can be repeated n (where n is just 1 for now) times to acheieve
greater accuracy. The master determines the offset of that client by taking
half the slave's RTT for the first reply/response and then taking half the
time between the last response sent and recieved.

## Example

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
Half 1st RTT = 3 / n = 1.5
Half 2nd RTT = 4 / n = 2
Avg Half RTT = (2 + 1.5) / n = 1.75

Half Offsets = (-6 + -7) / n = -6.5

Slave offset = -6.5 + 1.75 = -4.75

* You'll notice this is .75 seconds off, hopefully with more realistic transit
times and a larger value of n, this will be more accurate. *
```


## ProtoBuf

The communication is done with protobufs and you can find the protobuf files
in the `proto` file.

To generate new go files based on the proto files run:
```
protoc --go_out=. proto/report.proto proto/response.proto
```

## Todo

* Finish actual sync algorithm
* Run 1000s of samples to figure out best algorithm
* Actually start reporting off servers
* Add configurable n
