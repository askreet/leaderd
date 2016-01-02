# leaderd

Super simple leader election using DynamoDB, designed with auto-scaling groups
in mind.

## Build

This project uses `gb` to build.

```
go get -u github.com/constabulary/go
gb vendor restore
gb build
bin/leaderd -table ... -name ...
```

## Use

Create a DynamoDB table with a single String key, `LockName`.

On each node, run `leaderd` at boot with a unique node identifier. When using
an auto-scaling group, use the instance ID:

```
/usr/local/bin/leaderd -table leaderd -name i-abc23918
```

You can automatically create the table with CloudFormation and use a systemd
script to read the table name and use the instance ID as a node name. (TODO: example)

## Options

### interval (default: 10)

Number of seconds to wait between checks of the current leader (when the current
node is a follow), or the number of seconds between updates when in a leader
role.

### timeout (default: 60)

Amount of time at which the current leader is considered "expired" and the node
will attempt to steal leadership.

## Configuring AWS settings

AWS settings such as `REGION` should be set with instance variables supported by
the AWS SDK for Go.
