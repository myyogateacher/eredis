
### Usage

```
Opts = [].

{ok, C} = eredis:start_link(Opts).
{ok, <<"OK">>} = eredis:q(C, ["SET", "foo", "bar"]).
{ok, <<"bar">>} = eredis:q(C, ["GET", "foo"]).
```

Unix:

```
{ok, C1} = eredis:start_link({local, "/var/run/redis.sock"}, 0).
```

For Other Redis commands:

```
https://github.com/wooga/eredis/blob/master/README.md
```


## Pool

Start redis pool

```
{erlpool, [
        {pools, [
            {pool_redis, [
                {size, 5},
                {start_mfa, {eredis, start_link, [[
                    {host, "hostname"},
                    {port, 6379},
                    {database, 0},
                    {password, "password"},
                    {socket_options, [
                        {keepalive, true}
                    ]},
                ]]}}
            ]}
        ]}
    ]}
}.
```

Start pool with sentinel

```
{erlpool, [
        {pools, [
            {pool_redis, [
                {size, 5},
                {start_mfa, {eredis, start_link, [[
                    {database, 0},
                    {password, "password"},
                    {socket_options, [
                        {keepalive, true}
                    ]},
                    {sentinels, ["10.0.0.2:26379","10.0.0.3:26379"]},
                    {sentinel_master_id, "mymaster"}
                ]]}}
            ]}
        ]}
    ]}
}.
```

