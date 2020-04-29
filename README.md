# ksql-udaf-checkpointsum
A UDAF for ksqlDB that computes the SUM of a stream of records, when TYPE is "delta". If a record with with a TYPE of "absolute" is found, it resets the sum to the absolute value.

Given a stream of events below, this function correctly outputs  values based on type. 
```$xslt
t1:{"account": "AAA", "security": "xyz", "type": "absolute", "amount": 10}
t2:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t3:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t4:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t5:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t6:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t7:{"account": "AAA", "security": "xyz", "type": "absolute", "amount": 20}
t8:{"account": "AAA", "security": "xyz", "type": "delta", "amount": 1}
t9:{"account": "AAA", "security": "zzz", "type": "absolute", "amount": 2}
t9:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}

```

at t3 above:
```$xslt
{"account": "AAA", "security": "xyz", "type": "state", "amount": 10}
{"account": "AAA", "security": "zzz", "type": "delta", "amount": 3}
```

at t9 above:
```$xslt
{"account": "AAA", "security": "xyz", "type": "state", "amount": 21}
{"account": "AAA", "security": "zzz", "type": "delta", "amount": 7}
```
