# ZMeter

Send the following metrics to given ZeroMQ socket

* cpu
* mem
* disk
* net
* load
* iostat
* process
* apache
* mysql
* tomcat

```python
    from zmeter import ZMeter
    zm = ZMeter('tcp://127.0.0.1:5555')
    # print zm.fetch('cpu')
    zm.send('cpu')
    zm.close()
```
