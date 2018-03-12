本项目用于简单测试rabbitmq集群的性能

测试脚本test.sh:
```shell
#!/bin/bash
port=5672
host='10.254.240.41,10.254.240.41,10.254.240.41'
msgPerP=600000
ex=2
pPerEx=1
qPerEx=10

testGroup(){
  mgroup=$1
  echo "-------------- GROUP $mgroup START ---------------"
  java -jar mq-benchmark.jar -host=${host} -port=${port} -ex=${ex} -qPerEx=${qPerEx} -pPerEx=${pPerEx} -mode=prepare -group=${mgroup} -msgPerP=${msgPerP}

  java -jar mq-benchmark.jar -host=${host} -port=${port} -ex=${ex} -qPerEx=${qPerEx} -pPerEx=${pPerEx} -mode=consumer -group=${mgroup} -msgPerP=${msgPerP} & \
  (sleep 5 && java -jar mq-benchmark.jar -host=${host} -port=${port} -ex=${ex} -qPerEx=${qPerEx} -pPerEx=${pPerEx} -mode=producer -group=${mgroup} -msgPerP=${msgPerP})

  echo "-------------- GROUP $mgroup END ---------------"

  sleep 3
}

testGroup 1
testGroup 2
testGroup 3
testGroup 4
```
