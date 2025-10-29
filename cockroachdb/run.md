cd /local/jepsen/cockroachdb && \
lein run test -n node1 \
  --username Alephant \
  --ssh-private-key ~/.ssh/id_rsa \
  --test register \
  --nemesis parts \
  --concurrency 10


cd /local/jepsen/cockroachdb && timeout 600 lein run test \
  -n node1 -n node2 -n node3 -n node4 -n node5 \
  --username Alephant \
  --ssh-private-key ~/.ssh/id_rsa \
  --test register \
  --nemesis parts \
  --time-limit 60 \
  --concurrency 10 2>&1 | tee /tmp/jepsen-5node-test.log | grep -E "INFO.*jepsen|ERROR|Everything|Analysis|valid|Cut off|Healing"



lein run test \
  -n node1 -n node2 -n node3 -n node4 -n node5 \
  --username Alephant \
  --ssh-private-key ~/.ssh/id_rsa \
  --test sequential \
  --nemesis none \
  --time-limit 60 \
  --concurrency 15


lein run test \
  -n node1 -n node2 -n node3 -n node4 -n node5 \
  --username Alephant \
  --ssh-private-key ~/.ssh/id_rsa \
  --test minimal \
  --nemesis none \

##  timestamp inversion pitfall 
```
lein run test --test timestamp-inversion --nemesis none

lein run test -n node1 -n node2 -n node3 \
  --username Alephant \
  --ssh-private-key ~/.ssh/id_rsa \
  --test timestamp-inversion \
  --nemesis none \
  --concurrency 10
```


## install dependency
sudo apt-get install -y openjdk-8-jdk
sudo update-alternatives --list java
sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java


sudo apt-get install -y leiningen