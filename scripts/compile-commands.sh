# bin/CockroachTaoProxy.jar
javac -d bin/ -cp src/:lib/* -sourcepath src/ src/TaoProxy/CockroachTaoProxy.java
# use this on aws
#(cd bin/ && jar cfe CockroachTaoProxy.jar TaoProxy.CockroachTaoProxy Configuration Messages TaoProxy -C ../src Configuration/TaoDefaultConfigs)
# use this locally
(cd bin/ && jar cfe CockroachTaoProxy.jar TaoProxy.CockroachTaoProxy Configuration Messages TaoProxy)

# bin/TaoClient.jar
javac -d bin/ -cp src/:lib/* -sourcepath src/ src/TaoClient/TaoClient.java
# use this on aws
#(cd bin/ && jar cfe TaoClient.jar TaoClient.TaoClient Configuration Messages TaoClient TaoProxy -C ../src Configuration/TaoDefaultConfigs)
# use this locally
(cd bin/ && jar cfe TaoClient.jar TaoClient.TaoClient Configuration Messages TaoClient TaoProxy)
