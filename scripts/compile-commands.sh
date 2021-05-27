# bin/TaoServer.jar
javac -d bin/ -cp src/:lib/* -sourcepath src/ src/TaoServer/TaoServer.java
# use this on aws
#(cd bin/ && jar cfe TaoServer.jar TaoServer.TaoServer Configuration Messages TaoProxy TaoServer -C ../src Configuration/TaoDefaultConfigs)
# use this locally
(cd bin/ && jar cfe TaoServer.jar TaoServer.TaoServer Configuration Messages TaoProxy TaoServer)

# bin/TaoProxy.jar
javac -d bin/ -cp src/:lib/* -sourcepath src/ src/TaoProxy/TaoProxy.java
# use this on aws
#(cd bin/ && jar cfe TaoProxy.jar TaoProxy.TaoProxy Configuration Messages TaoProxy -C ../src Configuration/TaoDefaultConfigs)
# use this locally
(cd bin/ && jar cfe TaoProxy.jar TaoProxy.TaoProxy Configuration Messages TaoProxy)

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
