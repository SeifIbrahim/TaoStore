# bin/CockroachTaoProxy.jar
javac -d bin/ -cp src/:lib/commons-math3-3.6.1.jar:lib/guava-19.0.jar:lib/postgresql-42.2.18.jar -sourcepath src/ src/TaoProxy/CockroachTaoProxy.java
(cd bin/ && jar cfe CockroachTaoProxy.jar TaoProxy.CockroachTaoProxy Configuration Messages TaoProxy -C ../src Configuration/TaoDefaultConfigs)

# bin/TaoClient.jar
javac -d bin/ -cp src/:lib/commons-math3-3.6.1.jar:lib/guava-19.0.jar:lib/postgresql-42.2.18.jar -sourcepath src/ src/TaoClient/TaoClient.java
(cd bin/ && jar cfe TaoClient.jar TaoClient.TaoClient Configuration Messages TaoClient TaoProxy -C ../src Configuration/TaoDefaultConfigs)
