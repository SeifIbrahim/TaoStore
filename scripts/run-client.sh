java -cp bin/TaoClient.jar:lib/* TaoClient.TaoClient --runType test --load_test_type multiclient --clients $1 --load_size $2 --rwRatio $3 --zipfExp $4
