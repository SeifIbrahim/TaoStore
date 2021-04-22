java -cp bin/TaoClient.jar:lib/* TaoClient.TaoClient --runType test --load_test_type multiclient --clients $1 --load_test_length $2 --rwRatio $3 --zipfExp $4 --warmup_operations $5
