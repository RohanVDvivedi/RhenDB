gcc ./test_mvcc.c -o test_mvcc.out -lrondb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
gcc ./test_ttbl.c -o test_ttbl.out -lrondb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
gcc ./test_lckmgr.c -o test_lckmgr.out -lrondb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
