gcc ./test_mvcc.c -o test_mvcc.out -lrhendb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
gcc ./test_ttbl.c -o test_ttbl.out -lrhendb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
gcc ./test_lckmgr.c -o test_lckmgr.out -lrhendb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
gcc ./test_its.c -o test_its.out -lrhendb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
gcc ./test_dops.c -o test_dops.out -lrhendb -lmintxengine -lvolatilepagestore -ltupleindexer -ltuplestore -lbufferpool -lwale -lblockio -llockking -lboompar -lcutlery -lz
