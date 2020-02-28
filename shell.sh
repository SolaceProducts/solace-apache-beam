mvn -e compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.SolaceRecordTest -Dexec.args="--output=DR100A --cip=192.168.56.102 --cu=default --sql=Q/fx-001" -Pdirect-runner

mvn -e compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--output=DR100A --cip=192.168.56.102 --cu=default --sql=Q/fx-001" -Pdirect-runner


mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args='--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/solace --runner=DataflowRunner --numWorkers=2 --maxNumWorkers=2 --cip=35.190.184.20 --cu=default --sql=Q/fx-001,Q/fx-002' -Pdataflow-runner

mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/solace --runner=DataflowRunner --numWorkers=2 --maxNumWorkers=2 --cip=35.190.184.20 --cu=default --sql=Q/fx-001,Q/fx-002 --auto" -Pdataflow-runner


mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/solace --runner=DataflowRunner --numWorkers=4 --maxNumWorkers=4 --cip=35.190.184.20 --cu=default --sql=Q/fx-001,Q/fx-002,Q/fx-003,Q/fx-004" -Pdataflow-runner

mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/solace --runner=DataflowRunner --numWorkers=4 --maxNumWorkers=4 --cip=35.190.184.20 --cu=default --sql=Q/fx-001,Q/fx-002,Q/fx-003,Q/fx-004 --auto" -Pdataflow-runner


./sdkperf_java.sh -cip=35.190.184.20 -pql="Q/fx-001,Q/fx-002,Q/fx-003,Q/fx-004,Q/fx-005,Q/fx-006,Q/fx-007,Q/fx-008" -pfl message.txt  -mr=80000 -mn=2400000

./sdkperf_java.sh -cip=35.190.184.20 -pql="Q/fx-001,Q/fx-002" -pfl message.txt  -mr=20 -mn=2400


mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/solace --runner=DataflowRunner --numWorkers=8 --maxNumWorkers=8 --cip=35.190.184.20 --cu=default --sql=Q/fx-001,Q/fx-002,Q/fx-003,Q/fx-004,Q/fx-005,Q/fx-006,Q/fx-007,Q/fx-008" -Pdataflow-runner

mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/test300k --runner=DataflowRunner --numWorkers=8 --maxNumWorkers=8 --cip=35.190.184.20 --cu=default --sql=Q/fx-001,Q/fx-002,Q/fx-003,Q/fx-004,Q/fx-005,Q/fx-006,Q/fx-007,Q/fx-008 --auto" -Pdataflow-runner


mvn compile exec:java -Dexec.mainClass=com.solace.apache.beam.examples.WindowedWordCountSolace -Dexec.args="--project=beam-test-218908 --gcpTempLocation=gs://solace-beam/tmp --stagingLocation=gs://solace-beam/staging/ --output=gs://solace-beam/output/test100 --runner=DataflowRunner --numWorkers=1 --maxNumWorkers=1 --cip=35.190.184.20 --cu=default --sql=Q/fx-001" -Pdataflow-runner