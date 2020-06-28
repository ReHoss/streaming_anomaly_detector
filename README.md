# Web traffic anomaly detection app !
Class project: flink anomaly detector. Fraudulent pattern detection on web traffic.


## File descriptions :

anomalydetector: folder containing the final Flink app ! Please delete files presents in anomalydetector/src/main/data/ before launching the app because the app will create the output files in this directory. The abnormal events will be written in this directory (hence the directory must be empty before launching the job).

data: contains data used for the offline anomaly analysis on a Jupyter notebook.

drafts: contains draft files used to create the application and to perform some tests and try different Flink objects.

notebooks: contains analysis of the data extracted using a Flink Kafka connector to consume topics.


## App design :

- We use [low-level operations](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html#the-keyedprocessfunction)  (KeyedProcessFunction) to include timer in the detector.
- We use the timer to detect to fast repetitive clicks events (robot/scrapper behavior) by checking if the click event for one individul happens in a relatively small time interval.
- Hyperparameters (treshold are set in the detectors object definition).
- To differents detector are build, one looking for suspicious frequency patterns, the other one checking the click through rate.

Thank you !!!
