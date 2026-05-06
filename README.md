### команда запуска
python C:\stuff\пики_с_осциллографа\oscilloscope_stream.py --host 192.168.1.4 --backend visa --gui --csv live_signal.csv --baseline edges --interval 1 --threshold 1.5 --polarity positive --min-distance-samples 5


### режим дебага 
python C:\stuff\пики_с_осциллографа\oscilloscope_stream.py --gui --dummy True --dummy-csv live_signal.csv --baseline edges --interval 1 --threshold 1.5 --polarity positive --min-distance-samples 5