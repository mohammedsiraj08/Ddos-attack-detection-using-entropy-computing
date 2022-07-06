# Ddos-attack-detection-using-entropy-computing
Detected ddos using fast entropy computing method. 
1. Enter data into the mongodb object dataddos using enter.py file
2. To detect the ddos attack in the data run __init__.py it uses sliding window method (present in sliding_window_method.py) to calculate the entropy for certain time intervals divided. From the entropy values being calculated the randomness is detected using standard deviation and hence it is concluded that ddos attack is performed in that time interval.
