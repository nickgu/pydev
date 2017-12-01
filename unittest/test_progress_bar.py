#! /bin/env python
# encoding=utf-8
# author: nickgu 
# 

import pydev
import time

if __name__=='__main__':
    pb = pydev.ProgressBar()
    pb.start('test', 2000)

    for i in range(2000):
        pb.inc(1)
        time.sleep(0.02)

