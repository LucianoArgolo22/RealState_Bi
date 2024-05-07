from processing.processing_step_3 import processing_step_3
from processing.processing_step_2 import *
from processing.processing_step_1 import processing_step_1
import time

def pipeline(step_1:bool=False, step_2:bool=False) -> None:
    processing_step_1(test=step_1)
    time.sleep(2)
    processing_step_2(test=step_2)
    time.sleep(2)
    processing_step_3()
