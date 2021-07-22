import os.path
from os import path
import pandas as pd

class Daily_outputs_handler:
    path = None
    kpis = None
    def __init__(self, path, kpis):
        self.path = path
        self.kpis = kpis

    def aggregate_daily(self):
        pd.read_csv(self.path, delimiter='|', quotechar='"', header=True)

    def get_daily_agregates(self):
        if (path.exists(self.path)):


