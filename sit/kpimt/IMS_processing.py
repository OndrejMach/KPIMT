import re
from datetime import datetime


class IMS_processing:
    ims_data = None

    def __init__(self, ims_data):
        self.ims_data = ims_data

    def process_data(self):
        def get_date(date):
            date_f = str(date).split(sep=" ")[0]
            date_format = "%d.%m.%Y" if (re.match("\d{2}.\d{2}.\d{4}", date_f)) else "%Y-%m-%d"
            return datetime.strptime(date_f, date_format).strftime("%d.%m.%Y")

        def get_input_id(row):
            row['Input_ID'] = "{}{}-{}".format(row['Region'], row['KPI name'], row['Date'])
            return row

        input = self.ims_data[(self.ims_data['Date'] != '') & (self.ims_data['Date'].notna())].copy()
        input['Date'] = input['Date'].apply(lambda x: get_date(x))
        input['KPI name'] = input['KPI name'].apply(lambda x: str(x).upper())
        input['Input_ID'] = None
        input = input.apply(lambda row: get_input_id(row), axis=1)

        return input



