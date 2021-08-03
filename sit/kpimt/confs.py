natcos=["TMA", "TMCZ", "COSGRE", "TMHR", "COSROM", "GLOBUL", "TMD", "TMCG", "TMHU", "TMMK", "TMNL", "TMPL", "TMSK", "AMC"]
modes=["daily_input", "weekly_input", "monthly_input", "weekly_update", "monthly_update", "matrix"]
paths={"daily_input":"daily", "weekly_input":"weekly", "monthly_input":"monthly", "weekly_update":"daily_DWH_feed", "monthly_update":"daily_DWH_feed"}
outputs={"daily_input":"_daily.csv", "weekly_input":"_weekly.csv", "monthly_input": "_monthly.csv"}

weekly_datetime_format={"TMA": '%d.%m.%Y', "TMCZ" : '%d.%m.%Y', "COSGRE": '%d.%m.%Y', "TMHR": '%d.%m.%Y',
               "COSROM" :'%d.%m.%Y', "GLOBUL": '%d.%m.%Y', "TMD": '%d.%m.%Y', "TMCG":'%d.%m.%Y',
               "TMHU":'%d.%m.%Y', "TMMK":'%d.%m.%Y', "TMNL":'%d.%m.%Y', "TMPL":'%d.%m.%Y', "TMSK":'%Y-%m-%d %H:%M:%S',
               "AMC":'%d.%m.%Y'}

monthly_datetime_format={"TMA": '%d.%m.%Y', "TMCZ" : '%d.%m.%Y', "COSGRE": '%d.%m.%Y', "TMHR": '%d.%m.%Y',
               "COSROM" :'%d.%m.%Y', "GLOBUL": '%d.%m.%Y', "TMD": '%d.%m.%Y', "TMCG":'%d.%m.%Y',
               "TMHU":'%d.%m.%Y', "TMMK":'%d.%m.%Y', "TMNL":'%d.%m.%Y', "TMPL":'%d.%m.%Y', "TMSK":'%d.%m.%Y',
               "AMC":'%d.%m.%Y'}

daily_datetime_format={"TMA": '%Y%m%d%H%M%S', "TMCZ" : '%Y%m%d%H%M%S', "COSGRE": '%Y%m%d%H%M%S', "TMHR": '%Y%m%d%H%M%S',
               "COSROM" :'%Y%m%d%H%M%S', "GLOBUL": '%Y%m%d%H%M%S', "TMD": '%Y%m%d%H%M%S', "TMCG":'%Y%m%d%H%M%S',
               "TMHU":'%Y%m%d%H%M%S', "TMMK":'%Y%m%d%H%M%S', "TMNL":'%Y%m%d%H%M%S', "TMPL":'%Y%m%d%H%M%S', "TMSK":'%Y%m%d%H%M%S',
               "AMC":'%Y%m%d%H%M%S'}