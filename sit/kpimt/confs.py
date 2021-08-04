natcos=["TMA", "TMCZ", "COSGRE", "TMHR", "COSROM", "GLOBUL", "TMD", "TMCG", "TMHU", "TMMK", "TMNL", "TMPL", "TMSK", "AMC"]
modes=["daily_input", "weekly_input", "monthly_input", "weekly_update", "monthly_update", "matrix"]
paths={"daily_input":"daily", "weekly_input":"weekly", "monthly_input":"monthly", "weekly_update":"daily_DWH_feed", "monthly_update":"daily_DWH_feed"}
outputs={"daily_input":"_daily.csv", "weekly_input":"_weekly.csv", "monthly_input": "_monthly.csv"}

matrix_schema_weekly= ['Natco','KPI_ID','KPI_Name','requested_Weekly','Date','KEY1','KPI_Value','IsDelivered','Time','Input_File','Remarks','was_corrected_Flag']
matrix_schema_daily=  ['Natco','KPI_ID','KPI_Name','requested_Daily','Date','normDate', "Week","Year","Month","Day","YearMonth","Quarter","YearWeek",
                       "WeekDay" ,'KEY1','KPI_Value',"Time",'IsDelivered',"Input_File","SourceSystem","Denominator","Numerator",'was_corrected_Flag']
matrix_schema_monthly=  ['Natco','KPI_ID','KPI_Name','requested_Monthly','Date','KEY1','KPI_Value','IsDelivered','Time','Input_File','Remarks','was_corrected_Flag']

weekly_datetime_format={"TMA": '%d.%m.%Y', "TMCZ" : '%d.%m.%Y', "COSGRE": '%d.%m.%Y', "TMHR": '%Y-%m-%d %H:%M:%S',
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

corections_schema = ['Key_Corr', 'DatumKPI', 'KPINameQVD', 'Natco', 'KPIValueOld',
                                'KPIValueNew', 'CommentRowOld', 'CommentRowNew', 'CommentFileOld',
                                'CommentFileNew', 'TimestampCorrFile', 'Granularity', 'FileOld',
                                'FileNew', 'correction_timestamp']

output_schema = ['Input_ID', 'Date', 'KPI name', 'Region', 'Value', 'Remarks', 'Input_File', 'was_corrected_Flag']
output_daily_schema = ["Input_ID","Date","TimestampTo",	"Region","SourceSystem","KPI_ID","Denominator",	"Numerator","Value","Input_File","was_corrected_Flag"]