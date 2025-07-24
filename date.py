from datetime import datetime, timedelta
yday        = (datetime.now() - timedelta(days=2)).date()
print(yday.strftime("%Y/%m/%d"))   # 2025/07/14