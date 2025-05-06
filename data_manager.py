from xtquant import xtdata
import time

def on_progress(data):
    print(f"下载进度: {data}%")

class DataManager:
    """
    收拢获取历史数据的接口，不负责实时行情数据"""
    def __init__(self):
        #调用一次触发xtdata的初始化，否则容易取到更多的空数据
        xtdata.get_local_data(['close'], ['002639.SZ'], period='1d', start_time="20241101", end_time="20241102")
        time.sleep(0.5)
        pass

    def download_data_async(self, all_codes, period, start_date, end_date=None):
        """下载历史数据 下载接口本身不返回数据
        异步下载 """
        if end_date is None:
            end_date = time.strftime("%Y%m%d", time.localtime())
        for code in all_codes:
            xtdata.download_history_data(code, period=period, start_time=start_date, end_time=end_date)
        

    def download_data_sync(self, codes, period, start_date, end_date=None):
        #下载历史数据 下载接口本身不返回数据
        #会返回下载进度 通过回调函数
        if end_date is None:
            end_date = time.strftime("%Y%m%d", time.localtime())
        xtdata.download_history_data2(codes, period=period, start_time=start_date, end_time=end_date, callback=on_progress)

    def get_minutes_data(self, fileds, codes, start_date, end_date=None):
        if len(fileds) == 0 or len(codes) == 0:
            print("empty fileds or codes")
            return None
        if end_date is None:
            end_date = time.strftime("%Y%m%d%H%M%S", time.localtime())
        data = xtdata.get_market_data(fileds, codes, period='1m', start_time=start_date, end_time=end_date)
        return data
    
    def get_local_minutes_data(self, fileds, codes, start_date, end_date=None):
        """
        从本地获取数据
        """
        if len(fileds) == 0 or len(codes) == 0:
            print("empty fileds or codes")
            return None
        if end_date is None:
            end_date = time.strftime("%Y%m%d%H%M%S", time.localtime())
        data = xtdata.get_local_data(fileds, codes, period='1m', start_time=start_date, end_time=end_date)
        return data
    
    def get_daily_data(self, fileds, codes, start_date, end_date=None):
        if len(fileds) == 0 or len(codes) == 0:
            print("empty fileds or codes")
            return None
        if end_date is None:
            end_date = time.strftime("%Y%m%d", time.localtime())
        data = xtdata.get_market_data(fileds, codes, period='1d', start_time=start_date, end_time=end_date)
        return data
    
    def get_local_daily_data(self, fileds, codes, start_date, end_date=None):
        """
        从本地获取数据
        """
        if len(fileds) == 0 or len(codes) == 0:
            print("empty fileds or codes")
            return None
        if end_date is None:
            end_date = time.strftime("%Y%m%d", time.localtime())
        data = xtdata.get_local_data(fileds, codes, period='1d', start_time=start_date, end_time=end_date)
        return data


if __name__ == '__main__':
    start_date = '20241101'
    #end_date = '20250104'
    codes = ["300870.SZ","430139.BJ","688449.SH","920029.BJ"]
    data_manager = DataManager()
    data_manager.download_data_async(codes, '1d', start_date)
    #data1 = data_manager.get_daily_data(['close'], codes, start_date)
    #data2 = data_manager.get_local_daily_data(['close'], codes, start_date, end_date)
    data2 = data_manager.get_local_daily_data(['close', 'volume'], codes, start_date)
    #print(data1)
    print(data2)