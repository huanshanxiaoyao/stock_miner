# coding:utf-8
import os
import sys
import time
import unittest
import pandas as pd
from datetime import datetime, timedelta
from stock_code_config import SH50

# 添加当前目录到系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_manager import DataManager

class TestDataManager(unittest.TestCase):
    """
    测试DataManager类的数据下载和获取功能
    """
    
    def setUp(self):
        """
        测试前的准备工作
        """
        self.data_manager = DataManager()
        # 测试用的股票代码列表
        self.test_codes = ["300870.SZ", "430139.BJ", "688449.SH", "920029.BJ"]
        self.test_codes = SH50
        # 设置测试的起始日期（前3天）
        self.start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        # 设置测试的结束日期（当前日期）
        self.end_date = datetime.now().strftime("%Y%m%d")
    
    def test_get_daily_data(self):
        """
        测试获取股票列表过去30天的天级数据
        """
        print(f"\n开始测试获取天级数据...")
        print(f"测试股票: {self.test_codes}")
        print(f"起始日期: {self.start_date}")
        print(f"结束日期: {self.end_date}")
        
        # 1. 先尝试下载天级数据，确保数据存在
        print(f"下载天级数据以确保数据存在...")
        self.data_manager.download_data_sync(self.test_codes, '1d', self.start_date, self.end_date)
        
        # 等待几秒，确保数据下载完成
        wait_time = 3
        print(f"等待 {wait_time} 秒，确保数据下载完成...")
        time.sleep(wait_time)
        
        # 2. 获取天级数据
        print(f"获取天级数据...")
        daily_data = self.data_manager.get_local_daily_data(['close', 'volume', 'high', 'low'], 
                                                           self.test_codes, 
                                                           self.start_date, 
                                                           self.end_date)
        
        # 3. 验证数据
        self.assertIsNotNone(daily_data, "天级数据获取失败")
        
        # 检查每只股票的数据
        for code in self.test_codes:
            # 验证股票代码存在于返回的数据中
            self.assertIn(code, daily_data, f"股票 {code} 数据不存在")
            # 验证数据不为空
            self.assertFalse(daily_data[code].empty, f"股票 {code} 数据为空")
            
            # 获取数据点数量
            data_points = len(daily_data[code])
            print(f"股票 {code}: 天级数据点数量 {data_points}")
            
            # 验证数据点数量大于0
            self.assertGreater(data_points, 0, f"股票 {code} 没有天级数据")
            
            # 打印每只股票的数据统计信息
            try:
                # 将索引转换为datetime类型
                stock_data = daily_data[code]
                stock_data.index = pd.to_datetime(stock_data.index)
                
                # 计算统计信息
                close_mean = stock_data['close'].mean()
                close_std = stock_data['close'].std()
                volume_mean = stock_data['volume'].mean()
                volume_std = stock_data['volume'].std()
                
                print(f"\n股票 {code} 天级数据统计:")
                print("-" * 40)
                print(f"收盘价均值: {close_mean:.2f}, 标准差: {close_std:.2f}")
                print(f"成交量均值: {volume_mean:.2f}, 标准差: {volume_std:.2f}")
                
                # 打印最近5个交易日的数据
                print(f"\n最近5个交易日数据:")
                recent_data = stock_data.sort_index(ascending=False).head(5)
                for date, row in recent_data.iterrows():
                    print(f"日期: {date.strftime('%Y-%m-%d')}, 收盘价: {row['close']:.2f}, 成交量: {row['volume']:.0f}")
                
                print("-" * 40)
            except Exception as e:
                print(f"处理天级数据时出错: {str(e)}")
        
        print("天级数据获取测试完成，所有测试通过!")
    
    def test_download_minutes_data(self):
        """
        测试下载分钟级数据并验证是否成功
        """
        print(f"\n开始测试下载分钟级数据...")
        print(f"测试股票: {self.test_codes}")
        print(f"起始日期: {self.start_date}")
        print(f"结束日期: {self.end_date}")
        
        # 1. 先尝试获取本地数据，记录初始状态
        initial_data = self.data_manager.get_local_minutes_data(['volume'], self.test_codes, self.start_date, self.end_date)
        
        # 记录初始数据状态
        initial_status = {}
        if initial_data is not None:
            for code in self.test_codes:
                if code in initial_data and not initial_data[code].empty:
                    initial_status[code] = len(initial_data[code])
                else:
                    initial_status[code] = 0
        
        print(f"初始数据状态: {initial_status}")
        
        # 2. 下载分钟级数据
        print(f"开始下载分钟级数据...")
        self.data_manager.download_data_sync(self.test_codes, '1m', self.start_date, self.end_date)
        
        # 3. 等待几秒，确保数据下载完成
        wait_time = 3
        print(f"等待 {wait_time} 秒，确保数据下载完成...")
        time.sleep(wait_time)
        
        # 4. 再次获取数据，验证下载是否成功
        print(f"验证数据下载是否成功...")
        downloaded_data = self.data_manager.get_local_minutes_data(['volume'], self.test_codes, self.start_date, self.end_date)
        
        # 5. 验证数据
        self.assertIsNotNone(downloaded_data, "下载后获取数据失败")
        
        # 检查每只股票的数据
        for code in self.test_codes:
            # 验证股票代码存在于返回的数据中
            self.assertIn(code, downloaded_data, f"股票 {code} 数据不存在")
            # 验证数据不为空
            self.assertFalse(downloaded_data[code].empty, f"股票 {code} 数据为空")
            
            # 获取数据点数量
            data_points = len(downloaded_data[code])
            initial_points = initial_status.get(code, 0)
            
            print(f"股票 {code}: 初始数据点 {initial_points}, 下载后数据点 {data_points}")
            
            # 打印每天10点01分的分钟级数据
            try:
                # 将索引转换为datetime类型
                volume_data = downloaded_data[code]['volume']
                volume_data.index = pd.to_datetime(volume_data.index)
                
                # 按日期分组
                volume_data_by_date = volume_data.groupby(volume_data.index.date)
                
                print(f"\n股票 {code} 每天10:01分钟的成交量数据:")
                print("-" * 40)
                
                for date, group in volume_data_by_date:
                    # 查找10:01的数据
                    target_time = pd.Timestamp(date).replace(hour=10, minute=1)
                    # 查找最接近10:01的数据点
                    closest_data = group[group.index.map(lambda x: x.hour == 10 and x.minute == 1)]
                    
                    if not closest_data.empty:
                        print(f"日期: {date}, 时间: 10:01, 成交量: {closest_data.iloc[0]}")
                    else:
                        print(f"日期: {date}, 时间: 10:01, 成交量: 数据不存在")
                
                print("-" * 40)
            except Exception as e:
                print(f"打印10:01分钟数据时出错: {str(e)}")
            
            # 如果初始数据为空，验证下载后有数据
            if initial_points == 0:
                self.assertGreater(data_points, 0, f"股票 {code} 下载后仍然没有数据")
            # 如果初始数据不为空，验证数据点数量不减少
            else:
                self.assertGreaterEqual(data_points, initial_points, f"股票 {code} 下载后数据点减少")
        
        print("分钟级数据下载测试完成，所有测试通过!")

if __name__ == "__main__":
    unittest.main()