#!/usr/bin/env python3
"""
Tushare Realtime Service - A股实时行情服务
基于Tushare Pro API realtime_quote 接口（实时报价）和 rt_min 接口（分钟K线）
"""
import logging
import asyncio
import os
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    logging.warning("tushare not installed. Service will not be available.")


# Valid frequency options
FreqType = Literal["1MIN", "5MIN", "15MIN", "30MIN", "60MIN"]


class TushareRealtimeService:
    """
    A股实时分钟行情服务
    
    基于 Tushare Pro API rt_min 接口，提供：
    - 1/5/15/30/60 分钟级别实时行情
    - 支持多股票批量查询
    - 支持股票名称/代码双向查询
    """
    
    FREQ_OPTIONS = ["1MIN", "5MIN", "15MIN", "30MIN", "60MIN"]
    
    def __init__(self, token: Optional[str] = None):
        self.name = "Tushare Realtime Service"
        self.token = token or os.getenv('TUSHARE_TOKEN')
        self.pro = None
        
        # Cache settings
        self.cache_dir = Path("data/cache/tushare")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_expire_hours = 24  # Cache expiration for stock basic info
        
        if not self.token:
            logging.warning("TUSHARE_TOKEN not found in environment variables")

        if TUSHARE_AVAILABLE:
            try:
                if self.token:
                    ts.set_token(self.token)
                    self.pro = ts.pro_api(self.token)
                    logging.info("TushareRealtimeService initialized")
                else:
                    logging.warning("TushareRealtimeService initialized without token")
            except Exception as e:
                logging.error(f"Failed to initialize Tushare API: {str(e)}")
    
    def _load_cache(self, cache_key: str) -> Optional[Any]:
        """Load data from cache if valid"""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cache_time = cache_data.get('timestamp', 0)
                    if time.time() - cache_time < self.cache_expire_hours * 3600:
                        logging.debug(f"✅ Loaded {cache_key} from cache")
                        return cache_data.get('data')
            except Exception as e:
                logging.warning(f"Cache read error: {e}")
        return None
    
    def _save_cache(self, cache_key: str, data: Any):
        """Save data to cache"""
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            cache_data = {
                'timestamp': time.time(),
                'data': data
            }
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            logging.debug(f"💾 Saved {cache_key} to cache")
        except Exception as e:
            logging.warning(f"Cache write error: {e}")
    
    async def _get_stock_basic(self) -> Optional[pd.DataFrame]:
        """Get stock basic info with cache support"""
        if not self.pro:
            return None
        
        # Try load from cache first
        cached_data = self._load_cache('stock_basic')
        if cached_data:
            try:
                return pd.DataFrame(cached_data)
            except Exception:
                pass
        
        # Fetch from API
        try:
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(
                None, 
                lambda: self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
            )
            
            if df is not None and not df.empty:
                self._save_cache('stock_basic', df.to_dict('records'))
                return df
        except Exception as e:
            logging.error(f"Error fetching stock_basic: {e}")
        
        return None

    async def _get_code_by_name(self, names: List[str]) -> Dict[str, str]:
        """Get stock codes by names"""
        df = await self._get_stock_basic()
        if df is None or df.empty:
            return {}
        
        name_map = {}
        for name in names:
            row = df[df['name'] == name]
            if not row.empty:
                name_map[name] = row.iloc[0]['ts_code']
        
        return name_map

    async def _get_name_by_code(self, codes: List[str]) -> Dict[str, str]:
        """Get stock names by codes"""
        df = await self._get_stock_basic()
        if df is None or df.empty:
            return {}
        
        code_map = {}
        for code in codes:
            row = df[df['ts_code'] == code]
            if not row.empty:
                code_map[code] = row.iloc[0]['name']
        
        return code_map

    def _validate_freq(self, freq: str) -> str:
        """Validate and normalize frequency parameter"""
        freq_upper = freq.upper()
        if freq_upper not in self.FREQ_OPTIONS:
            raise ValueError(f"Invalid freq: {freq}. Must be one of {self.FREQ_OPTIONS}")
        return freq_upper

    async def _get_realtime_minute(
        self, 
        ts_codes: str, 
        freq: str = "1MIN"
    ) -> Dict[str, Any]:
        """
        获取A股实时分钟行情数据 (内部方法)
        
        Args:
            ts_codes: 股票代码，支持多个逗号分隔 (e.g., '600000.SH' 或 '600000.SH,000001.SZ')
            freq: 分钟周期，支持 5MIN/15MIN/30MIN/60MIN
            
        Returns:
            Dict: 包含实时行情数据
                - success (bool): 是否成功
                - data (list): 行情数据列表
                - metadata (dict): 元数据
        """
        if not self.pro:
            return {"success": False, "error": "Tushare not initialized"}
        
        try:
            freq = self._validate_freq(freq)
        except ValueError as e:
            return {"success": False, "error": str(e)}
        
        try:
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self.pro.rt_min(ts_code=ts_codes, freq=freq)
            )
            
            if df is None or df.empty:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {
                        "ts_codes": ts_codes,
                        "freq": freq,
                        "count": 0,
                        "message": "No data returned (market may be closed)"
                    }
                }
            
            # Get stock names for enrichment
            codes_list = ts_codes.split(',')
            code_name_map = await self._get_name_by_code(codes_list)
            
            # Process data
            records = []
            for _, row in df.iterrows():
                code = row['ts_code']
                record = {
                    "ts_code": code,
                    "name": code_name_map.get(code, ""),
                    "time": str(row['time']) if pd.notna(row['time']) else None,
                    "open": float(row['open']) if pd.notna(row['open']) else None,
                    "close": float(row['close']) if pd.notna(row['close']) else None,
                    "high": float(row['high']) if pd.notna(row['high']) else None,
                    "low": float(row['low']) if pd.notna(row['low']) else None,
                    "vol": float(row['vol']) if pd.notna(row['vol']) else None,
                    "amount": float(row['amount']) if pd.notna(row['amount']) else None,
                }
                records.append(record)
            
            return {
                "success": True,
                "data": records,
                "metadata": {
                    "ts_codes": ts_codes,
                    "freq": freq,
                    "count": len(records),
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
        except Exception as e:
            logging.error(f"Error fetching realtime minute data: {e}")
            return {"success": False, "error": str(e)}

    async def _get_minute_by_name(
        self, 
        stock_names: str, 
        freq: str = "1MIN"
    ) -> Dict[str, Any]:
        """
        按股票名称获取分钟K线行情 (内部方法)
        
        Args:
            stock_names: 股票名称，支持多个逗号分隔
            freq: 分钟周期，支持 1MIN/5MIN/15MIN/30MIN/60MIN
            
        Returns:
            Dict: 包含分钟K线数据
        """
        if not self.pro:
            return {"success": False, "error": "Tushare not initialized"}
        
        names_list = [n.strip() for n in stock_names.split(',') if n.strip()]
        if not names_list:
            return {"success": False, "error": "No stock names provided"}
        
        name_code_map = await self._get_code_by_name(names_list)
        
        if not name_code_map:
            return {"success": False, "error": f"Could not find codes for: {stock_names}"}
        
        not_found = [n for n in names_list if n not in name_code_map]
        
        ts_codes = ",".join(name_code_map.values())
        result = await self._get_realtime_minute(ts_codes, freq)
        
        if not_found and result.get("success"):
            result["metadata"]["not_found_names"] = not_found
        
        return result

    async def _get_realtime_quote(
        self, 
        ts_codes: str,
        src: str = "sina"
    ) -> Dict[str, Any]:
        """
        获取实时报价数据 (内部方法)
        基于 realtime_quote 接口，返回真正的实时价格
        
        Args:
            ts_codes: 股票代码，支持多个逗号分隔 (sina源最多50个，dc源只支持单个)
            src: 数据源 sina-新浪(默认) dc-东方财富
            
        Returns:
            Dict: 包含实时报价数据
        """
        if not TUSHARE_AVAILABLE:
            return {"success": False, "error": "Tushare not available"}
        
        try:
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(
                None,
                lambda: ts.realtime_quote(ts_code=ts_codes, src=src)
            )
            
            if df is None or df.empty:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {
                        "ts_codes": ts_codes,
                        "src": src,
                        "count": 0,
                        "message": "No data returned (market may be closed)"
                    }
                }
            
            # Process data
            records = []
            for _, row in df.iterrows():
                record = {
                    "ts_code": str(row.get('TS_CODE', '')),
                    "name": str(row.get('NAME', '')),
                    "price": float(row['PRICE']) if pd.notna(row.get('PRICE')) else None,
                    "change": round(float(row['PRICE']) - float(row['PRE_CLOSE']), 2) if pd.notna(row.get('PRICE')) and pd.notna(row.get('PRE_CLOSE')) else None,
                    "pct_change": round((float(row['PRICE']) - float(row['PRE_CLOSE'])) / float(row['PRE_CLOSE']) * 100, 2) if pd.notna(row.get('PRICE')) and pd.notna(row.get('PRE_CLOSE')) and float(row.get('PRE_CLOSE', 0)) != 0 else None,
                    "open": float(row['OPEN']) if pd.notna(row.get('OPEN')) else None,
                    "high": float(row['HIGH']) if pd.notna(row.get('HIGH')) else None,
                    "low": float(row['LOW']) if pd.notna(row.get('LOW')) else None,
                    "pre_close": float(row['PRE_CLOSE']) if pd.notna(row.get('PRE_CLOSE')) else None,
                    "volume": int(row['VOLUME']) if pd.notna(row.get('VOLUME')) else None,
                    "amount": float(row['AMOUNT']) if pd.notna(row.get('AMOUNT')) else None,
                    "bid": float(row['BID']) if pd.notna(row.get('BID')) else None,
                    "ask": float(row['ASK']) if pd.notna(row.get('ASK')) else None,
                    "date": str(row.get('DATE', '')),
                    "time": str(row.get('TIME', '')),
                }
                records.append(record)
            
            return {
                "success": True,
                "data": records,
                "metadata": {
                    "ts_codes": ts_codes,
                    "src": src,
                    "count": len(records),
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
        except Exception as e:
            logging.error(f"Error fetching realtime quote: {e}")
            return {"success": False, "error": str(e)}

    async def get_realtime_by_name(
        self, 
        stock_names: str
    ) -> Dict[str, Any]:
        """
        获取A股实时报价行情（按股票名称批量查询）
        
        功能说明:
            根据股票名称获取实时行情数据，支持同时查询多只股票，
            返回当前价格、涨跌幅、成交量等核心行情指标。
            
        使用场景:
            - 快速查看自选股实时行情
            - 批量获取多只股票当前价格
            - 监控股票涨跌情况
            - 获取买一卖一盘口数据
        
        Args:
            stock_names: 股票名称，支持多个逗号分隔
                        例如: '浦发银行' 或 '浦发银行,平安银行,贵州茅台'
                        单次最多查询50只股票
            
        Returns:
            Dict: 包含实时报价数据列表，每只股票包括:
                - ts_code: 股票代码 (e.g., '600000.SH')
                - name: 股票名称
                - price: 当前价格
                - change: 涨跌额
                - pct_change: 涨跌幅(%)
                - open: 今日开盘价
                - high: 今日最高价
                - low: 今日最低价
                - pre_close: 昨日收盘价
                - volume: 成交量（股）
                - amount: 成交金额（元）
                - bid: 买一价
                - ask: 卖一价
                - date: 日期
                - time: 时间
                
        注意事项:
            - 基于新浪财经数据源
            - 非交易时段返回收盘数据
        """
        names_list = [n.strip() for n in stock_names.split(',') if n.strip()]
        if not names_list:
            return {"success": False, "error": "No stock names provided"}
        
        if len(names_list) > 50:
            return {"success": False, "error": "Maximum 50 stocks per request"}
        
        # Convert names to codes
        name_code_map = await self._get_code_by_name(names_list)
        
        if not name_code_map:
            return {"success": False, "error": f"Could not find codes for: {stock_names}"}
        
        # Find names that couldn't be resolved
        not_found = [n for n in names_list if n not in name_code_map]
        
        ts_codes = ",".join(name_code_map.values())
        result = await self._get_realtime_quote(ts_codes, src="sina")
        
        if not_found and result.get("success"):
            result["metadata"]["not_found_names"] = not_found
        
        return result


    async def _get_realtime_tick(
        self,
        ts_code: str,
        src: str = "sina"
    ) -> Dict[str, Any]:
        """
        获取实时分笔成交数据（内部方法）
        基于 realtime_tick 接口（爬虫），返回当日开盘以来的所有分笔成交数据
        
        Args:
            ts_code: 股票代码，按tushare标准输入 (e.g., '000001.SZ'表示平安银行，'600000.SH'表示浦发银行)
                     单次只能输入一个股票
            src: 数据源 sina-新浪(默认) dc-东方财富
            
        Returns:
            Dict: 包含分笔成交数据
                - time: 交易时间
                - price: 现价
                - change: 价格变动
                - volume: 成交量（单位：手）
                - amount: 成交金额（元）
                - type: 类型 买入/卖出/中性
        """
        if not TUSHARE_AVAILABLE:
            return {"success": False, "error": "Tushare not available"}
        
        if not ts_code:
            return {"success": False, "error": "ts_code is required"}
        
        # Validate single stock code
        if ',' in ts_code:
            return {"success": False, "error": "Only single stock code is supported for realtime_tick"}
        
        try:
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(
                None,
                lambda: ts.realtime_tick(ts_code=ts_code, src=src)
            )
            
            if df is None or df.empty:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {
                        "ts_code": ts_code,
                        "src": src,
                        "count": 0,
                        "message": "No data returned (market may be closed)"
                    }
                }
            
            # Get stock name for enrichment
            code_name_map = await self._get_name_by_code([ts_code])
            stock_name = code_name_map.get(ts_code, "")
            
            # Process data
            records = []
            for _, row in df.iterrows():
                record = {
                    "time": str(row.get('TIME', row.get('time', ''))) if pd.notna(row.get('TIME', row.get('time'))) else None,
                    "price": float(row.get('PRICE', row.get('price', 0))) if pd.notna(row.get('PRICE', row.get('price'))) else None,
                    "change": float(row.get('CHANGE', row.get('change', 0))) if pd.notna(row.get('CHANGE', row.get('change'))) else None,
                    "volume": int(row.get('VOLUME', row.get('volume', 0))) if pd.notna(row.get('VOLUME', row.get('volume'))) else None,
                    "amount": float(row.get('AMOUNT', row.get('amount', 0))) if pd.notna(row.get('AMOUNT', row.get('amount'))) else None,
                    "type": str(row.get('TYPE', row.get('type', ''))) if pd.notna(row.get('TYPE', row.get('type'))) else None,
                }
                records.append(record)
            
            return {
                "success": True,
                "data": records,
                "metadata": {
                    "ts_code": ts_code,
                    "name": stock_name,
                    "src": src,
                    "count": len(records),
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
        except Exception as e:
            logging.error(f"Error fetching realtime tick: {e}")
            return {"success": False, "error": str(e)}

    async def get_realtime_tick_by_name(
        self,
        stock_name: str,
        src: str = "sina"
    ) -> Dict[str, Any]:
        """
        获取单只A股当日全部分笔成交明细（按股票名称查询）
        
        功能说明:
            返回该股票从今日开盘到当前时刻的所有逐笔成交记录，
            可用于分析主力资金动向、大单追踪、买卖盘力量对比等。
            
        使用场景:
            - 分析某只股票的实时成交明细
            - 追踪大单买入/卖出情况
            - 观察主力资金进出方向
            - 盘中实时监控成交动态
        
        Args:
            stock_name: 股票名称，仅支持单只股票 (e.g., '浦发银行', '平安银行', '贵州茅台')
            src: 数据源，可选 'sina'(新浪,默认) 或 'dc'(东方财富)
            
        Returns:
            Dict: 包含分笔成交数据列表
                - time: 成交时间 (e.g., '09:30:05')
                - price: 成交价格
                - change: 价格变动（相对上一笔）
                - volume: 成交量（单位：手，1手=100股）
                - amount: 成交金额（单位：元）
                - type: 成交类型 '买盘'/'卖盘'/'中性'
                
        注意事项:
            - 数据来自网络爬虫，非官方接口，仅供研究学习
            - 盘中数据量可能较大（数千条），请注意处理
            - 非交易时段可能返回空数据
        """
        if not stock_name:
            return {"success": False, "error": "stock_name is required"}
        
        # Convert name to code
        name_code_map = await self._get_code_by_name([stock_name.strip()])
        
        if not name_code_map:
            return {"success": False, "error": f"Could not find code for: {stock_name}"}
        
        ts_code = name_code_map.get(stock_name.strip())
        return await self._get_realtime_tick(ts_code, src)

    async def _get_realtime_list(
        self,
        src: str = "dc"
    ) -> Dict[str, Any]:
        """
        获取实时涨跌幅排名（内部方法）
        基于 realtime_list 接口（爬虫），返回全市场股票实时涨跌幅排名
        
        Args:
            src: 数据源 sina-新浪 dc-东方财富(默认)
            
        Returns:
            Dict: 包含全市场股票实时排名数据
            
            东财数据(dc)字段：
                - ts_code: 股票代码
                - name: 股票名称
                - price: 当前价格
                - pct_change: 涨跌幅(%)
                - change: 涨跌额
                - volume: 成交量（手）
                - amount: 成交金额（元）
                - swing: 振幅
                - low/high: 今日最低/最高价
                - open/close: 今日开盘/收盘价
                - vol_ratio: 量比
                - turnover_rate: 换手率
                - pe: 市盈率
                - pb: 市净率
                - total_mv: 总市值（元）
                - float_mv: 流通市值（元）
                - rise: 涨速
                - 5min: 5分钟涨幅
                - 60day: 60天涨幅
                - 1tyear: 1年涨幅
                
            新浪数据(sina)字段：
                - ts_code: 股票代码
                - name: 股票名称
                - price: 当前价格
                - pct_change: 涨跌幅(%)
                - change: 涨跌额
                - buy/sale: 买入价/卖出价
                - open/close: 今日开盘/收盘价
                - high/low: 今日最高/最低价
                - volume: 成交量（股）
                - amount: 成交金额（元）
                - time: 当前时间
        """
        if not TUSHARE_AVAILABLE:
            return {"success": False, "error": "Tushare not available"}
        
        try:
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(
                None,
                lambda: ts.realtime_list(src=src)
            )
            
            if df is None or df.empty:
                return {
                    "success": True,
                    "data": [],
                    "metadata": {
                        "src": src,
                        "count": 0,
                        "message": "No data returned (market may be closed)"
                    }
                }
            
            # Process data - convert DataFrame to list of dicts
            records = df.to_dict('records')
            
            # Clean up the records (handle NaN values)
            cleaned_records = []
            for record in records:
                cleaned = {}
                for key, value in record.items():
                    if pd.isna(value):
                        cleaned[key] = None
                    elif isinstance(value, (int, float)):
                        cleaned[key] = value
                    else:
                        cleaned[key] = str(value)
                cleaned_records.append(cleaned)
            
            return {
                "success": True,
                "data": cleaned_records,
                "metadata": {
                    "src": src,
                    "count": len(cleaned_records),
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
        except Exception as e:
            logging.error(f"Error fetching realtime list: {e}")
            return {"success": False, "error": str(e)}

    async def get_realtime_list_top(
        self,
        src: str = "dc",
        top_n: int = 20,
        sort_by: str = "pct_change",
        ascending: bool = False
    ) -> Dict[str, Any]:
        """
        获取A股全市场实时涨跌幅排行榜（支持多维度排序）
        
        功能说明:
            实时抓取沪深两市全部股票的行情数据，支持按涨跌幅、成交量、
            换手率等多个维度排序，返回排名前N的股票列表。
            
        使用场景:
            - 盘中实时查看涨幅榜/跌幅榜
            - 寻找成交量异动股票
            - 筛选高换手率活跃股
            - 发现量比异常的股票
            - 监控市场热点板块龙头
        
        Args:
            src: 数据源，可选 'dc'(东方财富,默认,字段更丰富) 或 'sina'(新浪)
            top_n: 返回前N条记录，默认20，最大不限
            sort_by: 排序字段，默认 'pct_change'(涨跌幅)
                     常用字段:
                     - pct_change: 涨跌幅(%)
                     - change: 涨跌额
                     - volume: 成交量
                     - amount: 成交额
                     - turnover_rate: 换手率(%)
                     - vol_ratio: 量比
                     - swing: 振幅(%)
                     - rise: 涨速
                     - total_mv: 总市值
            ascending: 排序方向
                     - False(默认): 降序，获取涨幅榜/成交量最大等
                     - True: 升序，获取跌幅榜/成交量最小等
            
        Returns:
            Dict: 包含排名前N的股票数据，东财(dc)数据字段包括:
                - ts_code: 股票代码 (e.g., '000001.SZ')
                - name: 股票名称
                - price: 当前价格
                - pct_change: 涨跌幅(%)
                - change: 涨跌额
                - volume: 成交量（手）
                - amount: 成交金额（元）
                - swing: 振幅(%)
                - open/close: 今开/收盘价
                - high/low: 最高/最低价
                - vol_ratio: 量比
                - turnover_rate: 换手率(%)
                - pe: 市盈率
                - pb: 市净率
                - total_mv: 总市值（元）
                - float_mv: 流通市值（元）
                - rise: 涨速
                - 5min/60day/1tyear: 5分钟/60日/1年涨幅
                
        示例用法:
            - 涨幅前10: get_realtime_list_top(top_n=10)
            - 跌幅前10: get_realtime_list_top(top_n=10, ascending=True)
            - 成交额前20: get_realtime_list_top(top_n=20, sort_by='amount')
            - 换手率前15: get_realtime_list_top(top_n=15, sort_by='turnover_rate')
            
        注意事项:
            - 数据来自网络爬虫，非官方接口，仅供研究学习
            - 非交易时段返回收盘数据
        """
        result = await self._get_realtime_list(src)
        
        if not result.get("success") or not result.get("data"):
            return result
        
        try:
            # Convert to DataFrame for sorting
            df = pd.DataFrame(result["data"])
            
            # Sort by specified column
            if sort_by in df.columns:
                df = df.sort_values(by=sort_by, ascending=ascending, na_position='last')
            
            # Get top N
            df_top = df.head(top_n)
            
            return {
                "success": True,
                "data": df_top.to_dict('records'),
                "metadata": {
                    "src": src,
                    "top_n": top_n,
                    "sort_by": sort_by,
                    "ascending": ascending,
                    "count": len(df_top),
                    "total_stocks": len(df),
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
        except Exception as e:
            logging.error(f"Error processing realtime list top: {e}")
            return {"success": False, "error": str(e)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    async def test():
        service = TushareRealtimeService()
        
        # Test: Get realtime quote by name (main tool)
        print("\n=== Test: Realtime quote by name ===")
        result = await service.get_realtime_by_name('浦发银行,平安银行')
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        # Test: Get realtime tick by name
        print("\n=== Test: Realtime tick by name ===")
        result = await service.get_realtime_tick_by_name('浦发银行')
        print(f"Tick count: {result.get('metadata', {}).get('count', 0)}")
        if result.get('data'):
            print(f"First 3 ticks: {json.dumps(result['data'][:3], ensure_ascii=False, indent=2)}")
        
        # Test: Get realtime list top 10
        print("\n=== Test: Realtime list top 10 ===")
        result = await service.get_realtime_list_top(top_n=10)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(test())

