"""
布林带做市策略主模块
"""

import time
from typing import List, Dict, Tuple, Optional
from decimal import Decimal
import random
import threading
import logging
import queue
import os
from datetime import datetime

from api.backpack_client import BackpackClient
from api.backpack_ws_client import BackpackWSClient
from config import (
    API_KEY,
    SECRET_KEY,
    SYMBOL,
    ORDER_AMOUNT,
    GRID_TOTAL_INVESTMENT,
    PRICE_PRECISION,
    QUANTITY_PRECISION,
    SPREAD,
    LONG_BOLL_PERIOD,
    LONG_BOLL_STD,
    SHORT_BOLL_PERIOD,
    SHORT_BOLL_STD,
    MAX_POSITION_SCALE,
    MIN_POSITION_SCALE,
    MIN_PROFIT_SPREAD,
    TRADE_IN_BAND,
    BUY_BELOW_SMA,
    BASE_ORDER_SIZE,
    QUOTE_ORDER_SIZE,
    LONG_BOLL_INTERVAL,
    SHORT_BOLL_INTERVAL,
    DYNAMIC_SPREAD,
    SPREAD_MIN,
    SPREAD_MAX,
    TREND_SKEW,
    UPTREND_SKEW,
    DOWNTREND_SKEW,
    STOP_LOSS_ACTIVATION,
    STOP_LOSS_RATIO,
    TAKE_PROFIT_RATIO,
    SELL_ABOVE_SMA,
    POSITION_BIAS,
    MAX_POSITION_RATIO,
    SELL_BIAS_MULTIPLIER
)
from utils.indicators import BollingerBands
from utils.database import PositionDB
from logger import setup_logger

logger = setup_logger("grid_bot")

class BollMakerBot:
    def __init__(self, config: dict):
        """初始化网格交易机器人"""
        self.config = config
        self.api_key = config["API_KEY"]
        self.secret_key = config["SECRET_KEY"]
        self.symbol = config["SYMBOL"]
        
        # 确保data目录存在
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        
        # 初始化数据库
        db_path = os.path.join(data_dir, "positions.db")
        self.db = PositionDB(db_path)
        self.logger = logging.getLogger(__name__)
        
        # 初始化消息队列和事件
        self.db_queue = queue.Queue()
        self.position_event = threading.Event()
        self.position_cache = None
        self.last_position_update = 0
        self.position_update_interval = 1
        self.cache_lock = threading.Lock()
        
        # 添加订单控制
        self.last_order_time = 0
        self.order_interval = 120  # 120秒，即2分钟
        
        # 初始化API客户端
        self.rest_client = BackpackClient(self.api_key, self.secret_key)
        self.ws_client = BackpackWSClient(
            self.api_key, 
            self.secret_key, 
            self.symbol,
            on_message_callback=self._handle_ws_message
        )
        
        # 只保留必要的锁
        self.boll_lock = threading.Lock()  # 布林带数据锁
        
        # 添加初始化完成事件
        self.init_complete = threading.Event()
        
        # K线更新控制
        self.kline_update_thread = None
        self.kline_update_interval = 60  # 每60秒更新一次K线数据
        
        # 初始化状态变量
        self.running = False
        self.current_orders = {}
        self.last_price = 0
        self.last_update_time = 0
        
        # 初始化布林带数据
        self.long_boll = BollingerBands(period=LONG_BOLL_PERIOD, num_std=LONG_BOLL_STD)
        self.short_boll = BollingerBands(period=SHORT_BOLL_PERIOD, num_std=SHORT_BOLL_STD)
        self.long_klines = {}
        self.short_klines = {}

    def setup_boll_data(self, prices: List[float], long_period: int = None, short_period: int = None):
        """
        为测试目的设置布林带数据
        
        Args:
            prices: 价格列表
            long_period: 长期布林带周期（可选）
            short_period: 短期布林带周期（可选）
        """
        if long_period:
            self.long_boll = BollingerBands(period=long_period, num_std=LONG_BOLL_STD)
        if short_period:
            self.short_boll = BollingerBands(period=short_period, num_std=SHORT_BOLL_STD)
            
        # 生成模拟的K线数据
        current_time = int(time.time())
        for i, price in enumerate(prices):
            timestamp = current_time - (len(prices) - i) * 60  # 每分钟一个K线
            kline_data = {
                "close": price,
                "timestamp": timestamp
            }
            
            # 更新长期和短期K线数据
            self.long_klines[timestamp] = kline_data
            self.short_klines[timestamp] = kline_data
            
        # 计算布林带
        with self.boll_lock:
            # 按时间戳排序
            sorted_long_klines = sorted(self.long_klines.values(), key=lambda x: x["timestamp"])
            sorted_short_klines = sorted(self.short_klines.values(), key=lambda x: x["timestamp"])
            
            # 获取收盘价
            long_closes = [float(k["close"]) for k in sorted_long_klines[-self.long_boll.period:]]
            short_closes = [float(k["close"]) for k in sorted_short_klines[-self.short_boll.period:]]
            
            # 更新布林带
            if len(long_closes) >= self.long_boll.period:
                for price in long_closes:
                    self.long_boll.update(price)
            if len(short_closes) >= self.short_boll.period:
                for price in short_closes:
                    self.short_boll.update(price)
                
        # 更新最新价格
        if prices:
            self.last_price = prices[-1]
            self.last_update_time = current_time

    def _process_db_queue(self):
        """处理数据库操作队列"""
        try:
            while True:
                try:
                    # 非阻塞方式获取消息
                    msg = self.db_queue.get_nowait()
                    action = msg.get('action')
                    data = msg.get('data')

                    if action == 'update_position':
                        try:
                            order_data = data
                            logger.info(f"开始处理持仓更新: {order_data}")
                            
                            size, cost = self.db.get_position(self.symbol)
                            logger.info(f"当前持仓: size={size}, cost={cost}")
                            
                            # 计算新的持仓
                            order_size = Decimal(str(order_data['quantity']))
                            order_price = Decimal(str(order_data['price']))
                            
                            # 修正：使用 'Bid' 而不是 'buy' 来判断买入
                            if order_data['side'].upper() == 'BID':
                                new_size = size + order_size
                                new_cost = cost + (order_size * order_price)
                                logger.info(f"买入更新: +{order_size} @ {order_price}")
                            else:  # Ask
                                new_size = size - order_size
                                # 修正：卖出时的成本计算
                                if size > 0:
                                    # 按比例减少成本
                                    cost_reduction = (order_size / size) * cost
                                    new_cost = cost - cost_reduction
                                else:
                                    new_cost = Decimal('0')
                                logger.info(f"卖出更新: -{order_size} @ {order_price}")
                            
                            # 确保数值不会出现负数
                            new_size = max(Decimal('0'), new_size)
                            new_cost = max(Decimal('0'), new_cost)
                            
                            # 更新数据库 - 转换为 float
                            self.db.update_position(self.symbol, float(new_size), float(new_cost))
                            logger.info(f"持仓更新成功: new_size={new_size}, new_cost={new_cost}")
                            
                            # 记录交易历史 - 转换为 float
                            self.db.add_trade(
                                self.symbol,
                                order_data['side'],
                                float(order_price),  # 转换为 float
                                float(order_size)    # 转换为 float
                            )
                            logger.info("交易历史记录添加成功")
                        except Exception as e:
                            logger.error(f"处理持仓更新时发生错误: {e}")
                            logger.exception("详细错误信息：")  # 添加详细的错误堆栈信息
                    elif action == 'get_position':
                        size, cost = self.db.get_position(self.symbol)
                        with self.cache_lock:
                            self.position_cache = {
                                'size': float(size),
                                'avg_price': float(cost) / float(size) if float(size) != 0 else 0
                            }
                            self.last_position_update = time.time()
                        self.position_event.set()

                except queue.Empty:
                    break
                    
        except Exception as e:
            self.logger.error(f"处理数据库队列时发生错误: {e}")
            if action == 'get_position':
                self.position_event.set()  # 确保在错误情况下也设置事件

    def _get_cached_position(self):
        """获取缓存的持仓信息"""
        current_time = time.time()
        if self.position_cache is None or current_time - self.last_position_update > self.position_update_interval:
            # 重置事件
            self.position_event.clear()
            
            # 请求更新持仓信息
            self.db_queue.put({
                'action': 'get_position',
                'data': None
            })
            
            # 等待更新完成
            self.position_event.wait(timeout=1.0)  # 设置超时以防止死锁
            
        return self.position_cache or {'size': 0, 'avg_price': 0}

    def update_position(self, order_data: dict):
        """更新持仓信息"""
        try:
            # 将更新操作放入队列
            self.db_queue.put({
                'action': 'update_position',
                'data': order_data
            })
        except Exception as e:
            self.logger.error(f"添加更新持仓任务失败: {e}")
            raise

    def _handle_ws_message(self, stream, event_data):
        """处理WebSocket消息回调"""
        try:
            # 处理行情数据
            if stream.startswith("bookTicker."):
                if 'b' in event_data and 'a' in event_data:
                    bid_price = float(event_data['b'])
                    ask_price = float(event_data['a'])
                    current_price = (bid_price + ask_price) / 2
                    current_time = time.time()
                    
                    # 直接更新价格（不需要锁，允许竞争）
                    self.last_price = current_price
                    self.last_update_time = current_time
                    
                    # 检查是否需要调整订单
                    if current_time - self.last_order_time >= self.order_interval:
                        if not self.init_complete.is_set():
                            logger.debug("等待K线数据初始化完成...")
                            return
                            
                        position = self._get_cached_position()
                        position_cost = position["avg_price"] if position else 0
                        self._adjust_orders(current_price, position_cost)
                        self.last_order_time = current_time
                    
            # 处理深度数据
            elif stream.startswith("depth."):
                if 'b' in event_data and 'a' in event_data:
                    # 这里可以添加处理深度数据的逻辑
                    pass
                    
            # 处理订单更新
            elif stream.startswith("account.orderUpdate."):
                event_type = event_data.get('e')
                
                # 处理订单成交事件
                if event_type == 'orderFill':
                    order_id = event_data.get('i')
                    side = event_data.get('S')
                    quantity = float(event_data.get('l', '0'))
                    price = float(event_data.get('p', '0'))
                    
                    # 更新持仓信息
                    order_data = {
                        'side': side,
                        'quantity': quantity,
                        'price': price
                    }
                    self.update_position(order_data)
                    
        except Exception as e:
            self.logger.error(f"处理WebSocket消息失败: {str(e)}")

    def _monitor_price(self):
        """监控价格变化"""
        while self.running:
            try:
                # 定期更新持仓缓存
                self._get_cached_position()
                time.sleep(1)
            except Exception as e:
                logger.error(f"监控价格失败: {str(e)}")
                time.sleep(5)  # 发生错误时等待一段时间再继续

    def _calculate_dynamic_spread(self, current_price):
        """
        计算动态价差
        :param current_price: 当前价格
        :return: (ask_spread, bid_spread) 卖出价差和买入价差
        """
        try:
            if not self.config["DYNAMIC_SPREAD"]:
                return self.config["SPREAD"], self.config["SPREAD"]

            # 获取布林带数据
            short_upper, _, short_lower = self.short_boll.get_bands()
            
            # 计算波动率（使用布林带范围相对于当前价格的比例）
            volatility = abs(short_upper - short_lower) / current_price

            # 记录波动率用于调试
            self.logger.debug(f"波动率计算 - 上轨: {short_upper}, 下轨: {short_lower}, " +
                          f"当前价格: {current_price}, 波动率: {volatility:.4%}")

            # 根据波动率计算基础价差
            if volatility <= 0.0025:  # 低波动率 (0.25%)
                base_spread = self.config["SPREAD_MIN"]
            elif volatility >= 0.05:  # 高波动率 (5%)
                base_spread = self.config["SPREAD_MAX"]
            else:
                # 在中间范围内线性插值
                spread_range = self.config["SPREAD_MAX"] - self.config["SPREAD_MIN"]
                normalized_vol = (volatility - 0.0025) / (0.05 - 0.0025)
                base_spread = self.config["SPREAD_MIN"] + spread_range * normalized_vol

            # 应用趋势偏移
            if self.config["TREND_SKEW"]:
                sma = self.short_boll.get_sma()
                if current_price > sma:  # 上升趋势
                    # 在上升趋势中，降低卖出价差使其更容易成交，同时提高买入价差以控制风险
                    # 使用(2-SKEW)确保买卖价差的总和保持不变，只是分配比例发生变化
                    ask_spread = base_spread * self.config["UPTREND_SKEW"]  # UPTREND_SKEW < 1，降低卖出价差
                    bid_spread = base_spread * (2 - self.config["UPTREND_SKEW"])  # 相应提高买入价差
                else:  # 下降趋势
                    # 在下降趋势中，提高卖出价差以控制风险，同时降低买入价差使其更容易成交
                    ask_spread = base_spread * self.config["DOWNTREND_SKEW"]  # DOWNTREND_SKEW > 1，提高卖出价差
                    bid_spread = base_spread * (2 - self.config["DOWNTREND_SKEW"])  # 相应降低买入价差
            else:
                ask_spread = bid_spread = base_spread

            # 确保价差在配置的范围内
            ask_spread = max(min(ask_spread, self.config["SPREAD_MAX"]), self.config["SPREAD_MIN"])
            bid_spread = max(min(bid_spread, self.config["SPREAD_MAX"]), self.config["SPREAD_MIN"])

            # 记录最终价差用于调试
            self.logger.debug(f"价差计算 - 基础价差: {base_spread:.4f}, " +
                          f"最终卖出价差: {ask_spread:.4f}, 买入价差: {bid_spread:.4f}")

            return ask_spread, bid_spread

        except Exception as e:
            self.logger.error(f"计算动态价差失败: {str(e)}")
            return self.config["SPREAD"], self.config["SPREAD"]

    def _check_risk_control(self, current_price: float, position_cost: float) -> bool:
        """检查风控条件"""
        try:
            if position_cost <= 0:
                return True
            
            # 计算当前收益率
            roi = (current_price - position_cost) / position_cost
            
            # 检查止损条件
            if abs(roi) >= self.config["STOP_LOSS_ACTIVATION"]:
                if roi < 0 and abs(roi) >= self.config["STOP_LOSS_RATIO"]:
                    logger.warning(f"触发止损: ROI={roi:.2%}, 当前价格={current_price}, 持仓成本={position_cost}")
                    # 市价卖出止损
                    self._close_position()
                    return False
            
            # 检查止盈条件
            if roi >= self.config["TAKE_PROFIT_RATIO"]:
                logger.info(f"触发止盈: ROI={roi:.2%}, 当前价格={current_price}, 持仓成本={position_cost}")
                # 市价卖出止盈
                self._close_position()
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"检查风控条件失败: {str(e)}")
            return True

    def _close_position(self):
        """市价平仓"""
        try:
            position = self._get_cached_position()
            if position and position["size"] > 0:
                order_details = {
                    "symbol": self.symbol,
                    "side": "Ask",
                    "orderType": "Market",
                    "quantity": str(position["size"]),
                    "timeInForce": "IOC"
                }
                order = self.rest_client.place_order(order_details)
                if order and "id" in order:
                    logger.info(f"市价平仓成功: 数量={position['size']}")
                
        except Exception as e:
            logger.error(f"市价平仓失败: {str(e)}")

    def _adjust_orders(self, current_price: float, position_cost: float):
        """根据当前价格和持仓成本调整订单"""
        try:
            # 检查风控条件
            if not self._check_risk_control(current_price, position_cost):
                return
            
            # 获取布林带数据的副本（最小化锁的持有时间）
            with self.boll_lock:
                long_bands = self.long_boll.get_bands()
                short_bands = self.short_boll.get_bands()
            
            # 使用数据副本进行计算（不需要锁）
            long_upper, long_middle, long_lower = long_bands
            short_upper, short_middle, short_lower = short_bands
            
            # 计算目标持仓比例
            position_scale = self._calculate_position_scale(
                current_price,
                long_upper, long_lower,
                short_upper, short_lower
            )
            
            # 取消现有订单
            self.rest_client.cancel_all_orders(self.symbol)
            self.current_orders = {}
            
            # 计算动态价差
            ask_spread, bid_spread = self._calculate_dynamic_spread(current_price)
            
            # 计算买卖价格
            buy_price = round(current_price * (1 - bid_spread), self.config["PRICE_PRECISION"])
            sell_price = round(current_price * (1 + ask_spread), self.config["PRICE_PRECISION"])
            
            # 检查是否满足交易条件 - 增加更严格的买入条件和更宽松的卖出条件
            can_buy = True
            can_sell = True
            
            if self.config["TRADE_IN_BAND"]:
                can_buy = self.short_boll.is_price_in_band(current_price, short_upper, short_lower)
                can_sell = self.short_boll.is_price_in_band(current_price, short_upper, short_lower)
                
            # 更严格的买入条件
            if self.config["BUY_BELOW_SMA"]:
                can_buy = can_buy and current_price < short_middle
                
            # 新增：更宽松的卖出条件
            if self.config.get("SELL_ABOVE_SMA", False):
                # 只要价格不是明显低于均线就可以卖出
                can_sell = can_sell and current_price >= short_middle * 0.995  # 允许稍微低于均线0.5%
                
            # 检查最小利润 - 降低卖出门槛
            if position_cost > 0:
                min_sell_price = round(position_cost * (1 + self.config["MIN_PROFIT_SPREAD"]), self.config["PRICE_PRECISION"])
                can_sell = can_sell and sell_price > min_sell_price
            else:
                # 没有持仓成本时，更容易卖出
                can_sell = True
                
            # 获取当前持仓信息，用于仓位控制
            current_position = self._get_cached_position()
            current_position_size = current_position.get("size", 0) if current_position else 0
            
            # 计算当前持仓价值占总资金的比例
            if current_position_size > 0 and current_price > 0:
                position_value = current_position_size * current_price
                position_ratio = position_value / self.config["GRID_TOTAL_INVESTMENT"]
                
                # 如果持仓比例超过最大限制，停止买入，强制卖出
                if position_ratio > self.config.get("MAX_POSITION_RATIO", 0.3):
                    can_buy = False
                    can_sell = True  # 强制允许卖出
                    logger.warning(f"持仓比例过高 {position_ratio:.2%}，停止买入，优先卖出")
            
            # 额外的保守买入条件：只在价格明显偏低时才买入
            if can_buy:
                # 只有当价格低于短期布林带中轨一定幅度时才买入
                buy_threshold = short_middle * 0.99  # 低于均线1%才买入
                can_buy = can_buy and current_price < buy_threshold
                
            # 计算订单数量 - 修改为卖单优先策略
            base_order_size = self.config["BASE_ORDER_SIZE"]
            quote_order_size = self.config["QUOTE_ORDER_SIZE"]
            
            # 降低最大订单限制，控制风险
            max_order_size = base_order_size * 3  # 从5倍降到3倍
            
            # 重新设计订单大小计算逻辑
            # 买单：更保守，基础大小 * 较小的比例
            buy_scale = min(position_scale * 0.7, 1.0)  # 最多70%的比例用于买单
            buy_size = min(max(base_order_size * buy_scale, base_order_size * 0.5), max_order_size)
            
            # 卖单：更激进，基础大小 * 更大的比例
            sell_scale = max(1.0, (2.0 - position_scale) * self.config.get("SELL_BIAS_MULTIPLIER", 1.5))
            sell_size = min(max(base_order_size * sell_scale, base_order_size), max_order_size)
            
            # 如果持仓过多，进一步增加卖单大小，减少买单大小
            if current_position_size > 0:
                position_value = current_position_size * current_price
                position_ratio = position_value / self.config["GRID_TOTAL_INVESTMENT"]
                
                if position_ratio > 0.2:  # 持仓超过20%时
                    # 减少买单大小
                    buy_size = buy_size * (1 - position_ratio)
                    # 增加卖单大小
                    sell_size = sell_size * (1 + position_ratio * 0.5)
                    sell_size = min(sell_size, max_order_size)
            
            # 格式化数量到指定精度（确保不超过3位小数）
            precision = self.config["QUANTITY_PRECISION"]
            buy_size = round(buy_size, precision)
            sell_size = round(sell_size, precision)
            
            # 确保数量大于0
            if buy_size <= 0 or sell_size <= 0:
                logger.warning(f"订单数量异常: buy_size={buy_size}, sell_size={sell_size}, position_scale={position_scale}")
                return
            
            # 下买单
            if can_buy:
                try:
                    order_details = {
                        "symbol": self.symbol,
                        "side": "Bid",
                        "orderType": "Limit",
                        "quantity": str(buy_size),
                        "price": format(buy_price, f".{self.config['PRICE_PRECISION']}f"),
                        "timeInForce": "GTC",
                        "postOnly": True
                    }
                    order = self.rest_client.place_order(order_details)
                    if order and "id" in order:
                        self.current_orders[order["id"]] = order
                        logger.info(f"下买单成功: 价格={buy_price}, 数量={buy_size}")
                except Exception as e:
                    logger.error(f"下买单失败: {str(e)}")
                    
            # 下卖单
            if can_sell:
                try:
                    order_details = {
                        "symbol": self.symbol,
                        "side": "Ask",
                        "orderType": "Limit",
                        "quantity": str(sell_size),
                        "price": format(sell_price, f".{self.config['PRICE_PRECISION']}f"),
                        "timeInForce": "GTC",
                        "postOnly": True
                    }
                    order = self.rest_client.place_order(order_details)
                    if order and "id" in order:
                        self.current_orders[order["id"]] = order
                        logger.info(f"下卖单成功: 价格={sell_price}, 数量={sell_size}")
                except Exception as e:
                    logger.error(f"下卖单失败: {str(e)}")
                    
        except Exception as e:
            logger.error(f"调整订单失败: {str(e)}")
            
    def _calculate_position_scale(self, current_price: float,
                                long_upper: float, long_lower: float,
                                short_upper: float, short_lower: float) -> float:
        """计算目标持仓比例 - 修改为更保守的买入策略"""
        try:
            # 不需要加锁，因为使用的是数据副本
            if not all([long_upper, long_lower, short_upper, short_lower]):
                logger.warning("布林带数据未完全初始化，返回保守仓位")
                return self.config["MIN_POSITION_SCALE"]

            # 检查分母是否为零或接近零
            long_range = long_upper - long_lower
            short_range = short_upper - short_lower
            
            min_range = 0.0001  # 定义最小有效范围
            
            if long_range <= min_range or short_range <= min_range:
                logger.warning("布林带范围过小，返回保守仓位")
                return self.config["MIN_POSITION_SCALE"]
            
            # 计算价格在长期和短期布林带中的位置
            long_position = (current_price - long_lower) / long_range
            short_position = (current_price - short_lower) / short_range
            
            # 限制position值在[0,1]范围内
            long_position = max(0.0, min(1.0, long_position))
            short_position = max(0.0, min(1.0, short_position))
            
            # 修改：减少反转程度，让仓位变化更平缓
            # 原来是完全反转(1.0 - position)，现在只部分反转
            long_position = 0.8 - 0.6 * long_position  # 从[0,1]映射到[0.8,0.2]
            short_position = 0.8 - 0.6 * short_position
            
            # 综合长期和短期指标
            position_scale = (long_position + short_position) / 2
            
            # 应用仓位偏移（负值表示倾向于减少持仓）
            position_scale = position_scale + self.config["POSITION_BIAS"]
            position_scale = max(0.1, min(1.0, position_scale))  # 限制在合理范围
            
            # 应用配置的仓位范围
            min_scale = self.config["MIN_POSITION_SCALE"]
            max_scale = self.config["MAX_POSITION_SCALE"]
            
            # 将position_scale映射到配置的范围
            position_scale = min_scale + (max_scale - min_scale) * position_scale
            
            logger.debug(f"计算仓位比例 - 当前价格: {current_price}, 长期位置: {long_position:.4f}, " +
                      f"短期位置: {short_position:.4f}, 偏移后: {position_scale:.4f}")
            
            return position_scale
                
        except Exception as e:
            logger.error(f"计算持仓比例失败: {str(e)}")
            return self.config["MIN_POSITION_SCALE"]  # 发生错误时返回最小仓位
            
    def subscribe_order_updates(self):
        """订阅订单更新流"""
        if not self.ws_client or not self.ws_client.is_connected():
            logger.warning("无法订阅订单更新：WebSocket连接不可用")
            return False
        
        # 尝试订阅订单更新流
        stream = f"account.orderUpdate.{self.symbol}"
        if stream not in self.ws_client.subscriptions:
            retry_count = 0
            max_retries = 3
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    success = self.ws_client.private_subscribe(stream)
                    if success:
                        logger.info(f"成功订阅订单更新: {stream}")
                        return True
                    else:
                        logger.warning(f"订阅订单更新失败，尝试重试... ({retry_count+1}/{max_retries})")
                except Exception as e:
                    logger.error(f"订阅订单更新时发生异常: {e}")
                
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)  # 重试前等待
            
            if not success:
                logger.error(f"在 {max_retries} 次尝试后仍无法订阅订单更新")
                return False
        else:
            logger.info(f"已经订阅了订单更新: {stream}")
            return True

    def _update_kline_data(self):
        """更新K线数据并计算布林带"""
        try:
            # 获取K线数据（不需要锁）
            long_klines = self.rest_client.get_klines(
                self.symbol,
                interval=self.config["LONG_BOLL_INTERVAL"],
                limit=self.config["LONG_BOLL_PERIOD"] * 2
            )
            
            short_klines = self.rest_client.get_klines(
                self.symbol,
                interval=self.config["SHORT_BOLL_INTERVAL"],
                limit=self.config["SHORT_BOLL_PERIOD"] * 2
            )
            
            # 检查K线数据
            if not long_klines or not short_klines:
                logger.error(f"获取K线数据失败 - 长周期: {len(long_klines) if long_klines else 0} 根, " +
                           f"短周期: {len(short_klines) if short_klines else 0} 根")
                return
                
            # 记录原始数据格式以便调试
            logger.debug(f"长周期K线数据格式: {type(long_klines)}, 数据: {long_klines[:1]}")
            logger.debug(f"短周期K线数据格式: {type(short_klines)}, 数据: {short_klines[:1]}")
                
            # 准备数据（不需要锁）
            if long_klines:
                try:
                    # 确保数据是列表格式
                    if isinstance(long_klines, dict):
                        long_klines = [long_klines]
                    
                    # 按开始时间排序
                    sorted_long_klines = sorted(long_klines, key=lambda x: x['start'] if isinstance(x, dict) else x[0])
                    recent_long_klines = sorted_long_klines[-self.config["LONG_BOLL_PERIOD"]:]
                    
                    # 获取收盘价
                    long_closes = []
                    for k in recent_long_klines:
                        if isinstance(k, dict):
                            long_closes.append(float(k['close']))
                        else:
                            long_closes.append(float(k[4]))  # 第4个元素是收盘价
                    
                    # 更新长周期布林带
                    with self.boll_lock:
                        self.long_boll = BollingerBands(
                            period=self.config["LONG_BOLL_PERIOD"],
                            num_std=self.config["LONG_BOLL_STD"]
                        )
                        for price in long_closes:
                            self.long_boll.update(price)
                        
                    logger.debug(f"更新长周期布林带 - 使用 {len(long_closes)} 根K线")
                except Exception as e:
                    logger.error(f"处理长周期K线数据时出错: {str(e)}")
            
            if short_klines:
                try:
                    # 确保数据是列表格式
                    if isinstance(short_klines, dict):
                        short_klines = [short_klines]
                    
                    # 按开始时间排序
                    sorted_short_klines = sorted(short_klines, key=lambda x: x['start'] if isinstance(x, dict) else x[0])
                    recent_short_klines = sorted_short_klines[-self.config["SHORT_BOLL_PERIOD"]:]
                    
                    # 获取收盘价
                    short_closes = []
                    for k in recent_short_klines:
                        if isinstance(k, dict):
                            short_closes.append(float(k['close']))
                        else:
                            short_closes.append(float(k[4]))  # 第4个元素是收盘价
                    
                    # 更新短周期布林带
                    with self.boll_lock:
                        self.short_boll = BollingerBands(
                            period=self.config["SHORT_BOLL_PERIOD"],
                            num_std=self.config["SHORT_BOLL_STD"]
                        )
                        for price in short_closes:
                            self.short_boll.update(price)
                        
                    logger.debug(f"更新短周期布林带 - 使用 {len(short_closes)} 根K线")
                except Exception as e:
                    logger.error(f"处理短周期K线数据时出错: {str(e)}")
            
            # 更新最新价格（不需要锁，允许竞争）
            if short_klines and len(short_klines) > 0:
                latest_kline = short_klines[-1]
                try:
                    if isinstance(latest_kline, dict):
                        # 将时间字符串转换为时间戳
                        latest_time = datetime.strptime(latest_kline['end'], '%Y-%m-%d %H:%M:%S').timestamp()
                        latest_price = float(latest_kline['close'])
                    else:
                        latest_time = float(latest_kline[0])
                        latest_price = float(latest_kline[4])
                        
                    if latest_time > self.last_update_time:
                        self.last_price = latest_price
                        self.last_update_time = latest_time
                        logger.debug(f"从K线更新最新价格: {self.last_price}")
                except Exception as e:
                    logger.error(f"更新最新价格时出错: {str(e)}")
                    if not isinstance(e, str):
                        import traceback
                        logger.error(f"详细错误信息: {traceback.format_exc()}")
            
            # 标记初始化完成
            if not self.init_complete.is_set():
                # 使用 is_ready 方法检查布林带是否准备就绪
                with self.boll_lock:
                    long_ready = self.long_boll.is_ready()
                    short_ready = self.short_boll.is_ready()
                
                if long_ready and short_ready:
                    self.init_complete.set()
                    logger.info("K线数据初始化完成")
                else:
                    logger.debug(f"布林带数据尚未准备就绪 - 长周期: {len(self.long_boll.prices)}/{self.config['LONG_BOLL_PERIOD']}, " +
                               f"短周期: {len(self.short_boll.prices)}/{self.config['SHORT_BOLL_PERIOD']}")
                
        except Exception as e:
            logger.error(f"更新K线数据失败: {str(e)}")
            if not isinstance(e, str):
                import traceback
                logger.error(f"详细错误信息: {traceback.format_exc()}")

    def _initialize_websocket(self):
        """等待WebSocket连接建立并进行初始化订阅"""
        wait_time = 0
        max_wait_time = 30  # 增加等待时间到30秒
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                wait_time = 0
                while not self.ws_client.connected and wait_time < max_wait_time:
                    time.sleep(1)
                    wait_time += 1
                    
                if self.ws_client.connected:
                    logger.info("WebSocket连接已建立，初始化数据流...")
                    
                    # 初始化订单簿
                    orderbook_initialized = self.ws_client.initialize_orderbook()
                    
                    # 订阅深度流和行情数据
                    if orderbook_initialized:
                        depth_subscribed = self.ws_client.subscribe_depth()
                        ticker_subscribed = self.ws_client.subscribe_bookTicker()
                        
                        if depth_subscribed and ticker_subscribed:
                            logger.info("行情数据流订阅成功!")
                            
                        # 订阅私有订单更新流
                        if self.subscribe_order_updates():
                            logger.info("订单更新流订阅成功!")
                            return True  # 所有初始化成功
                        else:
                            logger.error("订单更新流订阅失败")
                    else:
                        logger.error("订单簿初始化失败")
                
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"WebSocket初始化失败，正在进行第{retry_count + 1}次重试...")
                    self.ws_client.reconnect()
                    time.sleep(5)  # 重试前等待5秒
                    
            except Exception as e:
                logger.error(f"WebSocket初始化发生错误: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(5)
                    
        logger.error(f"在{max_retries}次尝试后WebSocket初始化仍然失败")
        return False

    def _check_and_reconnect_ws(self):
        """检查WebSocket连接状态并在需要时重连"""
        if not self.ws_client.connected:
            logger.warning("检测到WebSocket连接断开，尝试重新连接...")
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries and not self.ws_client.connected:
                try:
                    self.ws_client.reconnect()
                    time.sleep(2)  # 等待连接建立
                    
                    if self.ws_client.connected:
                        success = self._initialize_websocket()
                        if success:
                            logger.info("WebSocket重连成功并完成初始化")
                            return True
                            
                except Exception as e:
                    logger.error(f"WebSocket重连尝试失败: {str(e)}")
                    
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 5 * (retry_count + 1)  # 递增等待时间
                    logger.info(f"等待{wait_time}秒后进行第{retry_count + 1}次重试...")
                    time.sleep(wait_time)
            
            if not self.ws_client.connected:
                logger.error("WebSocket重连失败，将在下一个检查周期重试")
                return False
                
        return True

    def _kline_update_worker(self):
        """K线数据更新工作线程"""
        logger.info("K线更新线程已启动")
        while self.running:
            try:
                self._update_kline_data()
                time.sleep(self.kline_update_interval)
            except Exception as e:
                logger.error(f"K线更新线程发生错误: {str(e)}")
                time.sleep(5)  # 发生错误时等待较短时间后继续

    def start(self):
        """启动交易机器人"""
        try:
            self.running = True
            
            # 连接WebSocket并初始化数据流
            self.ws_client.connect()
            if not self._initialize_websocket():
                logger.error("WebSocket初始化失败，无法启动机器人")
                self.stop()
                return
            
            # 启动K线更新线程
            self.kline_update_thread = threading.Thread(
                target=self._kline_update_worker,
                daemon=True
            )
            self.kline_update_thread.start()
            
            # 等待初始化完成
            if not self.init_complete.wait(timeout=30):
                logger.error("K线数据初始化超时")
                self.stop()
                return
            
            logger.info("交易机器人已启动")
            
            last_ws_check = time.time()
            ws_check_interval = 30  # 每30秒检查一次WebSocket状态
            
            # 主循环
            while self.running:
                try:
                    current_time = time.time()
                    
                    # 处理数据库操作队列
                    self._process_db_queue()
                    
                    # 定期检查WebSocket连接状态
                    if current_time - last_ws_check >= ws_check_interval:
                        if not self._check_and_reconnect_ws():
                            # 如果重连失败，增加检查间隔以避免过于频繁的重试
                            ws_check_interval = min(ws_check_interval * 2, 300)  # 最大间隔5分钟
                        else:
                            ws_check_interval = 30  # 重置为正常间隔
                        last_ws_check = current_time
                    
                    time.sleep(0.1)  # 控制循环频率
                    
                except KeyboardInterrupt:
                    logger.info("收到退出信号，正在停止机器人...")
                    self.stop()
                    break
                except Exception as e:
                    logger.error(f"主循环发生错误: {str(e)}")
                    time.sleep(5)  # 发生错误时等待一段时间再继续
                    
        except Exception as e:
            logger.error(f"启动失败: {str(e)}")
            self.stop()

    def stop(self):
        """停止交易机器人"""
        try:
            self.running = False
            
            # 等待K线更新线程结束
            if self.kline_update_thread and self.kline_update_thread.is_alive():
                try:
                    self.kline_update_thread.join(timeout=5)
                except Exception as e:
                    logger.error(f"等待K线更新线程结束时发生错误: {str(e)}")
            
            # 取消所有订单
            self.rest_client.cancel_all_orders(self.symbol)
            
            # 关闭WebSocket连接
            if self.ws_client:
                self.ws_client.close()
            
            logger.info("交易机器人已停止")
        except Exception as e:
            logger.error(f"停止失败: {str(e)}")

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db'):
            self.db.close()

    def _calculate_total_balance(self, include_borrow_positions=True) -> Tuple[float, float, float, Optional[str]]:
        """
        计算账户总余额，包括现货和借贷仓位

        Args:
            include_borrow_positions (bool): 是否包含借贷仓位，默认为True

        Returns:
            Tuple[float, float, float, Optional[str]]: 
                - base_balance (float): 基础货币余额（包括现货和借贷）
                - quote_balance (float): 报价货币余额（包括现货和借贷）
                - total_value_in_quote (float): 总资产价值（以报价货币计算）
                - error_message (Optional[str]): 错误信息，如果没有错误则为None
        """
        try:
            # 获取现货余额
            balances = self.rest_client.get_balance()
            logger.info(f"API返回的余额数据: {balances}")  # 添加日志查看数据格式
            
            # 检查返回的数据是否有错误
            if isinstance(balances, dict) and "error" in balances:
                error_msg = f"获取余额API返回错误: {balances['error']}"
                logger.error(error_msg)
                return 0.0, 0.0, 0.0, error_msg
            
            # 从交易对中获取基础货币和计价货币
            base_currency, quote_currency = self.symbol.split('_')
            
            # 初始化现货余额
            spot_base_balance = 0
            spot_quote_balance = 0
            
            # 计算现货总余额（可用 + 锁定）
            if isinstance(balances, dict):
                for asset, details in balances.items():
                    if isinstance(details, dict):
                        available = float(details.get('available', 0))
                        locked = float(details.get('locked', 0))
                        total = available + locked
                        
                        if asset == base_currency:
                            spot_base_balance = total
                        elif asset == quote_currency:
                            spot_quote_balance = total
                    else:
                        logger.warning(f"资产 {asset} 的余额数据格式不正确: {details}")
            else:
                logger.warning(f"余额数据格式不正确: {balances}")
            
            # 初始化借贷调整
            borrow_lend_base_adjustment = 0
            borrow_lend_quote_adjustment = 0
            
            # 获取并处理借贷仓位
            if include_borrow_positions:
                try:
                    borrow_positions = self.rest_client.get_borrow_lend_positions()
                    if isinstance(borrow_positions, list):
                        for position in borrow_positions:
                            position_symbol = position.get('symbol')
                            net_quantity_str = position.get('netQuantity')
                            
                            if position_symbol and net_quantity_str:
                                try:
                                    net_quantity = float(net_quantity_str)
                                    if position_symbol == base_currency:
                                        borrow_lend_base_adjustment = net_quantity
                                        logger.debug(f"借贷调整 - 基础资产 ({base_currency}): {net_quantity}")
                                    elif position_symbol == quote_currency:
                                        borrow_lend_quote_adjustment = net_quantity
                                        logger.debug(f"借贷调整 - 报价资产 ({quote_currency}): {net_quantity}")
                                except ValueError as e:
                                    logger.error(f"处理借贷仓位数据时出错 ({position_symbol}): {e}")
                                    continue
                except Exception as e:
                    logger.warning(f"获取借贷仓位失败: {str(e)}")
                    # 继续执行，但不包括借贷仓位
            
            # 计算最终余额（现货余额 + 借贷调整）
            base_balance = spot_base_balance + borrow_lend_base_adjustment
            quote_balance = spot_quote_balance + borrow_lend_quote_adjustment
            
            # 获取当前价格用于计算总价值
            current_price = self.last_price  # 使用最新价格
            if not current_price:
                logger.warning("无法获取当前价格，总资产价值可能不准确")
                total_value_in_quote = quote_balance  # 假设基础资产价值为0
            else:
                total_value_in_quote = (base_balance * current_price) + quote_balance
            
            logger.info(f"计算后余额 - 基础({base_currency}): {base_balance:.8f} (现货: {spot_base_balance:.8f}, 借贷: {borrow_lend_base_adjustment:.8f})")
            logger.info(f"计算后余额 - 报价({quote_currency}): {quote_balance:.8f} (现货: {spot_quote_balance:.8f}, 借贷: {borrow_lend_quote_adjustment:.8f})")
            logger.info(f"计算后总价值: {total_value_in_quote:.8f} {quote_currency}")
            
            return base_balance, quote_balance, total_value_in_quote, None
            
        except Exception as e:
            error_msg = f"计算总余额时发生错误: {str(e)}"
            logger.error(error_msg)
            import traceback
            traceback.print_exc()
            return 0.0, 0.0, 0.0, error_msg

if __name__ == "__main__":
    # 构建配置字典
    config = {
        "API_KEY": API_KEY,
        "SECRET_KEY": SECRET_KEY,
        "SYMBOL": SYMBOL,
        "ORDER_AMOUNT": ORDER_AMOUNT,
        "GRID_TOTAL_INVESTMENT": GRID_TOTAL_INVESTMENT,
        "PRICE_PRECISION": PRICE_PRECISION,
        "QUANTITY_PRECISION": QUANTITY_PRECISION,
        "SPREAD": SPREAD,
        "LONG_BOLL_PERIOD": LONG_BOLL_PERIOD,
        "LONG_BOLL_STD": LONG_BOLL_STD,
        "SHORT_BOLL_PERIOD": SHORT_BOLL_PERIOD,
        "SHORT_BOLL_STD": SHORT_BOLL_STD,
        "MAX_POSITION_SCALE": MAX_POSITION_SCALE,
        "MIN_POSITION_SCALE": MIN_POSITION_SCALE,
        "MIN_PROFIT_SPREAD": MIN_PROFIT_SPREAD,
        "TRADE_IN_BAND": TRADE_IN_BAND,
        "BUY_BELOW_SMA": BUY_BELOW_SMA,
        "SELL_ABOVE_SMA": SELL_ABOVE_SMA,
        "BASE_ORDER_SIZE": BASE_ORDER_SIZE,
        "QUOTE_ORDER_SIZE": QUOTE_ORDER_SIZE,
        "LONG_BOLL_INTERVAL": LONG_BOLL_INTERVAL,
        "SHORT_BOLL_INTERVAL": SHORT_BOLL_INTERVAL,
        "DYNAMIC_SPREAD": DYNAMIC_SPREAD,
        "SPREAD_MIN": SPREAD_MIN,
        "SPREAD_MAX": SPREAD_MAX,
        "TREND_SKEW": TREND_SKEW,
        "UPTREND_SKEW": UPTREND_SKEW,
        "DOWNTREND_SKEW": DOWNTREND_SKEW,
        "STOP_LOSS_ACTIVATION": STOP_LOSS_ACTIVATION,
        "STOP_LOSS_RATIO": STOP_LOSS_RATIO,
        "TAKE_PROFIT_RATIO": TAKE_PROFIT_RATIO,
        "POSITION_BIAS": POSITION_BIAS,
        "MAX_POSITION_RATIO": MAX_POSITION_RATIO,
        "SELL_BIAS_MULTIPLIER": SELL_BIAS_MULTIPLIER
    }
    
    bot = None
    try:
        # 初始化并启动机器人
        bot = BollMakerBot(config)
        bot.start()
        
        # 主循环，保持程序运行
        while bot.running:
            try:
                time.sleep(1)
                
                # 检查WebSocket连接状态
                if bot.running and not bot.ws_client.connected:
                    logger.warning("WebSocket连接断开，尝试重新连接...")
                    bot.ws_client.reconnect()
                    
            except KeyboardInterrupt:
                logger.info("收到退出信号，正在停止机器人...")
                if bot:
                    bot.stop()
                break
            except Exception as e:
                logger.error(f"主循环发生错误: {str(e)}")
                time.sleep(5)  # 发生错误时等待一段时间再继续
                
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序发生错误: {str(e)}")
    finally:
        if bot:
            bot.stop()
        logger.info("程序已退出") 