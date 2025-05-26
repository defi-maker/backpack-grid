"""
配置文件
"""

import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# API配置
API_URL = "https://api.backpack.exchange"
WS_URL = "wss://ws.backpack.exchange"
API_VERSION = "v1"
DEFAULT_WINDOW = "5000"

# 交易配置
SYMBOL = "SOL_USDC"  # 交易对
ORDER_AMOUNT = 4  # 单次下单金额(USDC)，约为总资金的1/100
GRID_TOTAL_INVESTMENT = 400  # 总投资额(USDC)，包含SOL和USDC的总价值
PRICE_PRECISION = 2  # 价格精度
QUANTITY_PRECISION = 2  # 数量精度
SPREAD = 0.001  # 基础价差(0.1%)
BASE_ORDER_SIZE = 0.02  # 基础订单大小(SOL)
QUOTE_ORDER_SIZE = 4  # 基础订单大小(USDC)

# 布林带参数
LONG_BOLL_INTERVAL = "1h"  # 长周期布林带时间周期
LONG_BOLL_PERIOD = 21  # 长周期布林带K线数量
LONG_BOLL_STD = 2.0  # 长周期标准差倍数

SHORT_BOLL_INTERVAL = "5m"  # 短周期布林带时间周期
SHORT_BOLL_PERIOD = 21  # 短周期布林带K线数量
SHORT_BOLL_STD = 2.0  # 短周期标准差倍数

# 仓位控制
MAX_POSITION_SCALE = 3.0  # 降低最大仓位倍数，从10.0降到3.0，减少激进买入
MIN_POSITION_SCALE = 0.5  # 降低最小仓位倍数，从1.0降到0.5，减少基础买入量
MIN_PROFIT_SPREAD = 0.0015  # 提高最小获利价差，从0.001提高到0.0015，让卖出更容易盈利
TRADE_IN_BAND = True  # 是否只在布林带内交易
BUY_BELOW_SMA = True  # 是否只在均线下方买入
SELL_ABOVE_SMA = True  # 新增：是否只在均线上方卖出，增加卖出条件

# 动态价差配置
DYNAMIC_SPREAD = True  # 是否启用动态价差
SPREAD_MIN = 0.0015  # 提高最小价差，从0.001提高到0.0015
SPREAD_MAX = 0.003  # 提高最大价差，从0.002提高到0.003

# 趋势偏移配置 - 调整为更倾向卖出
TREND_SKEW = True  # 是否启用趋势偏移
UPTREND_SKEW = 0.6  # 上涨趋势时的偏移系数，从0.8降到0.6，更激进卖出
DOWNTREND_SKEW = 1.4  # 下跌趋势时的偏移系数，从1.2提高到1.4，更保守买入

# 风控参数
STOP_LOSS_ACTIVATION = 0.02  # 止损触发比例 2%
STOP_LOSS_RATIO = 0.03  # 止损比例 3%
TAKE_PROFIT_RATIO = 0.07  # 止盈比例 7%

# API密钥(请替换为您的密钥)
API_KEY = os.getenv("BACKPACK_API_KEY", "")
SECRET_KEY = os.getenv("BACKPACK_SECRET_KEY", "")

if not API_KEY or not SECRET_KEY:
    raise ValueError("请设置环境变量 BACKPACK_API_KEY 和 BACKPACK_SECRET_KEY")

# 新增：仓位控制参数
POSITION_BIAS = -0.3  # 仓位偏移，负值表示倾向于减少持仓（多卖少买）
MAX_POSITION_RATIO = 0.3  # 最大持仓比例，相对于总资金的30%
SELL_BIAS_MULTIPLIER = 1.5  # 卖单大小倍数，让卖单比买单大50%