# 策略优化：解决"买多卖少"问题

## 问题分析

原策略存在以下导致"买入快、卖出慢"的问题：

1. **仓位比例计算过于激进**：价格越低，买入量呈指数级增长
2. **订单大小不平衡**：买单随仓位比例大幅增加，卖单增幅较小
3. **交易条件不对称**：对买入有限制条件，但对卖出限制较少
4. **缺乏仓位上限控制**：没有最大持仓比例限制

## 优化方案

### 1. 配置参数调整

#### 仓位控制参数
- `MAX_POSITION_SCALE`: 10.0 → 3.0 (降低最大仓位倍数)
- `MIN_POSITION_SCALE`: 1.0 → 0.5 (降低最小仓位倍数)
- `MIN_PROFIT_SPREAD`: 0.001 → 0.0015 (提高最小获利要求)

#### 价差参数
- `SPREAD_MIN`: 0.001 → 0.0015 (提高最小价差)
- `SPREAD_MAX`: 0.002 → 0.003 (提高最大价差)

#### 趋势偏移参数
- `UPTREND_SKEW`: 0.8 → 0.6 (上涨时更激进卖出)
- `DOWNTREND_SKEW`: 1.2 → 1.4 (下跌时更保守买入)

#### 新增参数
- `SELL_ABOVE_SMA`: True (只在均线上方卖出)
- `POSITION_BIAS`: -0.3 (仓位偏移，负值倾向减仓)
- `MAX_POSITION_RATIO`: 0.3 (最大持仓比例30%)
- `SELL_BIAS_MULTIPLIER`: 1.5 (卖单大小倍数)

### 2. 仓位比例计算优化

#### 原逻辑问题
```python
# 原来：完全反转，价格越低仓位越大
long_position = 1.0 - long_position
short_position = 1.0 - short_position
```

#### 优化后逻辑
```python
# 现在：部分反转，变化更平缓
long_position = 0.8 - 0.6 * long_position  # [0,1] → [0.8,0.2]
short_position = 0.8 - 0.6 * short_position

# 应用仓位偏移（负值表示倾向减仓）
position_scale = position_scale + POSITION_BIAS
```

### 3. 交易条件优化

#### 更严格的买入条件
- 只在价格低于均线1%时才买入
- 持仓比例超过30%时停止买入
- 持仓比例超过20%时减少买单大小

#### 更宽松的卖出条件
- 允许在均线下方0.5%卖出
- 没有持仓成本时更容易卖出
- 持仓过多时强制允许卖出

### 4. 订单大小重新设计

#### 买单计算（更保守）
```python
buy_scale = min(position_scale * 0.7, 1.0)  # 最多70%比例
buy_size = min(max(base_order_size * buy_scale, base_order_size * 0.5), max_order_size)

# 持仓过多时进一步减少
if position_ratio > 0.2:
    buy_size = buy_size * (1 - position_ratio)
```

#### 卖单计算（更激进）
```python
sell_scale = max(1.0, (2.0 - position_scale) * SELL_BIAS_MULTIPLIER)
sell_size = min(max(base_order_size * sell_scale, base_order_size), max_order_size)

# 持仓过多时进一步增加
if position_ratio > 0.2:
    sell_size = sell_size * (1 + position_ratio * 0.5)
```

### 5. 风险控制增强

#### 仓位监控
- 实时监控持仓价值占总资金比例
- 超过最大比例时停止买入，优先卖出
- 动态调整订单大小以控制风险

#### 订单限制
- 最大订单限制从5倍降到3倍
- 买单最小为基础大小的50%
- 卖单最小为基础大小的100%

## 预期效果

1. **减少买入频率**：更严格的买入条件，只在明显低估时买入
2. **增加卖出频率**：更宽松的卖出条件，更大的卖单大小
3. **控制仓位风险**：最大持仓比例限制，动态仓位调整
4. **平衡收益风险**：虽然可能降低收益，但显著降低风险

## 使用建议

1. **监控持仓比例**：关注日志中的持仓比例警告
2. **观察订单平衡**：检查买卖单大小是否符合预期
3. **调整参数**：根据实际效果微调POSITION_BIAS和SELL_BIAS_MULTIPLIER
4. **风险评估**：定期评估策略表现，必要时进一步调整参数

## 参数微调指南

- 如果仍然买入过多：降低POSITION_BIAS（更负值）
- 如果卖出不够：提高SELL_BIAS_MULTIPLIER
- 如果风险过高：降低MAX_POSITION_RATIO
- 如果收益过低：适当放宽买入条件或降低价差 