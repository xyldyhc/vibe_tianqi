import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_excel('s1b_std_tap_lag.xlsx', sheet_name='sheet1')
data_column = df['std_lag']

# 生成箱线图
plt.figure(figsize=(10, 6))
box_plot = sns.boxplot(y=data_column, color='#8DA0CB', width=0.4)

# 获取箱线图的统计信息
stats = data_column.describe()
q1 = stats['25%']
median = stats['50%']
q3 = stats['75%']
iqr = q3 - q1
lower_whisker = q1 - 1.5 * iqr
upper_whisker = q3 + 1.5 * iqr

# 调整须线范围到实际数据的最小值和最大值
lower_whisker = max(lower_whisker, data_column.min())
upper_whisker = min(upper_whisker, data_column.max())

# 添加文本标签
# 计算数据范围，用于确定合适的偏移量
data_range = data_column.max() - data_column.min()
offset = data_range * 0.05  # 使用数据范围的5%作为偏移量

# 调整x位置，让标签分布在不同的水平位置
plt.text(0.8, upper_whisker + offset * 2, f'Upper: {upper_whisker:.2f}', fontsize=10, ha='center', va='bottom',
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
plt.text(0.8, q3 + offset, f'Q3: {q3:.2f}', fontsize=10, ha='center', va='bottom',
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
plt.text(0.8, median, f'Median: {median:.2f}', fontsize=10, ha='center', va='bottom',
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
plt.text(0.8, q1 - offset, f'Q1: {q1:.2f}', fontsize=10, ha='center', va='bottom', 
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
plt.text(0.8, lower_whisker - offset * 2, f'Lower: {lower_whisker:.2f}', fontsize=10, ha='center', va='top',
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))

plt.title('S1B Previous Version STD Lag Distribution', fontsize=14, fontweight='bold')
plt.ylabel('STD Lag (ms)', fontsize=12)
plt.xlabel('')
plt.grid(linestyle='--', alpha=0.3)
plt.tight_layout()
plt.savefig('s1b_previous_version_std_lag_boxplot.png', dpi=300, bbox_inches='tight')
plt.show()

# 计算统计信息
median = data_column.median()
q25 = data_column.quantile(0.25)
q75 = data_column.quantile(0.75)
q90 = data_column.quantile(0.90)
q95 = data_column.quantile(0.95)
q99 = data_column.quantile(0.99)
max_val = data_column.max()
min_val = data_column.min()
avg = data_column.mean()
std = data_column.std()

# 打印统计结果
print("=== S1B Previous Version STD Lag 统计信息 ===")
print(f"最小值: {min_val:.2f}")
print(f"25% 分位数: {q25:.2f}")
print(f"50% 分位数 (中位数): {median:.2f}")
print(f"75% 分位数: {q75:.2f}")
print(f"90% 分位数: {q90:.2f}")
print(f"95% 分位数: {q95:.2f}")
print(f"99% 分位数: {q99:.2f}")
print(f"最大值: {max_val:.2f}")
print(f"平均值: {avg:.2f}")
print(f"标准差: {std:.2f}")
print(f"数据点数量: {len(data_column.dropna())}")