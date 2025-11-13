import matplotlib.pyplot as plt
import duckdb

conn = duckdb.connect(database="database/verihubs.duckdb", read_only=True)
df = conn.execute("SELECT * FROM daily_order_status").df()

df_pivoted = df.pivot_table(index='day', columns='Status', values='order_count')

fig, ax = plt.subplots(figsize=(13, 6))
df_pivoted.plot(kind='bar', stacked=True, rot=90, ax=ax)
ax.xaxis.set_major_formatter(plt.FixedFormatter(df_pivoted.index.strftime('%Y-%m-%d')))
ax.set_xticks(range(0, len(df_pivoted.index), 7))
ax.set_xticklabels(df_pivoted.index.strftime('%Y-%m-%d')[::7], rotation=45, ha='right')
plt.legend(bbox_to_anchor=(1.27, 1.015), loc='upper right', title='Order Status')
plt.title('Daily Order Status Over Time')
plt.ylabel("Number of Orders")
plt.tight_layout()
plt.show()