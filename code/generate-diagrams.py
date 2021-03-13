import matplotlib.pyplot as plt
import numpy as np
import time

queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
rddApi = [16.368, 1*60+17.305, 1*60+29.439, 13.544, 10*60+20.809]
sparkSQLparquet = [10, 25.888, 30, 40, 50]
sparkSQLcsv = [20, 1*60+14.036, 60, 60, 100]

width = 0.30
x = np.arange(len(queries))

plt.style.use('dark_background')

fig, ax = plt.subplots(figsize=(16,12))
rect1 = ax.bar(x, rddApi, width, label='RDD Api', color='lightgrey')
rect2 = ax.bar(x+width, sparkSQLparquet, width, label='SparkSQL with Parquet input', color = 'lightsteelblue')
rect3 = ax.bar(x-width, sparkSQLcsv, width, label='SparkSQL with csv input', color = 'thistle')

ax.set_xticks(x)
ax.set_yticks(np.arange(0, np.max(rddApi)+5, 60.0))
ax.set_ylabel('Time (s)', fontsize = 16)
ax.set_xticklabels(queries, fontsize = 15)
ax.set_title('Time comparison', color = 'silver', fontsize = 20)
ax.set_xlabel('Queries', fontsize = 16)
ax.xaxis.label.set_color('silver')
ax.tick_params(axis='x', colors='silver')
ax.yaxis.label.set_color('silver')
ax.tick_params(axis='y', colors='silver')


def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(round(height,2)), xy=(rect.get_x()+rect.get_width()/2, height), xytext=(0, 3), 
                    textcoords="offset points", ha='center', va='bottom', fontsize = 16)
        
autolabel(rect1)
autolabel(rect2)
autolabel(rect3)

ax.legend(prop={'size': 16})
fig.tight_layout()

plt.show()


optimization = ['Enabled', 'Disabled']

times  = [6.0804, 11.6014]

width = 0.5
x = np.arange(len(optimization))

plt.style.use('dark_background')

fig, ax = plt.subplots(figsize=(8, 10))
rect1 = ax.bar((x[0]+1, x[1]-1), times, width, color='thistle')

ax.set_xticks(x)
ax.set_yticks(np.arange(0, np.max(times)+5, 1))
ax.set_ylabel('Time (s)', fontsize = 16)
ax.set_xticklabels(optimization, fontsize = 15)
ax.set_title('Time comparison', color = 'silver', fontsize = 20)
ax.set_xlabel('Join Optimization', fontsize = 16)
ax.xaxis.label.set_color('silver')
ax.tick_params(axis='x', colors='silver')
ax.yaxis.label.set_color('silver')
ax.tick_params(axis='y', colors='silver')


def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(round(height,2)), xy=(rect.get_x()+rect.get_width()/2, height), xytext=(0, 3), 
                    textcoords="offset points", ha='center', va='bottom', fontsize = 16)
        
autolabel(rect1)

fig.tight_layout()

plt.show()


join = ['Repartition', 'Broadcast']

times  = [1*60+55.236, 7*60+55.565]

width = 0.5
x = np.arange(len(join))

plt.style.use('dark_background')

fig, ax = plt.subplots(figsize=(8, 10))
rect1 = ax.bar((x[0]+1, x[1]-1), times, width, color='lightsteelblue')

ax.set_xticks(x)
ax.set_yticks(np.arange(0, np.max(times)+5, 60))
ax.set_ylabel('Time (s)', fontsize = 16)
ax.set_xticklabels(join, fontsize = 15)
ax.set_title('Time comparison', color = 'silver', fontsize = 20)
ax.set_xlabel('Join Type', fontsize = 16)
ax.xaxis.label.set_color('silver')
ax.tick_params(axis='x', colors='silver')
ax.yaxis.label.set_color('silver')
ax.tick_params(axis='y', colors='silver')


def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(round(height,2)), xy=(rect.get_x()+rect.get_width()/2, height), xytext=(0, 3), 
                    textcoords="offset points", ha='center', va='bottom', fontsize = 16)
        
autolabel(rect1)

fig.tight_layout()

plt.show()