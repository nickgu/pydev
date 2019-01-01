#! /bin/env python
# encoding=utf-8
# author: nickgu 
# 

def scatter(plt, points, format='b'):
    plt.scatter(map(lambda x:x[0], points), map(lambda x:x[1], points), c=format)

def distribution_bar(plt, data, begin, step, end, x_gap=1, format='b', label='Distribution'):
    import pyda
    
    dist = pyda.bucket_distribution(data, begin, step, end)
    ticks = map(lambda x:x[0], dist)
    ratios = map(lambda x:x[3], dist)

    bar2(plt, ticks, ratios, x_gap=x_gap, format=format, label=label)


def bar(plt, data, x_gap=1, format='b', label='Bar'):
    # input [(x, y), ...]
    axis_x_labels = map(lambda x:x[0], data)
    value = map(lambda x:x[1], data)

    bar2(plt, axis_x_labels, value, x_gap=x_gap, format=format, label=label)

def bar2(plt, axis_x_labels, data, x_gap=1, format='b', label='Bar'):
    # input (x, ..), (y, ..)
    x_num = len(axis_x_labels)
    bar_width = 0.8
    bar_id = 0

    axis_x = [i for i in range(x_num)]
    plt.bar(axis_x, data, fc=format, width=bar_width, label=label)
    plt.legend()
    
    offset = []
    x_labels = []
    for i in range(0, len(axis_x_labels), x_gap):
        offset.append(i)
        x_labels.append(axis_x_labels[i])
    plt.xticks(offset, x_labels)

def bars(plt, axis_x_labels, data_dict, x_gap=1):
    colors = ['b', 'r', 'g', 'y']
    x_num = len(axis_x_labels)
    bar_count = len(data_dict)
    bar_width = 0.8 / bar_count
    bar_id = 0
    for key, values in data_dict.iteritems():
        axis_x = [i+bar_width*bar_id for i in range(x_num)]
        plt.bar(axis_x, values, fc=colors[bar_id % len(colors)], width=bar_width, label=key)
        bar_id += 1
    plt.legend()
    
    offset = []
    x_labels = []
    for i in range(0, len(axis_x_labels), x_gap):
        offset.append(i)
        x_labels.append(axis_x_labels[i])
    plt.xticks(offset, x_labels)

if __name__=='__main__':
    pass
