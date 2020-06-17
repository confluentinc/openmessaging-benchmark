#!/usr/bin/env python3

import json
import sys
import statistics
import numpy as np
import argparse
import re
import pygal
from itertools import chain
from os import walk
from os import path


def create_quantile_chart(workload, title, y_label, time_series):
    import math
    chart = pygal.XY(  # style=pygal.style.LightColorizedStyle,
        # fill=True,
        legend_at_bottom=True,
        x_value_formatter=lambda x: '{} %'.format(100.0 - (100.0 / (10**x))),
        show_dots=True,
        dots_size=.3,
        show_x_guides=True)
    chart.title = title
    # chart.stroke = False

    chart.human_readable = True
    chart.y_title = y_label
    chart.x_title = 'Percentile'
    chart.x_labels = [1, 2, 3, 4, 5]

    for label, values in time_series:
        values = sorted((float(x), y) for x, y in values.items())
        xy_values = [(math.log10(100 / (100 - x)), y)
                     for x, y in values if x <= 99.999]
        chart.add(label, xy_values)

    chart.render_to_file('%s.svg' % workload)


def create_chart(workload, title, y_label, time_series):
    chart = pygal.XY(dots_size=.3,
                     legend_at_bottom=True,)
    chart.title = title

    chart.human_readable = True
    chart.y_title = y_label
    chart.x_title = 'Time (seconds)'
    # line_chart.x_labels = [str(10 * x) for x in range(len(time_series[0][1]))]

    ys = []
    for label, values in time_series:
        ys.append(values)
        chart.add(label, [(10*x, y) for x, y in enumerate(values)])
    ys = chain.from_iterable(ys)
    max_y = float('-inf')  # Hack for starting w/ INT64_MIN
    for y in ys:
        if max_y < y:
            max_y = y
    chart.range = (0, max_y * 1.20)
    chart.render_to_file('%s.svg' % workload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Plot Kafka OpenMessaging Benchmark results')

    parser.add_argument('--acks', dest='ack', help='ACK level')
    parser.add_argument('--message-size', dest='msg_size',
                        help='Message size (100b or 1kb)')
    parser.add_argument('--results', dest='results_dir',
                        help='Directory containing the results')
    parser.add_argument('--durability', dest='durability',
                        help='Either \'fsync\' or \'nofsync\'')
    args = parser.parse_args()

    file_name_template = f'{args.msg_size}-run-[0123456789]+-{args.durability}-{args.ack}-acks.json'

    aggregate = []

    # Get list of directories
    for (dirpath, dirnames, filenames) in walk(args.results_dir):
        for file in filenames:
            if re.match(file_name_template, file):
                file_path = path.join(dirpath, file)
                data = json.load(open(file_path))
                data['file'] = file
                aggregate.append(data)

    stats_pub_rate = []
    stats_con_rate = []
    stats_lat_p99 = []
    stats_lat_p999 = []
    stats_lat_p9999 = []
    stat_lat_avg = []
    stat_lat_max = []
    stat_lat_quantile = []
    drivers = []

    # Aggregate across all runs
    for data in aggregate:
        stats_pub_rate.append(data['publishRate'])
        stats_con_rate.append(data['consumeRate'])
        stats_lat_p99.append(data['publishLatency99pct'])
        stats_lat_p999.append(data['publishLatency999pct'])
        stats_lat_p9999.append(data['publishLatency9999pct'])
        stat_lat_avg.append(data['publishLatencyAvg'])
        stat_lat_max.append(data['publishLatencyMax'])

        stat_lat_quantile.append(data['aggregatedPublishLatencyQuantiles'])
        drivers.append(data['file'])

    # Generate latency quantiles
    time_series = zip(drivers, stat_lat_quantile)
    svg = f'{args.msg_size}-{args.durability}-{args.ack}-acks-latency-quantile'
    create_quantile_chart(svg, 'Publish latency quantiles',
                          y_label='Latency (ms)',
                          time_series=time_series)

    # Genrate publish rate
    svg = f'{args.msg_size}-{args.durability}-{args.ack}-acks-publish-rate'
    time_series = zip(drivers, stats_pub_rate)
    create_chart(svg, 'Publish throughput',
                 y_label='Message/sec', time_series=time_series)
