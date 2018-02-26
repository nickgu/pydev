#! /bin/env python
# encoding=utf-8
# author: nickgu 
# 

import os
import logging
import traceback
import sys
import pydev

def beta_range(disp, click, prob=0.95):
    from scipy.stats import beta
    return beta_bound(disp, click, (1-prob)*0.5), beta_bound(disp, click, prob+(1-prob)*0.5)

def beta_bound(disp, click, prob=0.05):
    from scipy.stats import beta
    return beta.ppf( prob, click, disp-click-1)

class AverageValue:
    def __init__(self):
        self.value = 0
        self.count = 0

    def add(self, value):
        self.count += 1
        self.value += value

    def average(self):
        if self.count == 0 :
            return 0
        return self.value *1.0 / self.count

    def __str__(self):
        return '%.3f (%d totals)' % (self.average(), self.count)

class RocRecorder:
    def __init__(self):
        self.data = []

    def add(self, x, y):
        self.data.append( (x, y) )

    def plot(self, bucket_num=20, descending=True):
        total_target = sum(map(lambda x:x[1], self.data))

        print >> sys.stderr, 'Begin sorting.. (num=%d, total_target=%.2f)' % (len(self.data), total_target)
        if descending:
            data = sorted(self.data, key=lambda x:-x[0])
        else:
            data = sorted(self.data, key=lambda x:x[0])
        print >> sys.stderr, 'Sort over.'

        bucket = 0
        acc_target = 0
        print 'bucket\tper_x\tx_value\troc_y'
        for idx, (x, y) in enumerate(data):
            per = (idx + 1) * 1.0 / len(data)
            acc_target += y
            if per >= (bucket+1.0) / bucket_num:
                bucket += 1
                print '%d\t%.3f\t%s\t%.3f' % (bucket, per, x, acc_target / total_target)

class DimInfo:
    def __init__(self, name=None):
        self.name = name
        self.distribution = {}
   
    def set(self, typename, ratio, score):
        self.distribution[typename] = [ratio, score]

    def uniform_ratio(self):
        sum = 0
        for key, (ratio, score) in self.distribution.iteritems():
            sum += ratio
        if sum>0:
            for key in self.distribution:
                self.distribution[key][0] = self.distribution[key][0] * 1.0 / sum

    def write(self, stream):
        print >> stream, '%s\t%s\n' % (json.dumps(self.name), json.dumps(self.distribution))

    def read(self, stream):
        line = stream.readline()
        key, value = line.split('\t')
        self.name = json.loads(key)
        self.distribution = json.loads(value)

    def score(self):
        self.uniform_ratio()
        ret = 0
        for (ratio, score) in self.distribution.values():
            ret += ratio * score
        return ret

    def compare(self, A):
        ''' analysis what makes diff from A to B. 
        '''
        final_score_A = A.score()
        final_score_B = self.score()
        print >> sys.stderr, 'score of A: %8.3f' % (final_score_A)
        print >> sys.stderr, 'score of B: %8.3f' % (final_score_B)
        print >> sys.stderr, '      diff: %8.3f' % (final_score_B - final_score_A)
        print >> sys.stderr, '-------------------------------------------'

        # analysis distribution diff.
        # assume the distribution is not change from A to B.
        #   then the delta = score_B - score_disA (score is same.)
        distribution_score = 0
        score_score = 0
        top_diff = []
        for key, (ratio_B, score_B) in self.distribution.iteritems():
            ratio_A, score_A = A.distribution.get(key, (0, 0))
            distribution_score += ratio_A * score_B
            score_score += ratio_B * score_A 
            diff_score = score_B * ratio_B  - score_A * ratio_A
            top_diff.append( (key, diff_score, 'B:%.1f%% x %.2f => A:%.1f%% x %.2f' % 
                (ratio_B*100., score_B, ratio_A*100., score_A  )) )

        for key, (ratio_A, score_A) in A.distribution.iteritems():
            if key in self.distribution:
                continue
            top_diff.append( (key, -score_A*ratio_A, 'B:%.1f%% x %.2f => A:%.1f%% x %.2f' % 
                (0, 0, ratio_A*100., score_A  )) )

        delta_distribution = final_score_B - distribution_score
        delta_score = final_score_B - score_score
        print >> sys.stderr, 'Diff by distribution : %8.3f (%.3f->%.3f)' % (
                delta_distribution, final_score_B, distribution_score)
        print >> sys.stderr, 'Diff by score        : %8.3f (%.3f->%.3f)' % (
                delta_score, final_score_B, score_score)

        print >> sys.stderr, '-------------------------------------------'
        for key, diff, info in sorted(top_diff, key=lambda x:-abs(x[1]))[:5]:
            print >> sys.stderr, '%30s\t%8.3f' % (key, diff)
            print >> sys.stderr, '%30s\t  : %s' % ('', info)

    def debug(self, stream):
        print >> stream, '----------------[[ %s ]]----------------' % self.name
        for key, (ratio, score) in sorted(self.distribution.iteritems(), key=lambda x:-x[1][0]):
            print >> stream, '%30s\t%8.3f\t%5.1f%%' % (key, score, ratio*100.)


def CMD_roc(argv):
    '''
        draw roc curve data.
            roc [--file -f] [-b:<bucket_num>] [-a]
            --file, -f      : input file.
            --bucket -b     : bucket num
            --ascending -a  : ascending order
    '''

    arg = pydev.Arg()
    arg.str_opt('file', 'f', 'input file')
    arg.str_opt('bucket', 'b', 'bucket num', default='20')
    arg.bool_opt('ascending', 'a', 'ascending order')
    opt = arg.init_arg(argv)

    bucket_num = int(opt.bucket)
    in_file = sys.stdin
    if opt.file is not None:
        in_file = file(opt.file)
    descending = True
    if opt.ascending:
        descending = False

    roc = RocRecorder()
    for line in in_file.readlines():
        arr = line.strip().split('\t')
        x, y = float(arr[0]), float(arr[1])
        roc.add(x, y)
    roc.plot(bucket_num, descending)


def CMD_dimdiff(argv):
    '''
    dimdiff: compare the diff between two DimInfo file.
        dimdiff <filename1> <filename2>
    '''
    a = DimInfo()
    a.read(file(argv[0]))
    b = DimInfo()
    b.read(file(argv[1]))

    b.compare(a)

def CMD_dimshow(argv):
    '''
    dimshow: show dim info of file.
        dimshow <filename>
    '''
    a = DimInfo()
    a.read(file(argv[0]))
    a.debug(sys.stderr)

def CMD_show(argv):
    '''Show all the commands.
    '''
    l = sys.modules['__main__'].__dict__.keys()
    for key in l:
        if key.find('CMD_') == 0:
            print ' %s: ' % key.replace('CMD_', '')
            f = eval(key)
            if f.__doc__ is None:
                print '    [NO_DOC]'
                print
            else:
                print '    %s' % (f.__doc__.replace('\n', '\n    '))


if __name__=='__main__':
    logging.basicConfig(level=logging.INFO)
    '''
    data_analysis.py <command>
        command-list:
            list:
                list all the availble command.
    '''
    if len(sys.argv)<=1:
        print (
'''Usage:
    data_analysis.py <command>
    you can use 'data_analysis.py show' to get all available command.
''')
        sys.exit(-1)

    com = eval('CMD_' + sys.argv[1])
    ret = com(sys.argv[2:])
    sys.exit(ret)

