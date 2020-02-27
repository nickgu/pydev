#! /bin/env python3
# encoding=utf-8
# author: nickgu 
# 
#   Compitible for python3
#

import sys
import argparse


class ColorString:
    TC_NONE         ="\033[m"
    TC_RED          ="\033[0;32;31m"
    TC_LIGHT_RED    ="\033[1;31m"
    TC_GREEN        ="\033[0;32;32m"
    TC_LIGHT_GREEN  ="\033[1;32m"
    TC_BLUE         ="\033[0;32;34m"
    TC_LIGHT_BLUE   ="\033[1;34m"
    TC_DARY_GRAY    ="\033[1;30m"
    TC_CYAN         ="\033[0;36m"
    TC_LIGHT_CYAN   ="\033[1;36m"
    TC_PURPLE       ="\033[0;35m"
    TC_LIGHT_PURPLE ="\033[1;35m"
    TC_BROWN        ="\033[0;33m"
    TC_YELLOW       ="\033[1;33m"
    TC_LIGHT_GRAY   ="\033[0;37m"
    TC_WHITE        ="\033[1;37m"

    def __init__(self):
        pass

    @staticmethod
    def colors(s, color):
       return color + s + ColorString.TC_NONE  

    @staticmethod
    def red(s): return ColorString.colors(s, ColorString.TC_RED)

    @staticmethod
    def yellow(s): return ColorString.colors(s, ColorString.TC_YELLOW)

    @staticmethod
    def green(s): return ColorString.colors(s, ColorString.TC_GREEN)

    @staticmethod
    def blue(s): return ColorString.colors(s, ColorString.TC_BLUE)

    @staticmethod
    def cyan(s): return ColorString.colors(s, ColorString.TC_CYAN)


def error(*args, on_screen=True):
    if on_screen:
        tag = ColorString.red('[ERROR] ')
    else:
        tag = '[ERROR] '
    print(tag, *args, file=sys.stderr)

def info(*args, on_screen=True):
    if on_screen:
        tag = ColorString.yellow('[INFO] ')
    else:
        tag = '[INFO] '
    print(tag, *args, file=sys.stderr)



class Arg(object):
    '''
    Sample code:
        ag=Arg()
        ag.str_opt('f', 'file', 'this arg is for file')
        opt = ag.init_arg()
        # todo with opt, such as opt.file
    '''
    def __init__(self, help='Lazy guy, no help'):
        self.is_parsed = False;
        #help = help.decode('utf-8').encode('gb18030')
        self.__parser = argparse.ArgumentParser(description=help)
        self.__args = None;
        #    -l --log 
        self.str_opt('log', 'l', 'logging level default=[error]', meta='[debug|info|error]');
    def __default_tip(self, default_value=None):
        if default_value==None:
            return ''
        return ' default=[%s]'%default_value

    def bool_opt(self, name, iname, help=''):
        #help = help.decode('utf-8').encode('gb18030')
        self.__parser.add_argument(
                '-'+iname, 
                '--'+name, 
                action='store_const', 
                const=1,
                default=0,
                help=help);
        return

    def str_opt(self, name, iname, help='', default=None, meta=None):
        help = (help + self.__default_tip(default))#.decode('utf-8').encode('gb18030')
        self.__parser.add_argument(
                '-'+iname, 
                '--'+name, 
                metavar=meta,
                help=help,
                default=default);
        pass

    def var_opt(self, name, meta='', help='', default=None):
        help = (help + self.__default_tip(default).decode('utf-8').encode('gb18030'))
        if meta=='':
            meta=name
        self.__parser.add_argument(name,
                metavar=meta,
                help=help,
                default=default) 
        pass

    def init_arg(self, input_args=None):
        if not self.is_parsed:
            if input_args is not None:
                self.__args = self.__parser.parse_args(input_args)
            else:
                self.__args = self.__parser.parse_args()
            self.is_parsed = True;
        if self.__args.log:
            format='%(asctime)s %(levelname)8s [%(filename)18s:%(lineno)04d]: %(message)s'
            if self.__args.log=='debug':
                logging.basicConfig(level=logging.DEBUG, format=format)
                logging.debug('log level set to [%s]'%(self.__args.log));
            elif self.__args.log=='info':
                logging.basicConfig(level=logging.INFO, format=format)
                logging.info('log level set to [%s]'%(self.__args.log));
            elif self.__args.log=='error':
                logging.basicConfig(level=logging.ERROR, format=format)
                logging.info('log level set to [%s]'%(self.__args.log));
            else:
                logging.error('log mode invalid! [%s]'%self.__args.log)
        return self.__args

    @property
    def args(self):
        if not self.is_parsed:
            self.__args = self.__parser.parse_args()
            self.is_parsed = True;
        return self.__args;


def dp_to_generate_answer_range(data):
    ''' 
        data shape: (batch, clen, 2), 
        last dim indicates start/end prob.
    '''
    ans = []
    l = data.shape[1]
    data = data.cpu().numpy()
    dp = [0.] * (l+1)
    dp_sidx = [-1] * (l+1)
    for b in data:
        max_prob = 0
        max_range = (0, 0)
        dp[0] = 0
        dp_sidx[0] = -1
        for idx in range(l):
            sp, ep = b[idx]
            cur_end_prob = dp[idx] * ep
            if cur_end_prob > max_prob:
                max_prob = cur_end_prob
                max_range = (dp_sidx[idx], idx)

            if sp>dp[idx]:
                dp[idx+1] = sp
                dp_sidx[idx+1] = idx
            else:
                dp[idx+1] = dp[idx]
                dp_sidx[idx+1] = dp_sidx[idx]
        ans.append(max_range)
    return ans


if __name__=='__main__':
    pass
    
