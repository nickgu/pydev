#! /bin/env python
# encoding=utf-8
# gusimiu@baidu.com
# 

#! /bin/env python
# encoding=utf-8
# gusimiu@baidu.com
#   
#   V1.0.2 change::
#       add Mapper mode. (--mapper)
#
#   V1.0.1 change:: 
#       dump(self, stream, sort)
#
#   V1.0
# 


import ConfigParser
import os
import re
import logging
import traceback
import socket
import sys
import time
from multiprocessing import *
#import threading


HEADER_LENGTH = 8
DETECTIVE_MSG = 'Are_you_alive?'

def foreach_line(fd, percentage=False):
    if percentage:
        cur_pos = fd.tell()
        fd.seek(0, 2)
        file_size = fd.tell()
        fd.seek(cur_pos)
        old_perc = 0
    while 1:
        line = fd.readline()
        if line == '':
            break
        if percentage:
            cur_pos = fd.tell()
            perc = int(100.0 * cur_pos / file_size)
            if perc>old_perc:
                old_perc = perc
                print >> sys.stderr, '%c[foreach_line] process %d%% (%d/%d)' % (
                        13, perc, cur_pos, file_size)
        yield line.strip('\n')


def foreach_row(fd, min_fields_num=-1, seperator='\t', percentage=False):
    if percentage:
        cur_pos = fd.tell()
        fd.seek(0, 2)
        file_size = fd.tell()
        fd.seek(cur_pos)
        old_perc = 0
    while 1:
        line = fd.readline()
        if line == '':
            break
        if percentage:
            cur_pos = fd.tell()
            perc = int(100.0 * cur_pos / file_size)
            if perc>old_perc:
                old_perc = perc
                print >> sys.stderr, '%c[foreach_line] process %d%% (%d/%d)' % (
                        13, perc, cur_pos, file_size)
        arr = line.strip('\n').split(seperator)
        if min_fields_num>0 and len(arr)<min_fields_num:
            continue
        yield arr

def echo(input_text):
    return ('ACK: ' + input_text)

def sock_recv(sock):
    d = sock.recv(HEADER_LENGTH)
    if len(d)==0:
        return None
    data_len = int(d)
    #print data_len
    data = ''
    while 1:
        n = min(4096, data_len)
        d = sock.recv(n)
        if not d:
            break

        data_len -= len(d)
        data += d

        #print 'left=%d cur=%d' % (data_len, len(data))
        if data_len<=0:
            break
    return data

def sock_send(sock, data):
    data_len = '%8d' % len(data)
    sock.sendall(data_len)
    sock.sendall(data)

def simple_query(query, ip='127.0.0.1', port=12345):
    sys.stderr.write('SEEK_TO: %s:%s\n' % (ip, port))
    clisock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clisock.connect((ip, port))

    sock_send(clisock, query)
    ret = sock_recv(clisock)
    clisock.close()
    return ret

def detect(ip='127.0.0.1', port=12345):
    try:
        ret = simple_query(DETECTIVE_MSG, ip, port)
        if ret != 'YES':
            return False
    except Exception, msg:
        sys.stderr.write('detect err: %s\n' % msg)
        return False
    return True

def simple_query_by_name(query, name, ip='127.0.0.1'):
    cmd = 'SEEK\t%s' % name
    ret = simple_query(cmd, ip, port=8769)
    arr = ret.split('\t')
    if arr[0] != 'OK':
        sys.stderr.write('seek name failed! [%s]' % ret)
        return None
    port = int(arr[1])
    return simple_query(query, ip, port)

class BasicService:
    def __init__(self):
        self.__handler_init = None
        self.__handler_process = None
        self.__handler_timer_process = None
        self.__timer = 0.0

    def set_init(self, h_init):
        self.__handler_init = h_init

    def set_process(self, h_process):
        self.__handler_process = h_process

    def set_timer_deamon(self, h_process, seconds=60.0):
        '''
            set a process which will be called each time interval.
        '''
        self.__handler_timer_process = h_process
        self.__timer = seconds

    def run_with_name(self, name, desc='No description.', ip='127.0.0.1', port=12345):
        '''
            尝试和本机服务管理器建立映射关系
        '''
        cmd = 'REGEDIT\t%s\t%d\t%s' % (name, port, desc)
        ret = simple_query(cmd, ip, port=8769)
        arr = ret.split('\t')
        if arr[0] != 'OK': 
            sys.stderr.write('SET NAME FAILED! [%s]' % ret)
            return
        self.run(ip, port)
        
    def run(self, ip='127.0.0.1', port=12345):
        if self.__handler_init:
            sys.stderr.write('init..\n')
            self.__handler_init()

        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1) 
        self.__sock.bind( (ip, port) )
        self.__sock.listen(32)
        sys.stderr.write('listen : %s:%d\n' % (ip, port))

        last_time = time.time()
        
        try:
            while 1:
                # check time at first.
                if self.__handler_timer_process:
                    dt = time.time() - last_time
                    if dt >= self.__timer:
                        try:
                            self.__handler_timer_process()
                        except Exception, msg:
                            sys.stderr.write('error in time_handler: %s\n' % msg)
                        last_time = time.time()

                # set a timer for accept:
                #   because i need to run a timer process.
                self.__sock.settimeout(1);
                try:
                    clisock, (remote_host, remote_port) = self.__sock.accept()
                except socket.timeout, msg:
                    continue

                try:
                    data = sock_recv(clisock)
                    if data == DETECTIVE_MSG:
                        sock_send(clisock, 'YES')
                    else:
                        sys.stderr.write('[%s:%s] connected dl=%d\n' % (remote_host, remote_port, len(data)))
                        if self.__handler_process:
                            response = self.__handler_process(data)
                            if response:
                                sock_send(clisock, response)

                except Exception, msg:
                    sys.stderr.write('err [%s]!\n' % msg)
                    traceback.print_stack()
                    traceback.print_exc()
                    continue
                finally:
                    clisock.close()
        finally:
            sys.stderr.write('byebye.\n')
            self.__sock.close()

class ManagerService:
    def __init__(self):
        self.__name_dct = {}
        self.__desc_dct = {}
        self.__recover()
        self.__svr = BasicService()
        self.__svr.set_process(self.process)
        self.__svr.set_timer_deamon(self.deamon_process, 5)

    def run(self):
        self.__svr.run(port=8769)

    def deamon_process(self):
        '''
            check whether each service is alive.
        '''
        sys.stderr.write('detect: %s\n' % time.asctime()) 
        del_names = []
        for name, port in self.__name_dct.iteritems():
            alive = detect(port=port)
            if not alive:
                sys.stderr.write('%s : %s[%d] is dead.\n' % (time.asctime(), name, port))
                del_names.append(name)
        for name in del_names:
            del self.__name_dct[name]
            del self.__desc_dct[name]
        self.__backup()

    def process(self, cmd):
        '''
            3 type(s) of cmd:
                'SEEK[\t][name]' => 'OK\tPORT' or 'ERR\tNOT_FOUND'
                'REGEDIT[\t][name][\t][port][\t][desc]' => 'OK' or 'ERR\tmsg'
                'INFO' => 'OK\tname info.'
        '''
        cmd = cmd.replace('\n', '')
        cmd = cmd.replace('###', '')
        cmd = cmd.replace('||', '')
        arr = cmd.split('\t')
        if arr[0] == 'SEEK':
            if len(arr)!=2:
                return 'ERR\tpara_num=%d' % len(arr)
            name = arr[1]
            if name not in self.__name_dct:
                return 'ERR\tNOT_FOUND'
            return 'OK\t%d' % self.__name_dct[name] 
        elif arr[0] == 'REGEDIT':
            if len(arr)!=4:
                return 'ERR\tpara_num=%d' % len(arr)
            name, port, desc = arr[1:4]
            if ':' in name:
                return 'ERR\tINVALID_NAME_NO_:_'
            port = int(port)
            self.__name_dct[name] = port
            self.__desc_dct[name] = desc
            return 'OK'
        elif arr[0] == 'INFO':
            info = ''
            for name, port in self.__name_dct.iteritems():
                desc = self.__desc_dct.get(name, '')
                info += '%s||%s||%s###' % (name, port, desc)
            return 'OK\t%s' % info
    
    def __recover(self):
        try:
            f = file('service_info.txt')
        except:
            sys.stderr.write('no backup info.\n')
            return
        for l in f.readlines():
            arr = l.strip('\n').split('\t')
            if len(arr)!=3: 
                continue
            name, port, desc = arr
            port = int(port)
            if name not in self.__name_dct:
                self.__name_dct[name] = port
                self.__desc_dct[name] = desc

    def __backup(self):
        f = file('service_info.txt', 'w')
        for name, port in self.__name_dct.iteritems():
            desc = ''
            if name in self.__desc_dct:
                desc = self.__desc_dct[name]
            f.write('%s\t%d\t%s\n' % (name, port, name))
        f.close()

class MapperCounter:
    def __init__(self):
        self.__dct = {}

    def inc(self, key, inc=1):
        if key not in self.__dct:
            self.__dct[key] = 0
        self.__dct[key] += inc

    def dump(self, stream, sort=False):
        if sort:
            for key, value in sorted(self.__dct.iteritems(), key=lambda x:-x[1]):
                print '%s\t%s' % (key, value)
        else:
            for key, value in self.__dct.iteritems():
                print '%s\t%s' % (key, value)

def __test_basic_service():
    # test a svr.
    svr = BasicService()
    svr.set_process(echo)
    svr.run_with_name('ECHO', desc='This is a echo service.')

class MPProcessor:
    '''多进程处理器
    给定进程数、处理函数和并发度，自动调度
    '''
    def __init__(self, functor, proc_num, stdout_dir='mp_out'):
        '''设定进程数和并发度
        '''
        self.functor = functor
        self.proc_num = proc_num
        self.processes = [];
        self.stdout_dir=stdout_dir;
        self.stdout_fn = [];
        for i in range(proc_num-1):
            self.processes.append(Process(target=self._inner_func, args=(i, )));
            out_fn = './%s/part-%05d'%(self.stdout_dir, i)
            self.stdout_fn.append(out_fn);
        # 补充一个thread num的FN
        out_fn = './%s/part-%05d'%(self.stdout_dir, self.proc_num)
        self.stdout_fn.append(out_fn);
        return

    def _inner_func(self, cur_i):
        '''
        多进程壳函数，调用真正的函数。同时做一些基本处理 
        '''
        # 先进行重定向
        old_stdout = sys.stdout;
        out_fn = self.stdout_fn[cur_i];
        logging.info('Process[%d] reset stdout to %s'%(cur_i, out_fn));
        sys.stdout = open( out_fn, 'w' )

        # 开始正式执行程序
        logging.info('Process[%d] begin to process.'%cur_i);
        # 执行进程函数、给定i和文件
        self.functor(cur_i, self.proc_num);

        sys.stdout = old_stdout; # 恢复bak stdout.
        logging.info('Process[%d] processes over.'%cur_i);

    def process_all(self):
        ''' START => JOIN.
        '''
        for process in self.processes:
            process.start();
    
        # 自己也跑一个。
        self._inner_func(self.proc_num-1);

        for process in self.processes:
            process.join();


class MTItemProcessor(MPProcessor):
    '''对一个item集合的多线程处理方式
    给定set/list/dict等，和一个functor，即可分发到不同线程中调用
    TODO: 待测试
    '''
    def __init__(self, 
            proc_set, functor, proc_num, stdout_dir):
        MPProcessor.__init__(functor, proc_num, stdout_dir);
        self.proc_set = proc_set
        self.inner_func = functor
        self.functor = self._shell_functor
        return ;
    
    def _shell_functor(self, cur_i):
        '''真实的内嵌函数，进行集合遍历并输出
        '''
        for it in self.proc_set:
            if (id/7) % (self.proc_num+1) == cur_i:
                # hit this processor.
                self.inner_func(it);

    def merge_stdout(self):
        '''把所有文件统一输出
        '''
        logging.info('MTP: merge stdout');
        line_cnt = 0;
        for fn in self.stdout_fn:
            fl = open(fn, 'r');
            line = fl.readline();
            while line:
                line=line.rstrip('\n');
                print line;
                line_cnt += 1;
                line = fl.readline()
            fl.close();
        logging.info('MTP: merge over! %d lines written.'%line_cnt);

def CMD_mgrservice(argv):
    '''Run the basic_service manager.
    '''
    s = ManagerService()
    s.run()

def CMD_show(argv):
    '''Show all the commands.
    '''
    l = sys.modules['__main__'].__dict__.keys()
    for key in l:
        if key.find('CMD_') == 0:
            print ' %s: ' % key.replace('CMD_', '')
            f = eval(key)
            print '    %s' % (f.__doc__.replace('\n', '\n    '))

def CMD_counter(argv):
    '''Run counter job.
        -i          : output int.
        --mapper    : run as mapper mode.
        -c [int]    : cut threshold.
    '''
    output_int = False
    arg_set = set(argv)
    cut_num = 0
    mapper_mode = False
    if '-i' in arg_set:
        # output as integer.
        output_int = True
    if '--mapper' in arg_set:
        mapper_mode = True
    for arg in arg_set:
        if arg.find('-c') == 0:
            cut_num = int(arg[2:])

    if mapper_mode:
        ct = MapperCounter()
        while 1:
            line = sys.stdin.readline()
            if line == '':
                break
            ct.inc(line.strip('\n'))
        ct.dump(sys.stdout)

    else:
        # reducer.
        last_key = None
        acc_value = 0
        while 1:
            line = sys.stdin.readline()
            if line == '':
                break
            arr = line.strip('\n').split('\t')
            if len(arr)!=2:
                continue
            key, value = arr
            if output_int:
                value = int(value)
            else:
                value = float(value)
            if key != last_key:
                if last_key:
                    if acc_value >= cut_num:
                        print '%s\t%s' % (last_key, acc_value)
                last_key = key
                acc_value = 0
            acc_value += value
        if last_key:
            if acc_value >= cut_num:
                print '%s\t%s' % (last_key, acc_value)


if __name__=='__main__':
    '''
    pygu.py <command>
        command-list:
            list:
                list all the availble command.
    '''
    if len(sys.argv)<=1:
        print (
'''Usage:
    pygu.py <command>
    you can use 'pygu.py show' to get all available command.
''')
        sys.exit(-1)

    com = eval('CMD_' + sys.argv[1])
    ret = com(sys.argv[2:])
    sys.exit(ret)




