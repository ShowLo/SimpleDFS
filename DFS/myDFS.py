# -*- coding: UTF-8 -*-

import os
import sys
import ConfigParser
import subprocess
import json

#保存到分布式系统中去
def saveToDFS(fileName, master):
    subprocess.call(['scp', fileName, master + ':' + fileName])

#从分布式系统中下载文件
def loadFromDFS(fileName, destFileName, master):
    subprocess.call(['ssh', master, 'python', 'master.py', 'load', fileName, destFileName])

#主函数
def main(args):
    if len(args) < 2:
        print 'Error : Too less parameters!'
        sys.exit()
    saveOrLoad = args[0]
    fileName = args[1]

    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')
    master = cf.get('master', 'address')

    #将master的程序及配置文件发送到对应机器上去
    subprocess.call(['scp', 'master.py', master + ':master.py'])
    subprocess.call(['scp', 'slaves.py', master + ':slaves.py'])
    subprocess.call(['scp', 'config.conf', master + ':config.conf'])

    if saveOrLoad == 'save':
        #保存到分布式系统中去
        saveToDFS(fileName, master)
        subprocess.call(['ssh', master, 'python', 'master.py', 'save', fileName])
    elif saveOrLoad == 'load':
        if len(args) < 3:
            print 'Not enough parameters for load function!'
            sys.exit()
        destFileName = args[2]
        #从分布式系统下载文件到客户端
        loadFromDFS(fileName, destFileName, master)

if __name__ == '__main__':
    main(sys.argv[1:])