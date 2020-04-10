# -*- coding: UTF-8 -*-

import os
import random
import ConfigParser
import sys
import json
import subprocess

#将文件发送到master中去
def loadToMaster(block_fileName, block_dir, master):
    subprocess.call(['scp', os.path.join(block_dir, block_fileName), master + ':' + os.path.join(block_dir, block_fileName)])

#保存分块文件时，如果此分块文件夹已存在，删除之前的分块，否则新建分块文件夹
def deleteBlockFilesOrCreateBlock(block_dir):
    #放置文件分块的文件夹是否存在
    if not os.path.exists(block_dir):
        #不存在的话新建文件夹
        os.mkdir(block_dir)
    else:
        #存在的话需要清空文件夹里面的分块
        for f in os.listdir(block_dir):
            os.remove(os.path.join(block_dir, f))

#发送文件分块到其他的slave上去
def saveToOtherSlave(filePath, destFilePath, slave):
    subprocess.call(['scp', filePath, slave + ':' + destFilePath])

#从其他slave下载文件
def loadFromOtherSlave(fileName, block_dir):
    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')
    master = cf.get('master', 'address')
    selfAddr = cf.get('self', 'address')
    #需要从master找到有此文件的slave
    process = subprocess.Popen(['ssh', master, 'python', 'master.py', 'slaveExistFile', fileName], stdout = subprocess.PIPE)
    slave = (process.stdout.read()).strip()
    #然后再直接从此slave中请求需要的文件分块
    subprocess.call(['ssh', slave, 'python', 'slaves.py', 'save', fileName, block_dir, selfAddr])
    #请求完之后将当前slave地址加入到所请求分块的slave信息中去
    subprocess.call(['ssh', master, 'python', 'master.py', 'addSlaveToFile', fileName, selfAddr])

def main(args):
    if len(args) < 2:
        print 'Not enough parameters for slaves.py!'
        sys.exit()
    functionName = args[0]
    if functionName == 'deleteBlockFilesOrCreate':
        deleteBlockFilesOrCreateBlock(args[1])
        return
    
    if len(args) < 3:
        print 'Not enough parameters for using  save() or load() in slaves.py!'
        sys.exit()
    #文件名
    block_fileName = args[1]
    block_dir = args[2]

    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')
    master = cf.get('master', 'address')

    if functionName == 'save':
        #自己的地址
        slave = args[3]
        #将分块发送到其他slave中去
        saveToOtherSlave(os.path.join(block_dir, block_fileName), os.path.join(block_dir, block_fileName), slave)
    elif functionName == 'load':
        #将分块数据发送到master中去
        loadToMaster(block_fileName, block_dir, master)

if __name__ == '__main__':
    main(sys.argv[1:])