# -*- coding: UTF-8 -*-

import os
import random
import ConfigParser
import sys
import json
import subprocess

#对文件做分割操作
def split(fileName, block_dir, block_size):
    sizeCount = 0
    blockCount = 1
    #块大小先从M转为字节表示
    block_size = block_size * 1024**2
    #放置文件分块的文件夹是否存在
    if not os.path.exists(block_dir):
        #不存在的话新建文件夹
        os.mkdir(block_dir)
    else:
        #存在的话需要清空文件夹里面的分块
        for f in os.listdir(block_dir):
            os.remove(os.path.join(block_dir, f))
    (realFileName, extension) = os.path.splitext(fileName)
    fWrite = open(os.path.join(block_dir, realFileName + '-%04d'%blockCount + extension), 'w')
    #按照给定分块大小，将大文件切割成多个分块
    if os.path.exists(fileName):
        with open(fileName, 'r') as f:
            for line in f:
                #统计读入文件大小
                sizeCount += len(line)
                if sizeCount > block_size:
                    #当前块大小马上要超过限制，停止下来
                    fWrite.close()
                    #开始写入新的块
                    blockCount += 1
                    #重新统计
                    sizeCount = len(line)
                    fWrite = open(os.path.join(block_dir, realFileName + '-%04d'%blockCount + extension), 'w')
                    fWrite.write(line)
                else:
                    fWrite.write(line)
            #读完整个文件，分块完成
            fWrite.close()
        #删除大文件
        os.remove(fileName)
    else:
        print 'Some errors happened, maybe the file ' + fileName + ' is not sent to the master successfully'
        sys.exit()

#保存到各slave中去
def saveToSlaves(fileName, block_dir, slavesList, replication_factor):
    #分块文件被分布式存放的信息
    fileDSInfo = {}
    slaveNum = len(slavesList)
    files = os.listdir(block_dir)
    deleteProcesses = []
    #先看各slaves上是否之前存放过此文件的分块，是的话删除分块
    for slave in slavesList:
        deleteProcesses.append(subprocess.Popen(['ssh', slave, 'python', 'slaves.py', 'deleteBlockFilesOrCreate', block_dir]))
    for dp in deleteProcesses:
        dp.wait()
    processes = []
    for fn in files:
        #随机选择replication_factor台slaves为当前分块存放的位置
        randomAddress = random.sample(range(slaveNum), replication_factor)
        slavesName = []
        for addr in randomAddress:
            slavesName.append(slavesList[addr])
            #保存文件到各slaves上
            processes.append(subprocess.Popen(['scp', os.path.join(block_dir, fn), slavesList[addr] + ':' + os.path.join(block_dir, fn)]))
        #存放各分块的位置信息
        fileDSInfo[fn] = slavesName
    #将分块的存放信息保存到文件
    with open(fileName + ".json", 'w') as f:
        json.dump(fileDSInfo, f)
    #等待所有将分块有写入slave的操作完成
    for p in processes:
        p.wait()
    #完成之后删除master上保存的分块
    for fn in os.listdir(block_dir):
        os.remove(os.path.join(block_dir, fn))
    os.removedirs(block_dir)

#将多个分块整个成一个文件
def join(fileName, block_dir):
    #对文件名排序以正确恢复
    files = os.listdir(block_dir)
    files.sort()
    with open(fileName, 'w') as fileWrite:
        for fl in files:
            with open(os.path.join(block_dir, fl), 'r') as f:
                for line in f:
                    fileWrite.write(line)

#从各个slave将分块传到master，由master整合各分块最终传送给客户端
def loadFromSlaves(fileName, block_dir, destFileName, client):
    fileDSInfoPath = fileName + '.json'
    if not os.path.exists(fileDSInfoPath):
        print 'The file ' + fileName + ' not exists!'
        sys.exit()
    #放置文件分块的文件夹是否存在
    if not os.path.exists(block_dir):
        #不存在的话新建文件夹
        os.mkdir(block_dir)
    else:
        #存在的话清空文件夹里面的分块
        for f in os.listdir(block_dir):
            os.remove(os.path.join(block_dir, f))
    processes = []
    with open(fileDSInfoPath, 'r') as f:
        fileDSInfo = json.load(f)
        for fn in fileDSInfo.keys():
            slaves = fileDSInfo.get(blockFileName)
            #随机获得一个存有此文件的slave
            slaveToFetch = slaves[random.randint(0, len(slaves) - 1)]
            processes.append(subprocess.Popen(['ssh', slaveToFetch, 'python', 'slaves.py', 'load', fn, block_dir]))
    for p in processes:
        p.wait()
    #整合为一个大文件
    join(fileName, block_dir)
    #最后发送回客户端
    subprocess.call(['scp', fileName, client + ':' + destFileName])
    #删除暂留在master中的文件
    for f in os.listdir(block_dir):
        os.remove(os.path.join(block_dir, f))
    os.removedirs(block_dir)
    os.remove(fileName)

#给出分块文件所在slave之一
def slavesExistFile(blockFileName):
    #恢复未分块前文件名
    fileName = '-'.join(blockFileName.split('-')[:-1]) + os.path.splitext(blockFileName)[1]
    fileDSInfoPath = fileName + '.json'
    if not os.path.exists(fileDSInfoPath):
        print 'The file ' + fileName + ' not exists!'
        sys.exit()
    #打开存放分块文件信息的json文件
    with open(fileDSInfoPath, 'r') as f:
        ffileDSInfo = json.load(f)
        slaves = fileDSInfo[blockFileName]
        #返回一个存有此文件的slave
        return slaves[random.randint(0, len(slaves) - 1)]

def addSlaveToFile(blockFileName, slave):
    #恢复未分块前文件名
    fileName = '-'.join(blockFileName.split('-')[:-1]) + os.path.splitext(blockFileName)[1]
    fileDSInfoPath = fileName + '.json'
    if not os.path.exists(fileDSInfoPath):
        print 'The file ' + fileName + ' not exists!'
        sys.exit()
    #将slave加到分块的slave信息中去
    with open(fileDSInfoPath, 'r') as f:
        fileDSInfo = json.load(f)
        fileDSInfo[blockFileName].append(slave)
        with open(fileDSInfoPath, 'w') as f:
            json.dump(fileDSInfo, f)

#将一个slave添加到分块文件的slave信息中去
def main(args):
    if len(args) < 2:
        print 'Not enough parameters for master.py!'
        sys.exit()

    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')

    commond = args[0]
    #文件名
    fileName = args[1]
    #存放分块的文件夹
    block_dir = os.path.splitext(fileName)[0] + '-block'

    if commond == 'save':        
        #各个slaves的地址
        slavesList = [item.strip() for item in cf.get('slave', 'address').split(',')]
        process = []
        #将slaves的程序及配置文件发送到各slaves机器上去
        for slave in slavesList:
            process.append(subprocess.Popen(['scp', 'slaves.py', slave + ':slaves.py']))
        for p in process:
            p.wait()
        for slave in slavesList:
            #发往各slaves的时候在配置文件中加上各个slave的地址
            tmp = ConfigParser.ConfigParser()
            tmp.read('config.conf')
            tmp.add_section('self')
            tmp.set('self', 'address', slave)
            #写回配置文件
            with open('tmp.conf', 'w') as f:
                tmp.write(f)
            subprocess.call(['scp', 'tmp.conf', slave + ':config.conf'])
        
        #删除不必要的文件
        os.remove('tmp.conf')
        os.remove('slaves.py')

        #块的大小
        block_size = cf.getint('master', 'block_size')
        #复制的块数
        replication_factor = cf.getint('master', 'replication_factor')

        #先对文件做分割
        split(fileName, block_dir, block_size)
        #再发送到各个slaves上去
        saveToSlaves(fileName, block_dir, slavesList, replication_factor)
    elif commond == 'load':
        if len(args) < 3:
            print 'Not enough parameters for load function in master.py!'
            sys.exit()
        
        #客户端地址
        client = cf.get('client', 'address')
        #客户端的目标文件名
        destFileName = args[2]
        #从各个slaves中将文件取出
        loadFromSlaves(fileName, block_dir, destFileName, client)
    elif commond == 'slaveExistFile':
        print slavesExistFile(fileName)
    elif commond == 'addSlaveToFile':
        if len(args) < 3:
            print 'Not enough parameters for load function in master.py!'
            sys.exit()
        slave = args[2]
        addSlaveToFile(fileName, slave)
        
if __name__ == '__main__':
    main(sys.argv[1:])