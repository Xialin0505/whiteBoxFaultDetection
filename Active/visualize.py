import numpy as np
import matplotlib.pyplot as plt
from textwrap import wrap
import os
import sys

headings = [('filename1', "|S20"), ('l0', float), ('l1', float), ('l2', float)]

def parseRaw(arr):
    timeline = []
    minTime = []
    maxTime = []
    avgTime = []

    for i in arr:
        timeline.append('\n'.join(wrap(i[0].decode('UTF-8'), 12)))
        minTime.append(i[1])
        maxTime.append(i[2])
        avgTime.append(i[3])

    timeline = np.array(timeline)
    minTime= np.array(minTime)
    maxTime = np.array(maxTime)
    avgTime = np.array(avgTime)

    return timeline, minTime, maxTime, avgTime

def plotLineGraph(timeline, minTime, maxTime, avgTime, title):
    plt.plot(timeline, minTime, label = "min latency") 
    plt.plot(timeline, maxTime, label = "max latency") 
    plt.plot(timeline, avgTime, label = "avg latency")
    plt.xticks(fontsize=5)
    plt.title(title) 
    plt.legend() 
    plt.show()

def generateRM():
    arr = np.genfromtxt("./log/RMGFD.csv", delimiter=",", dtype = headings)

    timeline, minTime, maxTime, avgTime = parseRaw(arr)
    plotLineGraph(timeline, minTime, maxTime, avgTime)

def generateLFDs():
    LFDlist = ["LFD1S1", "LFD2S2", "LFD3S3"]

    for i in LFDlist:
        filename = "./log/" + i + ".csv"
        if os.path.isfile(filename):
            generateLFD(filename, i)

def generateLFD(fileName, title):
    arr = np.genfromtxt(fileName, delimiter=",", dtype = headings)
    timeline, minTime, maxTime, avgTime = parseRaw(arr)
    plotLineGraph(timeline, minTime, maxTime, avgTime, title)

def generateGFDs():
    LFDlist = ["LFD1", "LFD2", "LFD3"]

    for i in LFDlist:
        filename = "./log/" + i + ".csv"
        if os.path.isfile(filename):
            generateGFD(filename)

def generateGFD(fileName, title):
    arr = np.genfromtxt(fileName, delimiter=",", dtype = headings)
    timeline, minTime, maxTime, avgTime = parseRaw(arr)
    plotLineGraph(timeline, minTime, maxTime, avgTime, title)

def generateServers():
    serverClient = ["C1S1", "C2S1", "C3S1", "C1S2", "C2S2", "C3S2", "C1S3", "C2S3", "C3S3"]

    for i in serverClient:
        filename = "./log/" + i + ".csv"
        if os.path.isfile(filename):
            generateServer(filename, i)

def generateServer(fileName, title):
    arr = np.genfromtxt(fileName, delimiter=",", dtype = headings)
    timeline, minTime, maxTime, avgTime = parseRaw(arr)
    plotLineGraph(timeline, minTime, maxTime, avgTime, title)

if __name__=="__main__": 
    if (sys.argv[1] == "client"):
        generateServers()
    elif (sys.argv[1] == "gfd"):
        generateGFDs()
    elif (sys.argv[1] == "rm"):
        generateRM()
    elif (sys.argv[1] == "lfd"):
        generateLFDs()