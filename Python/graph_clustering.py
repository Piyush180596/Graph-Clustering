import pyspark
import itertools
from itertools import combinations
from pyspark import SparkContext
import csv
import sys

# sc1 = SparkContext('local[*]','',conf=conf)
# conf = pyspark.SparkConf()
# conf.set("spark.executor.heartbeatInterval","5s")
# conf.set("spark.network.timeout","10s")
sc = SparkContext("local[*]", "GN")

# yelpFile = "C:\\Users\\cpiyu\\Downloads\\INF 553 Data Mining\\Assignments\\Assignment 4\\sample_data.csv"
filterThreshold = int(sys.argv[1])
yelpFile = sys.argv[2]
betweennessOutputFile = sys.argv[3]
communityOutputFile = sys.argv[4]
yelpData = sc.textFile(yelpFile)
yelpDataRDD = yelpData.map(lambda x:x.split(",")).filter(lambda x:str(x[0]) != 'user_id').map(lambda x: (x[0],[x[1]]))
groupedUsers = yelpDataRDD.reduceByKey(lambda x,y:x+y).collectAsMap()
# distinctUsers = yelpDataRDD.map(lambda x:x[0]).distinct()
allUsers = groupedUsers.keys()
allEdges = list()
comb = combinations(allUsers,2)
for c in list(comb):
  firstBusinessSet = set()
  secondBusinessSet = set()
  firstBusinessSet = set(groupedUsers[c[0]])
  secondBusinessSet = set(groupedUsers[c[1]])
  if(len(firstBusinessSet.intersection(secondBusinessSet)) >= filterThreshold):
    allEdges.append((c[0],c[1]))
    allEdges.append((c[1],c[0]))

# print("all edges : ",len(allUsers))

adjacencyList = dict()
for edge in allEdges:
  adjacencyList.setdefault(edge[0],[])
  adjacencyList[edge[0]] += [edge[1]]

# print("Number of nodes : ",len(adjacencyList.keys()))
filteredUsers = adjacencyList.keys()
initialAdjacencyList = adjacencyList


def GirvanNewmann(graph, start):

  edgeBetweenness = dict()
  # keep track of all visited nodes
  head = start
  explored = []
  # keep track of nodes to be checked
  queue = [start]
  bfsTree = list()
  bfsTree.append(start)
  parentDict = dict()
  levels = dict()
  levels[start] = 0
  treeLevel = {head:1}
  while queue:
      node = queue.pop(0)
      explored.append(node)
      children = graph[node]
      for child in children:
          if child not in explored:
              if(child not in queue):
                bfsTree.append(child)
                queue.append(child)
                levels[child]= levels[node]+1
              parentDict.setdefault(child,[])
              treeLevel.setdefault(child,0)
              if(levels[child] != levels[node]):  
                parentDict[child] += [node]
                treeLevel[child] += treeLevel[node]
              # print(treeLevel)
      # print(parentDict)  
      # treeLevel.setdefault(node,0)
      # treeLevel[node] += treeLevel[parentDict[node]]     
  # print("Parent : ",parentDict)
  # print("SP : ",treeLevel)
  # print(levels)
  # print(bfsTree)
  # print("bfs length : ",len(bfsTree))
  nodeDict = dict()
  
  for i in range(len(bfsTree) - 1,-1,-1):
    currentNode = bfsTree[i]
    nodeDict.setdefault(currentNode,1)
    if(currentNode in parentDict):  
      parents = parentDict[currentNode]
      for parent in parents:
        nodeDict.setdefault(parent,1)
        # addition = float(nodeDict[currentNode]/len(parents))
        addition = float((treeLevel[parent]/treeLevel[currentNode]) * nodeDict[currentNode])
        nodeDict[parent] += addition
        #edge = (min(currentNode,parent),max(currentNode,parent))
        edge = tuple(sorted(tuple((currentNode,parent))))
        edgeBetweenness.setdefault(edge,0)
        edgeBetweenness[edge] = addition
  return edgeBetweenness
  # print("len : ",len(edgeBetweenness.keys()))
  # return edgeBetweenness
  # return (edge,edgeBetweenness[edge])

def returnBetweenness(a):
  combinedBetweenness = dict()
  finalEdgeBetweenness = dict()
  for j,i in enumerate(filteredUsers):
    singleNodeBetweenness = dict()
    singleNodeBetweenness = GirvanNewmann(a,i)
    if(j == 0):
      combinedBetweenness = singleNodeBetweenness.copy()
      # print("2 : ",combinedBetweenness)
      # print("1 : ",singleNodeBetweenness)
    else:
      # print("Every iteration length : ",len(singleNodeBetweenness.keys()))
      # print("2 : ",combinedBetweenness)
      # print("1 : ",singleNodeBetweenness)
      combinedBetweenness = {x: combinedBetweenness.get(x, 0) + singleNodeBetweenness.get(x, 0) for x in set(combinedBetweenness).union(singleNodeBetweenness)}

      # combinedBetweenness = combinedBetweenness.updatetemp1
  finalEdgeBetweenness = combinedBetweenness
  # print("Size : ",len(combinedBetweenness.keys()))
  for k, v in finalEdgeBetweenness.items():
    finalEdgeBetweenness[k] = float(v/2)
  finalEdgeBetweenness = dict(sorted(finalEdgeBetweenness.items(),key = lambda x:x[0][1]))
  finalEdgeBetweenness = dict(sorted(finalEdgeBetweenness.items(),key = lambda x:x[0][0]))
  finalEdgeBetweenness = dict(sorted(finalEdgeBetweenness.items(),key = lambda x:x[1],reverse=True))
  return finalEdgeBetweenness

finalEdgeBetweenness = returnBetweenness(adjacencyList)
adjacencyListBackup = adjacencyList
# print(finalEdgeBetweenness)
# finalEdgeBetweenness = dict()
# for i in filteredUsers:
#   GirvanNewmann(adjacencyList,i)
# print("Size : ",len(edgeBetweenness.keys()))
# for k, v in edgeBetweenness.items():
#   finalEdgeBetweenness[k] = float(sum(v)/2)
  # print(k, sum(v))

# finalEdgeBetweenness = dict(sorted(finalEdgeBetweenness.items(),key = lambda x:x[1],reverse=True))
edge = ()
aValuesDict = dict()
edgeCount = len(allEdges)/2
nodeDegreeDict = dict()
for node in filteredUsers:
  nodeDegreeDict[node] = len(adjacencyList[node])
modularity = 0
for initialNode in filteredUsers:
  for finalNode in filteredUsers:
    if(finalNode in adjacencyList[initialNode]):
      A = 1
    else:
      A = 0
    difference = (A - 0.5*nodeDegreeDict[initialNode]*nodeDegreeDict[finalNode]/edgeCount)/(2*edgeCount)
    # edge = tuple(sorted((initialNode,finalNode)))
    # aValuesDict[edge] = A
    aValuesDict[(initialNode,finalNode)] = A
    modularity += difference
# print("Null modularity : ",modularity)

def DFS(presentConnectedComponent,v,visited,graph):
  visited.append(v)
  presentConnectedComponent.append(v)
  # print(temp)
  for u in graph[v]:
    if(u not in visited):
      # print("u",u)
      presentConnectedComponent = DFS(presentConnectedComponent,u,visited,graph)
  return presentConnectedComponent    

def connectedComponents(graph):
  visited = list()
  allConnectedComponents = list()
  for i in graph.keys():
    # print(i)
    if(i not in visited):
      temp = []
      allConnectedComponents.append(DFS(temp,i,visited,graph))
  return allConnectedComponents

initialConnectedComponents = connectedComponents(adjacencyList)
modelConfiguration = (modularity,initialConnectedComponents)  

# def calculateModularity(adjacencyList,community,edgeCount,aValuesDict):
#   # print("edge : ",edgeCount)
#   nodeDegreeDict = dict()
#   accessed = list()
#   for node in community:
#     nodeDegreeDict[node] = len(adjacencyList[node])
#   modularity = 0
#   edge = ()
#   for initialNode in community:
#     for finalNode in community:
#       edge = tuple(sorted((initialNode,finalNode)))
#       if(edge not in accessed):
#         A = aValuesDict[edge]
#         difference = (A - 0.5*nodeDegreeDict[initialNode]*nodeDegreeDict[finalNode]/edgeCount)/(2*edgeCount)
#         modularity += difference
#         accessed.append(edge)
#         edge = ()  
#   return modularity

def calculateModularity(nodeDegreeDict,community,edgeCount,aValuesDict):
  
  modularity = 0
  for initialNode in community:
    for finalNode in community:
      A = aValuesDict[(initialNode,finalNode)]
      difference = (A - 0.5*nodeDegreeDict[initialNode]*nodeDegreeDict[finalNode]/edgeCount)/(2*edgeCount)
      modularity += difference  
  return modularity

mod = 0

# for community in initialConnectedComponents:
#   mod += calculateModularity(adjacencyList,community,edgeCount,aValuesDict)
# modelConfiguration = (modularity,initialConnectedComponents)
# print("init : ",mod)

def swap(b):
  reverseBetweennessDict = dict()
  for k,v in b.items():
    reverseBetweennessDict.setdefault(v,[])
    reverseBetweennessDict[v] += [k]
  return reverseBetweennessDict
# print(reverseBetweennessDict)
reverseBetweennessDict = swap(finalEdgeBetweenness)
# print("initial : ",reverseBetweennessDict)

i = 0
while(1):
# for val in reverseBetweennessDict.items():
  # print("key : ",val)
  
  # print("rev : ",reverseBetweennessDict)
  
  temp = reverseBetweennessDict
  for edge in reverseBetweennessDict[list(temp.keys())[0]]:
    # print("Iteration no : ",i," ,Removing edge : ",edge," with betweenness value : ",list(temp.keys())[0])
    adjacencyList[edge[0]].remove(edge[1])
    adjacencyList[edge[1]].remove(edge[0])
  # print("\n")
  i = i+1
    # l = 0
    # for i in adjacencyList.keys():
    #   l += len(adjacencyList[i])
    # print("len : ",l)
  newModularity = 0
  communities = connectedComponents(adjacencyList)
    
  for community in communities:
    newModularity += calculateModularity(nodeDegreeDict,community,edgeCount,aValuesDict)
  modularity = newModularity
  # print("Modularity after removing ",modularity)
  if(modularity > modelConfiguration[0]):
    modelConfiguration = (modularity,communities)
  # print("final : ",modelConfiguration)

  l = 0
  # for i in adjacencyList.keys():
  #   l += len(adjacencyList[i])
  # print("len : ",l)  
  finalEdgeBetweennessDict = returnBetweenness(adjacencyList)
  # print("updated dict : ",finalEdgeBetweennessDict)
  reverseBetweennessDict.clear()
  updatedBetweennessDict = dict()
  updatedBetweennessDict = swap(finalEdgeBetweennessDict) 
  reverseBetweennessDict = updatedBetweennessDict.copy()
  # print("Comm : ",len(communities), ". Modularity : ",modularity)
  # print("final : ",reverseBetweennessDict)

  count = 0
  for k,v in adjacencyList.items():
    count += len(v)
  if(count == 0):
    break

# print(len(modelConfiguration[1]))
# print(modelConfiguration[1])
finalCommunities = modelConfiguration[1]

for value in finalCommunities:
  value.sort()
asd = sorted(finalCommunities)
# finalCommunities.sort(key = len)
finalCommunities = sorted(asd,key=len)
# finalCommunities = sorted(,key=len)
# print(finalCommunities)
# print("Maximum modularity : ",modelConfiguration[0])

temp1 = ""
fo = open(betweennessOutputFile,'w')
for k,v in finalEdgeBetweenness.items():
  temp1 += str(k) + ', '+ str(v) + '\n'
fo.write(temp1.rstrip())
fo.close()

temp = ""
with open(communityOutputFile,'w',newline='') as f:
  # csv_writer = csv.writer(f)
  # csv_writer.writerows(finalCommunities)
  for value in finalCommunities:
    temp += str(value)[1:-1] + "\n"
  f.write(temp.rstrip())
f.close()
# print(finalCommunities)

sc.stop()
