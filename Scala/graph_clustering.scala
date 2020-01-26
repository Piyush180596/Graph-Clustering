import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.{SortedMap, mutable}
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import java.io.{File, PrintWriter}


object piyush_chaudhari_task1 {
  def main(args : Array[String]) : Unit = {
    val conf1 = new SparkConf().setAppName("Betweenness").setMaster("local[*]")
    val sc1 = new SparkContext(conf1)
    val filterThreshold = args(0).toInt
    val yelpDataFile = args(1)
    val betweennessOutputFile = args(2)
    val communityOutputFile = args(3)
//    val yelpDataFile = "C:\\Users\\cpiyu\\Downloads\\INF 553 Data Mining\\Assignments\\Assignment 4\\sample_data.csv"
    val yelpData = sc1.textFile(yelpDataFile)
    val yelpRDD = yelpData.map(x => x.split(",")).filter(row => row(0) != "user_id").map(x => ((x(0), List(x(1))))).reduceByKey((x, y) => x ::: y).collectAsMap()
    val allUsers = yelpRDD.keys.toList
    //    print("Temp : "+temp)

    var combinations = allUsers.combinations(2)
    //    print("Comb : "+combinations.length)
    //    val loop = new Breaks;
    var allEdges = ListBuffer.empty[(String, String)]
    for (comb <- combinations) {
      var firstBusinessSet = yelpRDD(comb(0)).toSet
      var secondBusinessSet = yelpRDD(comb(1)).toSet
      var intersection = firstBusinessSet.intersect(secondBusinessSet)
      var firstEdge = Tuple2(comb(0), comb(1))
      var secondEdge = Tuple2(comb(1), comb(0))
      if (intersection.size >= filterThreshold) {
        allEdges = allEdges :+ firstEdge
        allEdges = allEdges :+ secondEdge
      }
    }
//    print("all edges : "+allEdges.size)
    var adjacencyList = Map[String, List[String]]().withDefaultValue(List())
    for (edge <- allEdges) {
      adjacencyList(edge._1) = edge._2 :: adjacencyList(edge._1)
    }

    var adjacencyListUpdated = Map[String, ListBuffer[String]]().withDefaultValue(ListBuffer())
//    for (edge <- allEdges) {
//      println("Node : "+(edge._1,edge._2))
//      println("Init : "+adjacencyListUpdated(edge._1).size)
//      adjacencyListUpdated(edge._1) = adjacencyListUpdated(edge._1) += edge._2
//      println("Final : "+adjacencyListUpdated(edge._1).size)
//    }

    var count2 = 0
    for(node <- adjacencyList.keys){
      adjacencyListUpdated(node) = adjacencyList(node).to[ListBuffer]
      count2 += adjacencyList(node).size
    }
    var count1 = 0
    for(node <- adjacencyListUpdated.keys){
      count1 += adjacencyListUpdated(node).size
    }
//    println("Nodes : " + adjacencyListUpdated.keys.size)
//    println("Original edges : " + count2)
//    println("New edges : "+count1)

    val users = adjacencyList.keys

    def GN(start: String, graph: Map[String, ListBuffer[String]]): Map[(String, String), Double] = {
      var edgeBetweenness = Map.empty[(String, String), Double].withDefaultValue(0)
      var head = start
      var explored = ListBuffer[String]()
      var queue = Queue[String]()
      queue += start
      var bfsTree = ListBuffer[String]()
      var levels = Map.empty[String, Int]
      var parentDict = Map.empty[String, List[String]].withDefaultValue(List())
      var treeLevel = Map.empty[String, Double].withDefaultValue(0)
      var children = ListBuffer[String]()
      //      levels(start) = 0
      levels += start -> 0
      //      treeLevel(head) = 1
      treeLevel += head -> 1
      //      print("queue",queue)
      while (queue.isEmpty == false) {
        val node = queue.dequeue()
        //        print("node : "+node)
        explored += node
        //        print("explored : ",explored)
        children = graph(node)

        //        print("Node : "+node+" Children : "+children)
        for (child <- children) {
          if (!explored.contains(child)) {
            if (!queue.contains(child)) {
              bfsTree += child
              queue += child
              levels(child) = levels(node) + 1
            }
            if (levels(child) != levels(node)) {
              //println("Node : "+node)
              //              println("Init : ",parentDict(child).size)
              parentDict(child) = node :: parentDict(child)
              //              println("Final : ",parentDict(child).size)
              treeLevel(child) += treeLevel(node)
            }
          }
        }
      }

      //print("bfs size : "+bfsTree.size)
      var nodeDict = Map.empty[String, Double].withDefaultValue(1)
      for (i <- bfsTree.size - 1 to 0 by -1) {
        var addition:Double = 0
        var currentNode = bfsTree(i)
        if (parentDict.contains(currentNode)) {
          var parents = parentDict(currentNode)
          for (parent <- parents) {
            //            println("parent : "+treeLevel(parent))
            //            println("curr_level : "+treeLevel(currentNode))
            //            println("curr_node : "+nodeDict(currentNode))

            addition = ((treeLevel(parent) / treeLevel(currentNode)) * nodeDict(currentNode))
            //            println("init : "+nodeDict(parent))
            nodeDict(parent) += addition
            //            println("addition : "+addition)
            //            var edge = (currentNode,parent)
            var edge = (List(currentNode, parent).sortWith(_ < _))
            var t = edge match {
              case List(a, b) => (a, b)
            }
            //            print("Edge : "+edge+" Betwenness : "+addition)
            edgeBetweenness(t) = addition
          }
        }
      }
      //      print(edgeBetweenness)
      return edgeBetweenness

    }

      def returnBetweenness(graph:Map[String, ListBuffer[String]]): Map[(String, String), Double] ={
        var combinedBetweenness = Map.empty[(String, String), Double].withDefaultValue(0)
        var finalEdgeBetweenness = Map.empty[(String, String), Double]
        var count = 0
        for (user <- users) {
          var singleNodeBetweenness = GN(user, graph)

          //      println("user : "+user)
          //println("Intermediate edges : "+singleNodeBetweenness.keys.size)

          combinedBetweenness = combinedBetweenness ++ singleNodeBetweenness.map { case (k, v) => k -> (v + combinedBetweenness.getOrElse(k, 0.toDouble)) }
          //      combinedBetweenness = (combinedBetweenness.keySet ++ singleNodeBetweenness.keySet).map {i=> (i,combinedBetweenness.getOrElse(i,0.0f) + singleNodeBetweenness.getOrElse(i,0.0f))}.toMap


          //      }
          singleNodeBetweenness.clear()
        }
        finalEdgeBetweenness = combinedBetweenness
        //    print("final : ", combinedBetweenness.toSeq.sortBy(-_._2))
        //    println("size : ", combinedBetweenness.keys.size)
        for((k,v) <- finalEdgeBetweenness){
          finalEdgeBetweenness(k) = v/2
        }
        var a = finalEdgeBetweenness.toList.sortBy(-_._2).toMap
        return finalEdgeBetweenness
      }
//    print(a)
//        print("final : ", finalEdgeBetweenness.toSeq.sortBy(-_._2))
    //    var a = sc1.parallelize(allUsers).map(x => GN(x,adjacencyList))

    var aValuesDict = Map.empty[(String, String), Double]
    var edgeCount = (allEdges.size / 2).toDouble
//    println("edges : "+edgeCount)
    var nodeDegreeDict = Map.empty[String, Double]
    var modularity:Double = 0
    var A:Double = 0
//    var difference = 0.0f
    for (node <- users) {
      nodeDegreeDict(node) = adjacencyListUpdated(node).size
    }
    for (initialNode <- users) {
      for (finalNode <- users) {
        if (adjacencyListUpdated(initialNode).contains(finalNode)) {
          A = 1
        }
        else {
          A = 0
        }
        var difference = (A - ((nodeDegreeDict(initialNode) * nodeDegreeDict(finalNode)) / (2 * edgeCount))) / (2 * edgeCount)
        var edge = (initialNode, finalNode)
        aValuesDict(edge) = A
        modularity += difference
      }
    }

//    var count = 0
//    for((k,v) <- aValuesDict){
//      if(v == 1){
//        count += 1
//      }
//    }
//    print("Count : "+count)
//    println("Null modularity : " + modularity)

    def DFS(presentConnectedComponent: ListBuffer[String], v: String, visited: ListBuffer[String], graph: Map[String, ListBuffer[String]]): ListBuffer[String] = {
      var tempVisited = ListBuffer.empty[String]
      var tempConnectedComponents = ListBuffer.empty[String]
      tempVisited = visited
      tempConnectedComponents = presentConnectedComponent
//      println("Node : "+v)
//      println("init visited : "+tempVisited.size)
      tempVisited += v
//      println("updated visited: "+tempVisited.size)
//      println("init cc: "+tempConnectedComponents.size)
      tempConnectedComponents += v
//      println("updated cc: "+tempConnectedComponents.size)
//      println()
//      println()
      for (u <- graph(v)) {
        if (!tempVisited.contains(u)) {
          tempConnectedComponents = DFS(tempConnectedComponents, u, tempVisited, graph)
        }
      }
      return tempConnectedComponents
    }

    def connectedComponents(graph: Map[String, ListBuffer[String]]): ListBuffer[ListBuffer[String]] = {
      var visited = ListBuffer.empty[String]
      var allConnectedComponents = ListBuffer.empty[ListBuffer[String]]
      for (i <- graph.keys) {
        if (!visited.contains(i)) {
          var temp = ListBuffer.empty[String]
          //          var ret = DFS(temp, i, visited, graph)
          allConnectedComponents.append(DFS(temp, i, visited, graph))
//          println("cc length : "+allConnectedComponents)
        }
      }
      return allConnectedComponents
    }

    /*
    var aValuesDict = Map.empty[(String,String),Double]
    var edgeCount = allEdges.size/2.toDouble
    var nodeDegreeDict = Map.empty[String,Double]
    */
    var initialConnectedComponents = ListBuffer.empty[ListBuffer[String]]
    initialConnectedComponents = connectedComponents(adjacencyListUpdated)
    var firstModularity = modularity
//    println("Connected components : "+initialConnectedComponents.size)
    var modelConfigiration = (modularity,initialConnectedComponents)

    def calculateModularity(nodeDegreeDict: Map[String, Double], community: ListBuffer[String], edgeCount: Double, aValuesDict:Map[(String, String), Double]): Double = {
      var communityModularity:Double = 0
      var A:Double = 0
      for (initialNode <- community) {
        for (finalNode <- community) {
          A = aValuesDict((initialNode,finalNode))
          var difference = (A - (nodeDegreeDict(initialNode) * nodeDegreeDict(finalNode) / (2 * edgeCount))) / (2 * edgeCount)
          communityModularity += difference
        }
      }
      return communityModularity
    }

    def swap(originalDict:Map[(String, String), Double]):mutable.LinkedHashMap[Double,ListBuffer[(String,String)]] = {
//      var returnBetweennessDict = mutable.LinkedHashMap.empty[Double,ListBuffer[(String,String)]].withDefaultValue(ListBuffer())
      var reverseBetweennessDict = Map.empty[Double,List[(String,String)]].withDefaultValue(List())
      var tempReverseBetweennessDict = Map.empty[Double,ListBuffer[(String,String)]].withDefaultValue(ListBuffer())
      var test = Map.empty[Double,(String,String)].withDefaultValue((" "," "))
      for((k,v) <- originalDict){
//        print("Initial : "+reverseBetweennessDict(v).size)
//        println("Key : "+reverseBetweennessDict(v))
//        reverseBetweennessDict(v).append(k)
        reverseBetweennessDict(v) = k :: reverseBetweennessDict(v)
//        print(", final : "+reverseBetweennessDict(v).size)
//        println()
      }
      for(node <- reverseBetweennessDict.keys){
        tempReverseBetweennessDict(node) = reverseBetweennessDict(node).to[ListBuffer]

      }
      var returnBetweennessDict = mutable.LinkedHashMap(tempReverseBetweennessDict.toSeq.sortBy(-_._1):_*)
      return returnBetweennessDict
    }

    var finalEdgeBetweenness = returnBetweenness(adjacencyListUpdated)

    val betweennessToWrite = mutable.LinkedHashMap(finalEdgeBetweenness.toSeq.sortBy(_._1).sortBy(-_._2):_*)
    val file = new PrintWriter(new File(betweennessOutputFile))
//    file.write("hi")
    //    val bw = new BufferedWriter(new FileWriter(file))
    var length = betweennessToWrite.keys.size
    var counter = 1
    for((k,v) <- betweennessToWrite){
//      file.write("hello")
//      print("key : "+k+" value : "+v)
      file.write("('")
      file.write(k._1.toString)
      file.write("', '")
      file.write(k._2.toString)
//      file.write(k.toString())
      file.write("')")
      file.write(", ")
      file.write(v.toString())
      if(counter < length){
        file.write("\n")
      }
      counter += 1
    }
    file.close()
//    println("Write done")

//    print("final betweenness : "+finalEdgeBetweenness)
    var returnBetweennessDict = swap(finalEdgeBetweenness)
//    var d = mutable.LinkedHashMap(c.toSeq.sortBy(-_._1):_*)
//    var reverseBetweennessDict  = collection.mutable.Map(c.toSeq: _*)


//    println("final : "+finalEdgeBetweenness)
//    println("final2 : "+finalEdgeBetweenness.valuesIterator.max)
//    println("original_! : "+b.head._2)
//    println("reversed : "+returnBetweennessDict)


    breakable{
      while (true){
        var temp = returnBetweennessDict
        var a = temp.keys.head
//        print("key : "+a)

//        var count1 = 0
//        for(node <- adjacencyListUpdated.keys){
//          count1 += adjacencyListUpdated(node).size
//        }

//        println("Initial length : "+count1)
        for(edge <- returnBetweennessDict(a)){
//          println("Removing edge : "+edge+" with betweenness : "+ a)
          adjacencyListUpdated(edge._1) -= edge._2
          adjacencyListUpdated(edge._2) -= edge._1
        }

//        var count2 = 0
//        for(node <- adjacencyListUpdated.keys){
//          count2 += adjacencyListUpdated(node).size
//        }
//        println("Final length : "+count2)

        var newModularity:Double = 0
        var communites = ListBuffer.empty[ListBuffer[String]]
        communites = connectedComponents(adjacencyListUpdated)
        for(community <- communites){
          newModularity += calculateModularity(nodeDegreeDict,community,edgeCount,aValuesDict)
        }

        modularity = newModularity
//        println("Connected Comp : "+initialConnectedComponents.size+" Mod : "+modularity+" Old Mod : "+firstModularity)
        if(modularity > firstModularity){
           firstModularity = modularity
           initialConnectedComponents = communites

        }
        finalEdgeBetweenness = returnBetweenness(adjacencyListUpdated)
        returnBetweennessDict.clear()
        var updatedBetweennessDict = swap(finalEdgeBetweenness)
        returnBetweennessDict = updatedBetweennessDict

        var count = 0
        for((k,v) <- adjacencyListUpdated){
          count += v.size
        }
        if(count == 0){
          break()
        }
      }

    }
//    println("Size : "+initialConnectedComponents.size)

    var finalConnectedComponents = ListBuffer.empty[ListBuffer[String]]
    for (component <- initialConnectedComponents){
      var community = component.sorted
      finalConnectedComponents.append(community)
    }
    def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

    var a = sort(finalConnectedComponents)
    var sortedConnectedComponents = a.sortWith(_.size < _.size)
//    println("a : "+sortedConnectedComponents)
    val communityFile = new PrintWriter(new File(communityOutputFile))
    var communityLength = sortedConnectedComponents.size
    var communityCount = 1
    for(community <- sortedConnectedComponents){
//      println()

      var length = community.size
      var count = 1
      for(component <- community){
        communityFile.write("'")
        communityFile.write(component.toString)
        communityFile.write("'")
        if(count < length){
          communityFile.write(", ")
        }
        count += 1
      }
      if(communityCount < communityLength){
        communityFile.write("\n")
      }
      communityCount += 1
    }
    communityFile.close()
    sc1.stop()
  }
}