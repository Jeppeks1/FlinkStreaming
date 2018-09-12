package project.clustering

import project.container.SiftDescriptor
import project.container.Cluster
import scala.collection.immutable.Queue
import java.io.Serializable
import scala.util.Random
import scala.math.{ceil, floor, pow}

@SerialVersionUID(123L)
class IndexTree[A] extends Serializable {

  case class InternalNode(children: Vector[InternalNode],
                          cluster: Cluster[A],
                          level: Int) extends Serializable{
    override def toString: String = cluster.clusterLeader.toString
  }

  var leafs: Vector[InternalNode] = _
  var root: InternalNode = _
  var levels: Int = -1

  def getNumberOfLeaders: Int = leafs.size

  def pickRandomLeaders(points: Vector[SiftDescriptor[A]], n: Int, seed: Long): Vector[SiftDescriptor[A]] = {
    val rng = new Random(seed)
    var vec = Vector.fill(n)(points(rng.nextInt(points.size)))

    // TODO: There must be a better way to do this
    while (vec.distinct.size != n){
       vec = vec :+ points(rng.nextInt(points.size))
    }

    vec.distinct
  }

  def toInternalNode(points: Vector[SiftDescriptor[A]],
                     children: Vector[InternalNode],
                     level: Int): Vector[InternalNode] = {
    points.map(SD => InternalNode(children, new Cluster(SD), level))
  }


  def buildIndexTree(points: Vector[SiftDescriptor[A]], L: Int, a: Int): Unit = {
    val IOGranularity = 128 * 1024 // 128 KB - based on Linux OS
    val inputSize = points.size
    val descriptorSize = inputSize * 4 // 400 Bytes
    val leaderCount = ceil(points.size / floor(IOGranularity / descriptorSize)).toInt
    val leaderCount2 = ceil(pow(inputSize, 1.0 - 1.0/(L + 1.0))).toInt

    this.leafs = toInternalNode(pickRandomLeaders(points, leaderCount2, System.currentTimeMillis), Vector(), L)
    this.levels = L

    if (L == 1) return

    // Build the index tree
    root = extendedClusterPruning(L, a, inputSize)

    // Assign the points to clusters

  }


  def extendedClusterPruning(L: Int, a: Int, n: Int): InternalNode = {

    var S = leafs
    for (i <- 2 to L){
      var nodesAtPreviousLevel = S.map(_.cluster.clusterLeader)
      var numberOfNodesAtThisLevel = ceil(pow(n, 1.0 - i/(L + 1.0))).toInt
      var setL = toInternalNode(pickRandomLeaders(nodesAtPreviousLevel,
                                                  numberOfNodesAtThisLevel,
                                                  System.currentTimeMillis), S, L - i + 1)

      S = setL
    }

    InternalNode(S, null, 1)
  }


}

// Delegate methods to object for easy imports
object IndexTree {

}
