package project.local.container

import java.io.Serializable

class Cluster[A](var points: Vector[SiftDescriptor[A]]
                ,var clusterLeader: SiftDescriptor[A]) extends Serializable{

  // The leader itself is added to the cluster when assigning points to clusters
  def this(leader: SiftDescriptor[A]){
    this(Vector(), leader)
  }

  def addPoint(point: SiftDescriptor[A]): Unit = {
    this.points = points :+ point
  }

  def addPoints(pointsIn: Vector[SiftDescriptor[A]]): Unit = {
    this.points = points ++ pointsIn
  }

  def getSize: Long = points.size

  def kNearestNeighbor(k: Int): Unit ={

  }



}


object Cluster {

  def combine[A](c1: Cluster[A], c2: Cluster[A]): Unit ={
    c1.addPoints(c2.points)
  }


}
