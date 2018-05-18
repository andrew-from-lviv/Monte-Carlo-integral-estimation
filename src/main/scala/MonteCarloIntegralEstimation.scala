import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread, RecursiveTask}

import org.scalameter

import scala.util.Random
import org.scalameter._

object MonteCarloIntegralEstimation {

  val forkJoinPool = new ForkJoinPool

  def task[T](computation: => T): RecursiveTask[T] = {
    val t = new RecursiveTask[T] {
      def compute = computation
    }

    Thread.currentThread match {
      case wt: ForkJoinWorkerThread =>
        t.fork() // schedule for execution
      case _ =>
        forkJoinPool.execute(t)
    }

    t
  }

  def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
    val right = task { taskB }
    val left = taskA

    (left, right.join())
  }

  def parallel[A, B, C, D](taskA: => A, taskB: => B, taskC: => C, taskD: => D): (A, B, C, D) = {
    val ta = task { taskA }
    val tb = task { taskB }
    val tc = task { taskC }
    val td = taskD
    (ta.join(), tb.join(), tc.join(), td)
  }

  def integFunc(x: Double): Double = {
    x*x
  }

  def integrate(a: Double, b: Double, integrableFunc: Double => Double, totalNumberOfPoints: Int): Double = {
    val rndx = new Random()
    val rndy = new Random()
    var sum = 0.0
    def simulation(pointsGenerated: Int): Double =
      if (pointsGenerated >= totalNumberOfPoints)
        sum
      else{
        val x = a + rndx.nextDouble()*(b-a)  //The random point in the interval [a,b]
        sum+=integrableFunc(x)*(b-a)
        simulation(pointsGenerated+1)
      }
    simulation(0)/totalNumberOfPoints
  }

  def integratePar1(a: Double, b: Double, func: Double => Double, numberOfPoints: Int): Double = {
    val (sum1, sum2)  = parallel(integrate(a, b, func, numberOfPoints/2), integrate(a, b, func, numberOfPoints/2))
    (sum1 + sum2)/2
  }

  def integrateParTwoProcesses(a: Double, b: Double, integrableFunc: Double => Double, totalNumberOfPoints: Int): Double = {
    val (integr1, integr2)  = parallel(integrate(a, b, integrableFunc, totalNumberOfPoints/2),
      integrate(a, b, integrableFunc, totalNumberOfPoints/2))
    (integr1 + integr2)/2
  }

  def integrateParFourProcesses(a: Double, b: Double, integrableFunc: Double => Double, totalNumberOfPoints: Int): Double = {
    val (integr1, integr2, integr3, integr4)  = parallel(integrate(a, b, integrableFunc, totalNumberOfPoints/4),
      integrate(a, b, integrableFunc, totalNumberOfPoints/4),
      integrate(a, b, integrableFunc, totalNumberOfPoints/4),
      integrate(a, b, integrableFunc, totalNumberOfPoints/4))
    (integr1 + integr2 + integr3 + integr4)/4
  }

  def integrateParHalfs(a: Double, b: Double, integrableFunc: Double => Double, totalNumberOfPoints: Int): Double = {
    val middle = a + (b - a)/2
    val (integr1, integr2)  = parallel(integrate(a, middle, integrableFunc, totalNumberOfPoints),
      integrate(middle, b, integrableFunc, totalNumberOfPoints))
    integr1 + integr2
  }




  def main(args: Array[String]): Unit = {
    val totalNumberOfPoints = 50000
    val a = 0
    val b = 2


    val standardConfig = config(
      Key.exec.minWarmupRuns -> 100,
      Key.exec.maxWarmupRuns -> 300,
      Key.exec.benchRuns -> 100,
      Key.verbose -> false) withWarmer new scalameter.Warmer.Default

    println(s"seq res: ${integrate(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)}")
    println(s"seq res: ${integratePar1(a = a, b = b, func= integFunc, numberOfPoints= totalNumberOfPoints)}")
    println(s"seq res: ${integrateParFourProcesses(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)}")
    println(s"seq res: ${integrateParHalfs(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)}")

    val seqtime = standardConfig measure{
      integrate(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)
    }

    println(s"seq time ${seqtime.value}")

    val partimeTwo = standardConfig measure{
      integrateParTwoProcesses(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)
    }

    println(s"par two processes time ${partimeTwo.value}")

    val partimeFour = standardConfig measure{
      integrateParFourProcesses(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)
    }

    println(s"par four processes time ${partimeFour.value}")

    val partimeHalfs = standardConfig measure{
      integrateParHalfs(a = a, b = b, integrableFunc= integFunc, totalNumberOfPoints= totalNumberOfPoints)
    }

    println(s"par halfs  time ${partimeHalfs.value}")


    println(s"two speadup ${seqtime.value/partimeTwo.value}")
    println(s"four speadup ${seqtime.value/partimeFour.value}")
    println(s"halfs speadup ${seqtime.value/partimeHalfs.value}")


  }

  main(new Array[String](1))

}
