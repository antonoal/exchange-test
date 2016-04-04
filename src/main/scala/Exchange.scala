import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import scala.concurrent.Future

class Exchange extends Actor {

  import Exchange._

  var openOrders = Map.empty[OpenOrderKey, List[Order]]
  var execOrders = List.empty[(Order, Order)]

  implicit val exec = context.dispatcher

  def findMatch(o: Order, os: List[Order]): Option[Order] = {
    val matches = os.filter(o2 => o.direction.priceFilterFn(o.price, o2.price))
    if (matches.nonEmpty) {
      val bestPrice = o.direction.findBestPriceFn(matches.map(_.price))
      matches.find(_.price == bestPrice)
    }
    else None
  }

  override def receive: Receive = {
    case o @ Order(direction, ric, qty, _, _) =>
      val orderKey = (ric, qty)
      openOrders.get(orderKey) match {
        case Some(os) if direction == os.head.direction =>
          openOrders = openOrders.updated(orderKey, os :+ o)
        case Some(os) =>
          findMatch(o, os) match {
            case Some(m) =>
              execOrders = (o, m) :: execOrders
              if (os.tail.isEmpty) openOrders -= orderKey
              else openOrders = openOrders.updated(orderKey, os.filterNot(_ == m))
            case None => openOrders = openOrders.updated(orderKey, os :+ o)
          }
        case None =>
          openOrders += orderKey -> List(o)
      }
    case OpenInterest(ric, direction) => Future {
        val interest = openOrders.filterKeys(_._1 == ric).values.withFilter(_.head.direction == direction)
          .flatMap(_.map(o => o.price -> o.qty)).groupBy(_._1).mapValues(_.map(_._2).sum)
        (ric, direction, interest)
      } pipeTo sender()
    case AverageExecPrice(ric) => Future {
      val (sum, qty) = execOrders.foldLeft((0.0, 0)) {
        case ((total, totalQty), (o, _)) => (total + o.price * o.qty, totalQty + o.qty)
      }
      (ric, sum / qty)
    } pipeTo sender()
    case ExecQty(ric, user) =>
      def getQty(o: Order) = {
        if (o.user == user) o.direction match {
          case Buy => o.qty
          case Sell => - o.qty
        } else 0
      }

      Future {
        val res = execOrders.foldLeft(0) { case (acc, (o1, o2)) => getQty(o1) + getQty(o2) + acc }
        (ric, user, res)
      } pipeTo sender()
  }
}

object Exchange {

  type RIC = String
  type OpenOrderKey = (RIC, Int)

  sealed trait Direction {
    def priceFilterFn(p: Double, p2: Double): Boolean
    def findBestPriceFn(ps: List[Double]): Double
  }
  case object Buy extends Direction {

    override def priceFilterFn(p: Double, p2: Double): Boolean = p <= p2

    override def findBestPriceFn(ps: List[Double]): Double = ps.min

  }
  case object Sell extends Direction {

    override def priceFilterFn(p: Double, p2: Double): Boolean = p >= p2

    override def findBestPriceFn(ps: List[Double]): Double = ps.max

  }

  case class User(name: String)

  case class Order(direction: Direction, ric: RIC, qty: Int, price: Double, user: User)

  case class OpenInterest(ric: RIC, direction: Direction)
  case class AverageExecPrice(ric: RIC)
  case class ExecQty(ric: RIC, user: User)

  def main(args: Array[String]) {
    val system = ActorSystem("LSE")
    val x = system.actorOf(Props[Exchange], "exchange")
    val user1 = User("User1")
    val user2 = User("User2")

    val o1 = Order(Buy, "VOD.L", 1000, 100.2, user1)
    val o2 = Order(Buy, "VOD.L", 1000, 100.2, user2)
    val o3 = Order(Sell, "VOD.L", 1000, 100.2, user2)

    import akka.util.Timeout
    import scala.concurrent.duration._
    import scala.concurrent.Await

    implicit val timeout = Timeout(5 second)

    val i1 = Await.result(x ? OpenInterest("VOD.L", Buy), 5 second)
    val a1 = Await.result(x ? AverageExecPrice("VOD.L"), 5 second)
    val q1 = Await.result(x ? ExecQty("VOD.L", user1), 5 second)

    x ! o1
    x ! o2
    x ! o3

    val i2 = Await.result(x ? OpenInterest("VOD.L", Buy), 5 second)
    val i3 = Await.result(x ? OpenInterest("VOD.L", Sell), 5 second)
    val a2 = Await.result(x ? AverageExecPrice("VOD.L"), 5 second)
    val q2 = Await.result(x ? ExecQty("VOD.L", user1), 5 second)

    system.shutdown()
  }
}