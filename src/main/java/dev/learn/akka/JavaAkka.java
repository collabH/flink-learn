//package dev.learn.akka;
//
//import akka.actor.ActorRef;
//import akka.actor.ActorSystem;
//import akka.actor.Props;
//import com.typesafe.config.ConfigFactory;
//
///**
// * @fileName: JavaAkka.java
// * @description: JavaAkka.java类说明
// * @author: by echo huang
// * @date: 2021/4/4 12:51 上午
// */
//public class JavaAkka {
//    public static void main(String[] args) {
//        ActorSystem system = ActorSystem.create("sys");
//        ActorSystem actorSystem = ActorSystem.create("helloakka", ConfigFactory.load("appsys"));
//
//        ActorRef helloRef = system.actorOf(Props.create(HelloActor.class), "hello");
//        helloRef.tell("hhh", ActorRef.noSender());
//        system.terminate();
//    }
//}
