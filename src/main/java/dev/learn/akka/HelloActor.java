package dev.learn.akka;

import akka.actor.ActorPath;
import akka.actor.typed.ActorRef;

/**
 * @fileName: HelloActor.java
 * @description: HelloActor.java类说明
 * @author: by echo huang
 * @date: 2021/7/3 8:18 下午
 */
public class HelloActor implements ActorRef<String> {

    @Override
    public void tell(String msg) {
        System.out.println(msg);
    }

    @Override
    public ActorRef<String> narrow() {
        return null;
    }

    @Override
    public <U> ActorRef<U> unsafeUpcast() {
        return null;
    }

    @Override
    public ActorPath path() {
        return null;
    }


    @Override
    public int compareTo(ActorRef<?> o) {
        return 0;
    }
}
