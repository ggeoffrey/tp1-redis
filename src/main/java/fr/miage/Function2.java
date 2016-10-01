package fr.miage;

/**
 * Created by geoffrey on 28/09/2016.
 */
@FunctionalInterface
public interface Function2<A,B,R>{
    public R apply(A a, B b);
}