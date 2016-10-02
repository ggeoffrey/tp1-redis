package fr.miage;

/**
 * Describe an anonymous function that takes two parameters
 * of type A and B respectively and return a result of type R.
 * Created by geoffrey on 28/09/2016.
 */
@FunctionalInterface
public interface Function2<A, B, R> {
    public R apply(A a, B b);
}