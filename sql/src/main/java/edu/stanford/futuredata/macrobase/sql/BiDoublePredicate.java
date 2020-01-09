package edu.stanford.futuredata.macrobase.sql;

import java.util.Objects;

/**
 * Represents a predicate (boolean-valued function) of two {@code double}-valued argument (i.e.,
 * {@link #test(double, double)} takes in two arguments, instead of one). This is the {@code
 * double}-consuming primitive type specialization of {@link java.util.function.Predicate}.
 *
 * Implementation adapted from {@link java.util.function.DoublePredicate}
 *
 * @see java.util.function.DoublePredicate
 * @since 1.8
 */
@FunctionalInterface
public interface BiDoublePredicate {

    /**
     * Evaluates this predicate on the given argument.
     *
     * @param x the first input argument
     * @param y the second input argument
     * @return {@code true} if the input argument matches the predicate, otherwise {@code false}
     */
    boolean test(double x, double y);

    /**
     * Returns a composed predicate that represents a short-circuiting logical AND of this predicate
     * and another.  When evaluating the composed predicate, if this predicate is {@code false},
     * then the {@code other} predicate is not evaluated.
     *
     * <p>Any exceptions thrown during evaluation of either predicate are relayed to the caller; if
     * evaluation of this predicate throws an exception, the {@code other} predicate will not be
     * evaluated.
     *
     * @param other a predicate that will be logically-ANDed with this predicate
     * @return a composed predicate that represents the short-circuiting logical AND of this
     * predicate and the {@code other} predicate
     * @throws NullPointerException if other is null
     */
    default BiDoublePredicate and(BiDoublePredicate other) {
        Objects.requireNonNull(other);
        return (x, y) -> test(x, y) && other.test(x, y);
    }

    /**
     * Returns a predicate that represents the logical negation of this predicate.
     *
     * @return a predicate that represents the logical negation of this predicate
     */
    default BiDoublePredicate negate() {
        return (x, y) -> !test(x, y);
    }

    /**
     * Returns a composed predicate that represents a short-circuiting logical OR of this predicate
     * and another.  When evaluating the composed predicate, if this predicate is {@code true}, then
     * the {@code other} predicate is not evaluated.
     *
     * <p>Any exceptions thrown during evaluation of either predicate are relayed to the caller; if
     * evaluation of this predicate throws an exception, the {@code other} predicate will not be
     * evaluated.
     *
     * @param other a predicate that will be logically-ORed with this predicate
     * @return a composed predicate that represents the short-circuiting logical OR of this
     * predicate and the {@code other} predicate
     * @throws NullPointerException if other is null
     */
    default BiDoublePredicate or(BiDoublePredicate other) {
        Objects.requireNonNull(other);
        return (x, y) -> test(x, y) || other.test(x, y);
    }
}
