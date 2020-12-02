using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools.Util
{
    public static class LinqExtensions
    {
        public static IEnumerable<TResult> FullOuterJoin<TLeft, TRight, TKey, TResult>(
        this IEnumerable<TLeft> left,
        IEnumerable<TRight> right,
        Func<TLeft, TKey> leftKeySelector,
        Func<TRight, TKey> rightKeySelector,
        Func<TLeft, TRight, TKey, TResult> resultSelector,
        IEqualityComparer<TKey> comparator = null,
        TLeft defaultLeft = default(TLeft),
        TRight defaultRight = default(TRight))
        {
            if (left == null) throw new ArgumentNullException("left");
            if (right == null) throw new ArgumentNullException("right");
            if (leftKeySelector == null) throw new ArgumentNullException("leftKeySelector");
            if (rightKeySelector == null) throw new ArgumentNullException("rightKeySelector");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");

            comparator = comparator ?? EqualityComparer<TKey>.Default;
            return FullOuterJoinIterator(left, right, leftKeySelector, rightKeySelector, resultSelector, comparator, defaultLeft, defaultRight);
        }

        internal static IEnumerable<TResult> FullOuterJoinIterator<TLeft, TRight, TKey, TResult>(
            this IEnumerable<TLeft> left,
            IEnumerable<TRight> right,
            Func<TLeft, TKey> leftKeySelector,
            Func<TRight, TKey> rightKeySelector,
            Func<TLeft, TRight, TKey, TResult> resultSelector,
            IEqualityComparer<TKey> comparator,
            TLeft defaultLeft,
            TRight defaultRight)
        {
            var leftLookup = left.ToLookup(leftKeySelector, comparator);
            var rightLookup = right.ToLookup(rightKeySelector, comparator);
            var keys = leftLookup.Select(g => g.Key).Union(rightLookup.Select(g => g.Key), comparator);

            foreach (var key in keys)
                foreach (var leftValue in leftLookup[key].DefaultIfEmpty(defaultLeft))
                    foreach (var rightValue in rightLookup[key].DefaultIfEmpty(defaultRight))
                        yield return resultSelector(leftValue, rightValue, key);
        }
    }
}
