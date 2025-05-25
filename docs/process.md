# notes on process

There are quite a few duplicate documents across the train and test datasets.
Here are the frequency of counts of duplicates relative to the train and test datasets, averaged by split date.

```
+---+---------+---------+
|  n|    train|     test|
+---+---------+---------+
|  1|1592568.0|1643971.0|
|  2| 158678.0| 198806.0|
|  3|  40312.0|  64503.0|
|  4|  12406.0|  22669.0|
|  5|   4003.0|   7577.0|
|  6|   1200.0|   2383.0|
|  7|    301.0|    616.0|
|  8|     94.0|    185.0|
|  9|     31.0|     64.0|
| 10|     18.0|     21.0|
| 11|      4.0|      5.0|
| 12|      1.0|      1.0|
| 13|      1.0|      1.0|
| 14|     NULL|      1.0|
| 15|     NULL|      1.0|
| 16|      1.0|     NULL|
| 17|      1.0|     NULL|
| 21|      2.0|     NULL|
| 22|      1.0|     NULL|
| 23|      1.0|     NULL|
+---+---------+---------+
```

Roughly 10-20% of the documents in the datasets are duplicates.
These cause issues with retrieval and ranking, since the same document can thus be retrieved multiple times.
These need to be adjusted for in those processes then.
We find that they're generally the same document, so we can just take the longest document.
