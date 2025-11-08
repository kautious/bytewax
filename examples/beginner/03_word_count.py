"""Word Count - Classic Stream Processing Example

This example demonstrates the classic "word count" problem:
- Read text data
- Split into words
- Count occurrences of each word
- Output results

Concepts introduced:
- Keyed streams (grouping by key)
- Stateful processing (reduce)
- Working with tuples (key, value)
- Aggregation patterns
"""

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource, TestingSink, run_main


def example_1_simple_word_count():
    """Example 1: Basic word count."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Word Count")
    print("=" * 60)

    flow = Dataflow("word_count")

    # Input: lines of text
    lines = [
        "hello world",
        "hello bytewax",
        "stream processing with bytewax",
        "hello stream",
    ]
    s = op.input("lines", flow, TestingSource(lines))

    # Split lines into words
    s = op.flat_map("split_words", s, lambda line: line.lower().split())

    # Add a count of 1 to each word (key, value) pairs
    s = op.map("add_count", s, lambda word: (word, 1))

    # Group by word (key) and sum the counts
    s = op.reduce_final("sum_counts", s, lambda acc, x: acc + x)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nInput text:")
    for line in lines:
        print(f"  {line}")

    print("\nWord counts:")
    for word, count in sorted(output):
        print(f"  '{word}': {count}")


def example_2_word_count_with_inspect():
    """Example 2: Word count with inspection of intermediate steps."""
    print("\n" + "=" * 60)
    print("Example 2: Word Count with Debugging")
    print("=" * 60)

    flow = Dataflow("word_count_debug")

    lines = ["hello world", "hello bytewax"]
    s = op.input("lines", flow, TestingSource(lines))

    print("\n📝 Processing pipeline:")

    # Step 1: Split into words
    s = op.flat_map("split", s, lambda line: line.split())
    s = op.inspect("words", s, lambda x: print(f"  Word: {x}"))

    # Step 2: Convert to (word, 1) pairs
    s = op.map("pair", s, lambda word: (word, 1))
    s = op.inspect("pairs", s, lambda x: print(f"  Pair: {x}"))

    # Step 3: Sum by key
    s = op.reduce_final("sum", s, lambda acc, x: acc + x)
    s = op.inspect("counts", s, lambda x: print(f"  Count: {x}"))

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print(f"\nFinal counts: {sorted(output)}")


def example_3_filtered_word_count():
    """Example 3: Word count with filtering."""
    print("\n" + "=" * 60)
    print("Example 3: Word Count with Filtering")
    print("=" * 60)

    flow = Dataflow("filtered_word_count")

    text = """
    The quick brown fox jumps over the lazy dog.
    The dog was not amused. The fox was very quick.
    """

    s = op.input("text", flow, TestingSource([text]))

    # Clean and split into words
    s = op.flat_map(
        "split",
        s,
        lambda text: text.lower().replace(".", "").replace(",", "").split(),
    )

    # Filter out short words (< 4 characters)
    s = op.filter("long_words", s, lambda word: len(word) >= 4)

    # Create (word, 1) pairs
    s = op.map("pair", s, lambda word: (word, 1))

    # Count occurrences
    s = op.reduce_final("count", s, lambda acc, x: acc + x)

    # Filter: keep only words that appear more than once
    s = op.filter("frequent", s, lambda pair: pair[1] > 1)

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nWords appearing more than once (4+ chars):")
    for word, count in sorted(output, key=lambda x: x[1], reverse=True):
        print(f"  '{word}': {count} times")


def example_4_top_n_words():
    """Example 4: Find top N most common words."""
    print("\n" + "=" * 60)
    print("Example 4: Top 5 Most Common Words")
    print("=" * 60)

    flow = Dataflow("top_words")

    # Sample text (lorem ipsum style)
    lines = [
        "data stream processing with bytewax",
        "bytewax makes stream processing easy",
        "processing data in real time",
        "stream data through operators",
        "bytewax stream processing framework",
    ]

    s = op.input("lines", flow, TestingSource(lines))

    # Split and lowercase
    s = op.flat_map("split", s, lambda line: line.lower().split())

    # Count words
    s = op.map("pair", s, lambda word: (word, 1))
    s = op.reduce_final("count", s, lambda acc, x: acc + x)

    # Collect all counts
    s = op.key_on("to_global", s, lambda pair: "all")
    s = op.fold_final("collect", s, list, lambda acc, x: acc + [x[1]])

    # Get value only (drop key)
    s = op.map_value("get_list", s, lambda items: items)

    # Sort and take top 5
    s = op.map_value("sort", s, lambda items: sorted(items, reverse=True)[:5])

    output = []
    op.output("out", s, TestingSink(output))

    run_main(flow)

    print("\nAll text:")
    for line in lines:
        print(f"  {line}")

    print("\nTop 5 word counts:")
    for count in output[0][1]:
        print(f"  {count} occurrences")


def example_5_word_statistics():
    """Example 5: Comprehensive word statistics."""
    print("\n" + "=" * 60)
    print("Example 5: Word Statistics")
    print("=" * 60)

    flow = Dataflow("word_stats")

    text = """
    Python is great. Bytewax is great.
    Stream processing is powerful.
    Python and Bytewax make stream processing easy.
    """

    s = op.input("text", flow, TestingSource([text]))

    # Clean and split
    s = op.flat_map(
        "words",
        s,
        lambda t: t.lower().replace(".", "").replace(",", "").split(),
    )

    # Count words
    s = op.map("pair", s, lambda w: (w, 1))
    word_counts = op.reduce_final("count", s, lambda acc, x: acc + x)

    output = []
    op.output("out", word_counts, TestingSink(output))

    run_main(flow)

    # Calculate statistics
    counts = [count for _, count in output]
    words = [word for word, _ in output]

    print(f"\nTotal unique words: {len(words)}")
    print(f"Total word count: {sum(counts)}")
    print(f"Average occurrences: {sum(counts) / len(counts):.2f}")
    print(f"Max occurrences: {max(counts)}")

    print("\nAll word counts:")
    for word, count in sorted(output, key=lambda x: x[1], reverse=True):
        print(f"  '{word}': {'█' * count} ({count})")


if __name__ == "__main__":
    print("\n📊 Word Count Examples")
    print("Learn keyed streams and aggregation")

    example_1_simple_word_count()
    example_2_word_count_with_inspect()
    example_3_filtered_word_count()
    example_4_top_n_words()
    example_5_word_statistics()

    print("\n" + "=" * 60)
    print("✅ All word count examples completed!")
    print("=" * 60)


"""
Key Takeaways:

1. KEYED STREAMS:
   - Use tuples (key, value) to group data
   - Many operators work on keyed streams
   - Key determines grouping for aggregations

2. REDUCE operator:
   - Aggregates values for each key
   - reduce_final: produces one final result per key
   - Accumulator function: (acc, value) -> new_acc

3. COMMON PATTERN:
   - Split data
   - Map to (key, value) pairs
   - Aggregate with reduce
   - Post-process results

4. PROCESSING STEPS:
   Text → Words → (word, 1) → (word, count) → Results

5. VARIATIONS:
   - Filter before counting (remove stopwords)
   - Filter after counting (frequent words only)
   - Sort and limit (top N)
   - Calculate statistics

Practice exercises:
1. Add stopword filtering (the, and, is, etc.)
2. Count word lengths instead of words
3. Find words that appear exactly once
4. Create a histogram of word counts
"""
