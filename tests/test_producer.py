"""Unit tests for the producer's batching / wrap-around logic."""

from producer import next_batch


def test_batch_size_and_pointer_advance():
    records = list(range(100))
    batch, pointer = next_batch(records, 0, 10)
    assert batch == list(range(10))
    assert pointer == 10


def test_wrap_around_at_end():
    records = list(range(10))
    batch, pointer = next_batch(records, 8, 5)   # only 2 left, need 5
    assert batch == [8, 9, 0, 1, 2]              # wraps to the start
    assert pointer == 3


def test_pointer_resets_exactly_at_end():
    records = list(range(10))
    batch, pointer = next_batch(records, 5, 5)   # consumes 5-9 exactly
    assert batch == [5, 6, 7, 8, 9]
    assert pointer == 0                          # reset, not out of bounds


def test_batch_never_empty_and_correct_length():
    records = list(range(7))
    pointer = 0
    for _ in range(20):                          # loop many times over a short list
        batch, pointer = next_batch(records, pointer, 4)
        assert len(batch) == 4
        assert 0 <= pointer < len(records)
