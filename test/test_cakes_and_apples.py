from cake_and_apple import count_box, count_item_in_box


def test_count_box():
    assert count_box(0, 0) == 0
    assert count_box(0, 1) == 1
    assert count_box(1, 0) == 1
    assert count_box(10, 5) == 5
    assert count_box(25, 20) == 5


def test_count_item_in_box():
    assert count_item_in_box(0, 0) == (0, 0)
    assert count_item_in_box(0, 1) == (0, 1)
    assert count_item_in_box(1, 0) == (1, 0)
    assert count_item_in_box(10, 5) == (2, 1)
    assert count_item_in_box(25, 20) == (5, 4)
