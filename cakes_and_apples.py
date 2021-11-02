def count_box(cakes, apples):
    """Calculate the number of boxes needed using Highest Common Divisor

    Returns:
        int: Number of boxes
    """
    x, y = cakes, apples
    while y:
        x, y = y, x % y
    return x


def count_item_in_box(cakes, apples):
    """Count the number of items in each box

    Returns:
        tuple: Number of cake and apple
    """
    # Edge cases where there is no cake and apple
    box_ = count_box(cakes, apples)
    if box_ == 0:
        return (0, 0)
    return int(cakes / box_), int(apples / box_)
