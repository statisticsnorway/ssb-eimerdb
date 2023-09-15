from eimerdb.functions import example_function


def test_example_function() -> None:
    assert example_function(1, 2) == "1 is less than 2"
    assert example_function(1, 0) == "1 is greater than or equal to 0"
