from clock import LamportClock, VectorClock

import pytest


@pytest.mark.parametrize("clock_class", [LamportClock, VectorClock])
def test_clock_is_monoton(clock_class):
    c = clock_class()
    t1 = c.get_time()
    t2 = c.get_time()
    assert t1 < t2


@pytest.mark.parametrize("clock_class", [LamportClock, VectorClock])
def test_comunication_is_monoton(clock_class):
    c1 = clock_class()
    c2 = clock_class()
    t1_before = c1.get_time()
    t2_before = c2.get_time()
    # communicate
    c2.seen_time(t1_before)
    c1.seen_time(t2_before)
    # after communication, time has advanced
    t1_after = c1.get_time()
    t2_after = c2.get_time()
    assert t1_before < t1_after
    assert t2_before < t1_after
    assert t1_before < t2_after
    assert t2_before < t2_after


def test_lamport_time_is_always_ordered():
    t1 = LamportClock().get_time()
    t2 = LamportClock().get_time()
    assert (t1 < t2) or (t2 < t1)


def test_vector_time_without_communication_is_unordered():
    t1 = VectorClock().get_time()
    t2 = VectorClock().get_time()

    assert (t1 < t2) is False
    assert (t2 < t1) is False
    assert t1 != t2


@pytest.mark.parametrize("clock_class", [LamportClock, VectorClock])
def test_can_restore_clock(clock_class):
    # create a fresh clock, advance the time
    c1 = clock_class()
    for _ in range(10):
        c1.get_time()
    # export data (to save after restart)
    ts = c1.get_time()
    export = c1.export()

    # create another clock from the export
    c2 = clock_class.create_from_export(export)

    # the result must be "the same" clock
    # but with advanced time

    assert c1.process_id == c2.process_id
    assert ts < c2.get_time()
