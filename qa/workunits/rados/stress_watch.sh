#!/bin/sh -e

ceph_test_stress_watch
ceph_multi_stress_watch rep foo foo
ceph_multi_stress_watch ec ecfoo ecfoo

exit 0
