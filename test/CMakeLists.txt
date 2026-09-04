add_executable(test_defer defer_run.cpp)
add_test(NAME defer_scope_exit COMMAND $<TARGET_FILE:test_defer>)

add_executable(test_serialization run.cpp)
target_link_libraries(test_serialization PRIVATE boost_serialization)
add_test(NAME serialization_round_trip COMMAND $<TARGET_FILE:test_serialization>)

add_executable(test_format format.cpp)
target_link_libraries(test_format PRIVATE boost_serialization)
add_test(NAME format_substitution COMMAND $<TARGET_FILE:test_format>)

add_executable(test_thread ../example/fiberExample/test_thread.cc)
target_link_libraries(test_thread PRIVATE fiber_lib pthread dl)
add_test(NAME thread_join COMMAND $<TARGET_FILE:test_thread>)

add_executable(test_iomanager_sleep iomanager_sleep.cpp)
target_link_libraries(test_iomanager_sleep PRIVATE fiber_lib pthread dl)
add_test(NAME iomanager_cooperative_sleep COMMAND $<TARGET_FILE:test_iomanager_sleep>)
set_tests_properties(iomanager_cooperative_sleep PROPERTIES TIMEOUT 5)

add_executable(test_persister_wal persister_wal.cpp)
target_link_libraries(test_persister_wal PRIVATE skip_list_on_raft boost_serialization)
add_test(NAME persister_wal COMMAND $<TARGET_FILE:test_persister_wal>)
set_tests_properties(persister_wal PROPERTIES TIMEOUT 10)

add_executable(test_skip_list_snapshot skip_list_snapshot.cpp)
target_link_libraries(test_skip_list_snapshot PRIVATE boost_serialization)
add_test(NAME skip_list_snapshot COMMAND $<TARGET_FILE:test_skip_list_snapshot>)

add_executable(test_snapshot_policy snapshot_policy.cpp ../src/raftCore/snapshot_policy.cpp)
add_test(NAME snapshot_policy COMMAND $<TARGET_FILE:test_snapshot_policy>)
set_tests_properties(skip_list_snapshot snapshot_policy PROPERTIES TIMEOUT 5)

add_executable(test_status_proto status_proto.cpp ${src_raftRpcPro})
target_link_libraries(test_status_proto PRIVATE protobuf)
add_test(NAME status_proto COMMAND $<TARGET_FILE:test_status_proto>)
set_tests_properties(status_proto PROPERTIES TIMEOUT 5)

add_executable(rpc_frame_test rpc_frame_test.cpp
  ../src/rpc/rpc_frame.cpp
  ../src/rpc/mprpcchannel.cpp
  ../src/rpc/mprpccontroller.cpp
  ../src/rpc/rpcheader.pb.cc
  ../src/raftRpcPro/kvServerRPC.pb.cc)
target_link_libraries(rpc_frame_test PRIVATE muduo_net muduo_base protobuf boost_serialization pthread)
add_test(NAME rpc_frame_test COMMAND $<TARGET_FILE:rpc_frame_test>)
set_tests_properties(rpc_frame_test PROPERTIES TIMEOUT 5)

add_executable(raft_append_entries_test raft_append_entries_test.cpp ${src_common})
target_link_libraries(raft_append_entries_test PRIVATE skip_list_on_raft boost_serialization protobuf pthread fiber_lib dl)
add_test(NAME raft_append_entries_test COMMAND $<TARGET_FILE:raft_append_entries_test>)
set_tests_properties(raft_append_entries_test PROPERTIES TIMEOUT 10)

add_executable(raft_fault_policy_test raft_fault_policy_test.cpp ../src/raftCore/raft_fault_policy.cpp)
add_test(NAME raft_fault_policy_test COMMAND $<TARGET_FILE:raft_fault_policy_test>)
set_tests_properties(raft_fault_policy_test PROPERTIES TIMEOUT 5)

add_executable(test_cluster_config cluster_config.cpp ${src_common})
target_link_libraries(test_cluster_config PRIVATE boost_serialization)
add_test(NAME cluster_config COMMAND $<TARGET_FILE:test_cluster_config>)
set_tests_properties(cluster_config PROPERTIES TIMEOUT 5)

add_test(NAME scheduler_plain_sleep COMMAND $<TARGET_FILE:test_scheduler>)
set_tests_properties(scheduler_plain_sleep PROPERTIES TIMEOUT 20)

set_tests_properties(
  defer_scope_exit
  serialization_round_trip
  format_substitution
  thread_join
  PROPERTIES TIMEOUT 5
)

find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  add_test(NAME scenario_suite_help
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/run_scenario_suite.py --help)
  set_tests_properties(scenario_suite_help PROPERTIES TIMEOUT 5)

  function(add_raft_integration_test test_name scenario_name)
    add_test(
      NAME ${test_name}
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/integration/raft_cluster_test.py
              --scenario ${scenario_name}
              --raft-node $<TARGET_FILE:raftNode>
              --kvctl $<TARGET_FILE:kvctl>
              --artifact-root ${CMAKE_CURRENT_BINARY_DIR}/artifacts)
    set_tests_properties(${test_name} PROPERTIES
      LABELS raft_integration
      TIMEOUT 120
      SKIP_RETURN_CODE 77)
  endfunction()
  add_raft_integration_test(raft_integration_failover failover)
  add_raft_integration_test(raft_integration_restart restart)
  add_raft_integration_test(raft_integration_snapshot_restart snapshot_restart)
  add_raft_integration_test(raft_integration_snapshot_catchup snapshot_catchup)
  add_raft_integration_test(raft_integration_wal_tail wal_tail)
  add_raft_integration_test(raft_cluster_transport_faults transport_faults)
else()
  message(STATUS "Python 3 not found: Raft integration tests unavailable")
endif()
