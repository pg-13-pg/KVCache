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

add_test(NAME scheduler_plain_sleep COMMAND $<TARGET_FILE:test_scheduler>)
set_tests_properties(scheduler_plain_sleep PROPERTIES TIMEOUT 20)

set_tests_properties(
  defer_scope_exit
  serialization_round_trip
  format_substitution
  thread_join
  PROPERTIES TIMEOUT 5
)
